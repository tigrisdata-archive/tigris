// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package middleware

import (
	"context"
	"net/url"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metrics"

	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"

	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
)

type TokenCtxkey struct{}

var (
	headerAuthorize           = "authorization"
	UnknownNamespace          = "unknown"
	BypassAuthForTheseMethods = container.NewHashSet(
		"/HealthAPI/Health",
		"/tigrisdata.auth.v1.Auth/getAccessToken",
		"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
	)
)

type Namespace struct {
	Code string `json:"code"`
}

type User struct {
	Email string `json:"email"`
}

type CustomClaim struct {
	Namespace Namespace `json:"https://tigris/n"`
	User      User      `json:"https://tigris/u"`
}

func (c CustomClaim) Validate(_ context.Context) error {
	if len(c.Namespace.Code) == 0 {
		return errors.PermissionDenied("empty namespace code in token")
	}
	return nil
}

func AuthFromMD(ctx context.Context, expectedScheme string) (string, error) {
	val := api.GetHeader(ctx, headerAuthorize)
	if val == "" {
		log.Debug().Msg("No authorization header present")
		return "", errors.Unauthenticated("request unauthenticated with " + expectedScheme)
	}
	splits := strings.SplitN(val, " ", 2)
	if len(splits) < 2 {
		log.Debug().Msg("Invalid authorization header present")
		return "", errors.Unauthenticated("bad authorization string")
	}
	if !strings.EqualFold(splits[0], expectedScheme) {
		log.Debug().Msg("Unsupported authorization scheme")
		return "", errors.Unauthenticated("request unauthenticated with bearer")
	}
	return splits[1], nil
}

func GetJWTValidator(config *config.Config) *validator.Validator {
	issuerURL, _ := url.Parse(config.Auth.IssuerURL)
	provider := jwks.NewCachingProvider(issuerURL, config.Auth.JWKSCacheTimeout)

	jwtValidator, err := validator.New(
		provider.KeyFunc,
		validator.RS256,
		issuerURL.String(),
		[]string{config.Auth.Audience},
		validator.WithAllowedClockSkew(time.Duration(config.Auth.TokenClockSkewDurationSec)*time.Second),
		validator.WithCustomClaims(
			func() validator.CustomClaims {
				return &CustomClaim{}
			},
		),
	)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to configure JWTValidator")
	}
	return jwtValidator
}

func measuredAuthFunction(ctx context.Context, jwtValidator *validator.Validator, config *config.Config, cache *lru.Cache) (ctxResult context.Context, err error) {
	spanMeta := metrics.NewSpanMeta("auth", "auth", metrics.AuthSpanType, metrics.GetAuthBaseTags(ctx))
	spanMeta.StartTracing(ctx, true)
	ctxResult, err = authFunction(ctx, jwtValidator, config, cache)
	if err != nil {
		metrics.AuthErrorCount.Tagged(spanMeta.GetAuthErrorTags(err)).Counter("error").Inc(1)
		spanMeta.FinishWithError(ctxResult, "auth", err)
		spanMeta.RecordDuration(metrics.AuthErrorRespTime, spanMeta.GetAuthErrorTags(err))
		return
	}
	metrics.AuthOkCount.Tagged(spanMeta.GetAuthOkTags()).Counter("ok").Inc(1)
	spanMeta.FinishTracing(ctxResult)
	spanMeta.RecordDuration(metrics.AuthRespTime, spanMeta.GetAuthOkTags())
	return
}

func authFunction(ctx context.Context, jwtValidator *validator.Validator, config *config.Config, cache *lru.Cache) (ctxResult context.Context, err error) {
	reqMetadata, err := request.GetRequestMetadata(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to load request metadata")
	}
	defer func() {
		if err != nil {
			if config.Auth.LogOnly {
				err = nil
			} else {
				if reqMetadata != nil {
					log.Debug().Str("error", err.Error()).Str("unauthenticated_namespace", reqMetadata.GetUnAuthenticatedNamespaceName()).Err(err).Msg("could not validate token")
				} else {
					log.Debug().Str("error", err.Error()).Err(err).Msg("could not validate token")
				}
			}
		}
	}()
	// disable health check authn/z
	fullMethodName, fullMethodNameFound := grpc.Method(ctx)
	if fullMethodNameFound && BypassAuthForTheseMethods.Contains(fullMethodName) {
		return ctx, nil
	}
	tkn, err := AuthFromMD(ctx, "bearer")
	if err != nil {
		return ctx, err
	}

	validatedToken, found := cache.Get(tkn)
	if !found {
		validatedToken, err = jwtValidator.ValidateToken(ctx, tkn)
		if err != nil {
			if reqMetadata != nil {
				log.Debug().Str("error", err.Error()).Str("unauthenticated_namespace", reqMetadata.GetUnAuthenticatedNamespaceName()).Err(err).Msg("Failed to validate access token")
			} else {
				log.Debug().Str("error", err.Error()).Err(err).Msg("Failed to validate access token")
			}
			return ctx, errors.Unauthenticated("Failed to validate access token")
		}
		cache.Add(tkn, validatedToken)
	}

	// validate custom claims
	if validatedClaims, ok := validatedToken.(*validator.ValidatedClaims); ok {
		// validate expiration
		if validatedClaims.RegisteredClaims.Expiry+int64(config.Auth.TokenClockSkewDurationSec) < time.Now().Unix() {
			return nil, errors.Unauthenticated("Failed to validate access token")
		}

		if customClaims, ok := validatedClaims.CustomClaims.(*CustomClaim); ok {
			// if incoming namespace is empty, set it to unknown for observables and reject request
			if customClaims.Namespace.Code == "" {
				log.Warn().Msg("Valid token with empty namespace received")
				ctx = request.SetNamespace(ctx, UnknownNamespace)
				return ctx, errors.Unauthenticated("You are not authorized to perform this admin action")
			}
			isAdmin := fullMethodNameFound && request.IsAdminApi(fullMethodName)
			if isAdmin {
				// admin api being called, let's check if the user is of admin allowed namespaces
				if !isAdminNamespace(customClaims.Namespace.Code, config) {
					log.Warn().
						Interface("AdminNamespaces", config.Auth.AdminNamespaces).
						Str("IncomingNamespace", customClaims.Namespace.Code).
						Msg("Valid token received for admin action - but not allowed to administer from this namespace")
					return ctx, errors.Unauthenticated("You are not authorized to perform this admin action")
				}
			}

			log.Debug().Msg("Valid token received")
			token := &request.AccessToken{
				Namespace: customClaims.Namespace.Code,
				Sub:       validatedClaims.RegisteredClaims.Subject,
			}
			return request.SetAccessToken(ctx, token), nil
		}
	}
	// this should never happen.
	return ctx, errors.Unauthenticated("You are not authorized to perform this action")
}

func isAdminNamespace(incomingNamespace string, config *config.Config) bool {
	for _, allowedAdminNamespace := range config.Auth.AdminNamespaces {
		if incomingNamespace == allowedAdminNamespace {
			return true
		}
	}
	return false
}

func getAuthFunction(config *config.Config) func(ctx context.Context) (context.Context, error) {
	if config.Auth.Enabled {
		jwtValidator := GetJWTValidator(config)

		lruCache, err := lru.New(config.Auth.TokenCacheSize)
		if err != nil {
			panic("Failed to setup token cache")
		}

		// inline closure to access the state of jwtValidator
		if config.Tracing.Enabled {
			return func(ctx context.Context) (context.Context, error) {
				return measuredAuthFunction(ctx, jwtValidator, config, lruCache)
			}
		} else {
			return func(ctx context.Context) (context.Context, error) {
				return authFunction(ctx, jwtValidator, config, lruCache)
			}
		}
	}

	return nil
}
