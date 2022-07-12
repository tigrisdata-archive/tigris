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

	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
)

type TokenCtxkey struct{}

var (
	headerAuthorize = "authorization"
)

type Namespace struct {
	Code string `json:"code"`
}

type User struct {
	Email string `json:"email"`
}

type CustomClaim struct {
	Namespace Namespace `json:"https://tigris-db-api/n"`
	User      User      `json:"https://tigris-db-api/u"`
}

func (c CustomClaim) Validate(_ context.Context) error {
	if len(c.Namespace.Code) == 0 {
		return api.Errorf(api.Code_PERMISSION_DENIED, "empty namespace code in token")
	}
	return nil
}

func AuthFromMD(ctx context.Context, expectedScheme string) (string, error) {
	val := api.GetHeader(ctx, headerAuthorize)
	if val == "" {
		return "", api.Errorf(api.Code_UNAUTHENTICATED, "request unauthenticated with "+expectedScheme)
	}
	splits := strings.SplitN(val, " ", 2)
	if len(splits) < 2 {
		return "", api.Errorf(api.Code_UNAUTHENTICATED, "bad authorization string")
	}
	if !strings.EqualFold(splits[0], expectedScheme) {
		return "", api.Errorf(api.Code_UNAUTHENTICATED, "request unauthenticated with bearer")
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
		validator.WithAllowedClockSkew(time.Minute),
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

func AuthFunction(ctx context.Context, jwtValidator *validator.Validator, config *config.Config) (ctxResult context.Context, err error) {
	defer func() {
		if err != nil {
			//log.Warn().Bool("log_only?", config.Auth.LogOnly).Str("error", err.Error()).Err(err).Msg("could not validate token")
			if config.Auth.LogOnly {
				err = nil
			}
		}
	}()

	tkn, err := AuthFromMD(ctx, "bearer")
	if err != nil {
		return ctx, err
	}

	validatedToken, err := jwtValidator.ValidateToken(ctx, tkn)
	if err != nil {
		return ctx, api.Errorf(api.Code_UNAUTHENTICATED, err.Error())
	}

	if validatedClaims, ok := validatedToken.(*validator.ValidatedClaims); ok {
		if customClaims, ok := validatedClaims.CustomClaims.(*CustomClaim); ok {
			log.Debug().Msg("Valid token received")
			token := &AccessToken{
				Namespace: customClaims.Namespace.Code,
				Sub:       validatedClaims.RegisteredClaims.Subject,
			}
			return setAccessToken(ctx, token), nil
		}
	}
	// this should never happen.
	return ctx, api.Errorf(api.Code_UNAUTHENTICATED, "You are not authorized to perform this action")
}
