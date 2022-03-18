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
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/config"
	"google.golang.org/grpc/codes"
)

var (
	headerAuthorize   = "authorization"
	grpcGatewayPrefix = "grpc-gateway-"
)

func getHeader(ctx context.Context, header string) string {
	if val := metautils.ExtractIncoming(ctx).Get(header); val != "" {
		return val
	}

	return metautils.ExtractIncoming(ctx).Get(grpcGatewayPrefix + header)
}

func AuthFromMD(ctx context.Context, expectedScheme string) (string, error) {
	val := getHeader(ctx, headerAuthorize)
	if val == "" {
		return "", api.Error(codes.Unauthenticated, "request unauthenticated with "+expectedScheme)
	}
	splits := strings.SplitN(val, " ", 2)
	if len(splits) < 2 {
		return "", api.Error(codes.Unauthenticated, "bad authorization string")
	}
	if !strings.EqualFold(splits[0], expectedScheme) {
		return "", api.Error(codes.Unauthenticated, "request unauthenticated with bearer")
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
	)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to configure JWTValidator")
	}
	return jwtValidator
}

func AuthFunction(ctx context.Context, jwtValidator *validator.Validator, config *config.Config) (ctxResult context.Context, err error) {
	defer func() {
		if err != nil {
			log.Warn().Bool("log_only?", config.Auth.LogOnly).Str("error", err.Error()).Err(err).Msg("could not validate token")
			if config.Auth.LogOnly {
				err = nil
			}
		}
	}()

	token, err := AuthFromMD(ctx, "bearer")
	if err != nil {
		return ctx, err
	}

	validToken, err := jwtValidator.ValidateToken(ctx, token)
	if err != nil {
		return ctx, api.Error(codes.Unauthenticated, err.Error())
	}

	log.Debug().Msg("Valid token received")
	return context.WithValue(ctx, key("token"), validToken), nil
}
