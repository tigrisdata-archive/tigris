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
	"testing"

	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestAuth(t *testing.T) {
	enforcedAuthConfig := config.Config{
		Server: config.ServerConfig{},
		Log: log.LogConfig{
			Level: "trace",
		},
		Auth: config.AuthConfig{
			IssuerURL:        "",
			Audience:         "",
			JWKSCacheTimeout: 0,
			LogOnly:          false,
		},
		FoundationDB: config.FoundationDBConfig{},
	}

	t.Run("log_only mode: no token", func(t *testing.T) {
		ctx, err := AuthFunction(context.TODO(), &validator.Validator{}, &config.DefaultConfig)
		require.NotNil(t, ctx)
		require.Nil(t, err)
	})

	t.Run("enforcing mode: no token", func(t *testing.T) {
		_, err := AuthFunction(context.TODO(), &validator.Validator{}, &enforcedAuthConfig)
		require.NotNil(t, err)
		require.Equal(t, err, api.Error(codes.Unauthenticated, "request unauthenticated with bearer"))
	})

	t.Run("enforcing mode: Bad authorization string1", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("authorization", "bearer"))
		_, err := AuthFunction(incomingCtx, &validator.Validator{}, &enforcedAuthConfig)
		require.NotNil(t, err)
		require.Equal(t, err, api.Error(codes.Unauthenticated, "bad authorization string"))
	})

	t.Run("enforcing mode: Bad token", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("authorization", "bearer somebadtoken"))
		_, err := AuthFunction(incomingCtx, &validator.Validator{}, &enforcedAuthConfig)
		require.NotNil(t, err)
		require.Equal(t, err, api.Error(codes.Unauthenticated, "could not parse the token: square/go-jose: compact JWS format must have three parts"))
	})

	t.Run("enforcing mode: Bad token 2", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("authorization", "bearer some.bad.token"))
		_, err := AuthFunction(incomingCtx, &validator.Validator{}, &enforcedAuthConfig)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "could not parse the token: illegal base64 data")
	})

	t.Run("test valid extraction of organization name - 1", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("host", "project1-tigrisdata.dev.tigrisdata.cloud"))
		organizationName, err := getOrganizationName(incomingCtx)
		require.Nil(t, err)
		require.Equal(t, "tigrisdata", organizationName)
	})

	t.Run("test invalid extraction of organization name - 1", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("host", "project1tigrisdata.dev.tigrisdata.cloud"))
		organizationName, err := getOrganizationName(incomingCtx)
		require.Nil(t, err)
		require.Equal(t, organizationName, "project1tigrisdata")
	})

	t.Run("test invalid extraction of organization name - 2", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("host", "project1-tigris-data.dev.tigrisdata.cloud"))
		_, err := getOrganizationName(incomingCtx)
		require.Equal(t, err, api.Error(codes.FailedPrecondition, "hostname is not as per expected scheme"))
	})

}
