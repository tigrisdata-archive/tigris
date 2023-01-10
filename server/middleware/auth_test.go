// Copyright 2022-2023 Tigris Data, Inc.
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
	"os"
	"testing"

	"github.com/auth0/go-jwt-middleware/v2/validator"
	lru "github.com/hashicorp/golang-lru"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc/metadata"
)

func TestAuth(t *testing.T) {
	enforcedAuthConfig := config.Config{
		Server: config.ServerConfig{},
		Log: log.LogConfig{
			Level: "error",
		},
		Auth: config.AuthConfig{
			IssuerURL:                "",
			Audience:                 "",
			JWKSCacheTimeout:         0,
			LogOnly:                  false,
			EnableNamespaceIsolation: false,
			AdminNamespaces:          []string{"tigris-admin"},
		},
		FoundationDB: config.FoundationDBConfig{},
	}

	cache, err := lru.New(5)
	if err != nil {
		panic("Failed to setup cache")
	}
	t.Run("log_only mode: no token", func(t *testing.T) {
		ctx, err := authFunction(context.TODO(), &validator.Validator{}, &config.DefaultConfig, cache)
		require.NotNil(t, ctx)
		require.Nil(t, err)
	})

	t.Run("enforcing mode: no token", func(t *testing.T) {
		_, err := authFunction(context.TODO(), &validator.Validator{}, &enforcedAuthConfig, cache)
		require.NotNil(t, err)
		require.Equal(t, err, errors.Unauthenticated("request unauthenticated with bearer"))
	})

	t.Run("enforcing mode: Bad authorization string1", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("authorization", "bearer"))
		_, err := authFunction(incomingCtx, &validator.Validator{}, &enforcedAuthConfig, cache)
		require.NotNil(t, err)
		require.Equal(t, err, errors.Unauthenticated("bad authorization string"))
	})

	t.Run("enforcing mode: Bad token", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("authorization", "bearer somebadtoken"))
		_, err := authFunction(incomingCtx, &validator.Validator{}, &enforcedAuthConfig, cache)
		require.NotNil(t, err)
		require.Equal(t, err, errors.Unauthenticated("Failed to validate access token"))
	})

	t.Run("enforcing mode: Bad token 2", func(t *testing.T) {
		incomingCtx := metadata.NewIncomingContext(context.TODO(), metadata.Pairs("authorization", "bearer some.bad.token"))
		_, err := authFunction(incomingCtx, &validator.Validator{}, &enforcedAuthConfig, cache)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "Failed to validate access token")
	})

	t.Run("isAdminNamespace", func(t *testing.T) {
		require.False(t, isAdminNamespace("test-name", &enforcedAuthConfig))
		require.True(t, isAdminNamespace("tigris-admin", &enforcedAuthConfig))
	})
}

func TestMain(m *testing.M) {
	log.Configure(log.LogConfig{Level: "disabled"})
	os.Exit(m.Run())
}
