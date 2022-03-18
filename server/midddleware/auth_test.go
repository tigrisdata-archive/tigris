package middleware

import (
	"context"
	"testing"

	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestNoToken(t *testing.T) {
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
}
