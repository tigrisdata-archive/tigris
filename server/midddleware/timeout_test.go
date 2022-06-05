package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"google.golang.org/grpc/metadata"
)

func TestTimeout(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		api.HeaderRequestTimeout: "0.2",
	}))
	ctx, _ = setDeadlineUsingHeader(ctx)
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(200*time.Millisecond), deadline, 50*time.Millisecond)

	ctx = context.Background()
	ctx, _ = setDeadlineUsingHeader(ctx)
	_, ok = ctx.Deadline()
	require.False(t, ok)
}
