package realtime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/store/cache"
)

func TestFactory(t *testing.T) {
	ctx := context.TODO()
	factory := newFactory(t)

	t.Run("get_list_channels", func(t *testing.T) {
		channel, err := factory.GetOrCreateChannel(ctx, 1, 1, "test")
		require.NoError(t, err)
		defer factory.DeleteChannel(ctx, channel)

		channels, err := factory.ListChannels(ctx, 1, 1, "*")
		require.NoError(t, err)
		require.Equal(t, []string{"cache:1:1:test"}, channels)
	})
	t.Run("strict_create_channel", func(t *testing.T) {
		channel1, err := factory.GetOrCreateChannel(ctx, 1, 1, "test")
		require.NoError(t, err)
		defer factory.DeleteChannel(ctx, channel1)

		channel2, err := factory.CreateChannel(ctx, 1, 1, "test")
		require.Equal(t, cache.ErrStreamAlreadyExists, err)
		require.Nil(t, channel2)

		channels, err := factory.ListChannels(ctx, 1, 1, "*")
		require.NoError(t, err)
		require.Equal(t, []string{"cache:1:1:test"}, channels)

		channel3, err := factory.GetChannel(ctx, 1, 1, "test")
		require.NoError(t, err)
		require.Equal(t, channel1, channel3)
	})
}

func newFactory(t *testing.T) *ChannelFactory {
	cacheS := cache.NewCache(config.GetTestCacheConfig())
	encoder := metadata.NewCacheEncoder()
	return NewChannelFactory(cacheS, encoder, NewHeartbeatFactory(cacheS, encoder))
}
