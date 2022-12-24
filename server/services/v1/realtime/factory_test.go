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
		require.Equal(t, []string{"test"}, channels)
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
		require.Equal(t, []string{"test"}, channels)

		channel3, err := factory.GetChannel(ctx, 1, 1, "test")
		require.NoError(t, err)
		require.Equal(t, channel1, channel3)
	})
}

func newFactory(_ *testing.T) *ChannelFactory {
	cacheS := cache.NewCache(config.GetTestCacheConfig())
	encoder := metadata.NewCacheEncoder()
	return NewChannelFactory(cacheS, encoder, NewHeartbeatFactory(cacheS, encoder))
}
