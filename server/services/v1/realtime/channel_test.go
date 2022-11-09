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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/store/cache"
)

func TestChannel(t *testing.T) {
	cache.BlockReadGroupDuration = 100 * time.Millisecond

	ctx := context.TODO()
	cacheS := cache.NewCache(config.GetTestCacheConfig())

	stream, err := cacheS.CreateOrGetStream(ctx, "test")
	require.NoError(t, err)
	stream.Delete(ctx)

	t.Run("publish_read", func(t *testing.T) {
		stream, err := cacheS.CreateStream(ctx, "test")
		require.NoError(t, err)
		channel := NewChannel("test", stream)
		defer channel.Close(ctx)

		first := []byte(`{"a": 1}`)
		id1, err := channel.PublishMessage(ctx, internal.NewStreamData(nil, first))
		require.NoError(t, err)

		second := []byte(`{"b": 2}`)
		id2, err := channel.PublishMessage(ctx, internal.NewStreamData(nil, second))
		require.NoError(t, err)

		streamMessages, hasData, err := channel.Read(ctx, "0")
		require.True(t, hasData)
		require.NoError(t, err)

		out, err := streamMessages.Decode(streamMessages.Messages[0])
		require.Equal(t, first, out.RawData)
		require.Equal(t, id1, out.Id)

		streamMessages, hasData, err = channel.Read(ctx, out.Id)
		require.True(t, hasData)
		require.NoError(t, err)
		out, err = streamMessages.Decode(streamMessages.Messages[0])
		require.Equal(t, second, out.RawData)
		require.Equal(t, id2, out.Id)

		streamMessages, hasData, err = channel.Read(ctx, out.Id)
		require.False(t, hasData)
		require.Nil(t, streamMessages)
	})
	t.Run("watcher", func(t *testing.T) {
		stream, err := cacheS.CreateStream(ctx, "test")
		require.NoError(t, err)
		channel := NewChannel("test", stream)
		defer channel.Close(ctx)

		watcher, err := channel.GetWatcher(ctx, "watch", streamFromCurrentPos)
		require.NoError(t, err)
		require.Equal(t, watcher, channel.watchers["watch"])

		dummyWatch := &dummyWatch{
			t:       t,
			eventNo: 0,
		}

		totalEvents := 16
		for i := 0; i < totalEvents; i++ {
			dummyWatch.expEvents = append(dummyWatch.expEvents, []byte(fmt.Sprintf(`{"a": %d}`, i)))
		}

		watcher.StartWatching(dummyWatch.watch)

		var expectedIds []string
		for i := 0; i < totalEvents; i++ {
			id, err := channel.PublishMessage(ctx, internal.NewStreamData(nil, dummyWatch.expEvents[i]))
			require.NoError(t, err)
			expectedIds = append(expectedIds, id)
		}

		time.Sleep(1 * time.Second)

		for i := 0; i < totalEvents; i++ {
			require.Equal(t, expectedIds[i], dummyWatch.receivedIds[i])
		}
	})
}

type dummyWatch struct {
	t           *testing.T
	eventNo     int
	expEvents   [][]byte
	receivedIds []string
}

func (dummy *dummyWatch) watch(events *cache.StreamMessages, err error) ([]string, error) {
	var ids []string
	for _, m := range events.Messages {
		data, err := events.Decode(m)
		require.NoError(dummy.t, err)
		require.Equal(dummy.t, dummy.expEvents[dummy.eventNo], data.RawData, string(dummy.expEvents[dummy.eventNo]), string(data.RawData))
		dummy.eventNo++
		dummy.receivedIds = append(dummy.receivedIds, data.Id)
		ids = append(ids, data.Id)
	}
	return ids, nil
}
