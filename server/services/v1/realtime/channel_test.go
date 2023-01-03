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

package realtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	_ = cacheS.DeleteStream(ctx, "ch_test")

	t.Run("publish_read", func(t *testing.T) {
		stream, err := cacheS.CreateStream(ctx, "ch_test")
		require.NoError(t, err)
		channel := NewChannel("ch_test", stream)
		defer channel.Close(ctx)

		first := []byte(`{"a": 1}`)
		id1, err := channel.PublishMessage(ctx, internal.NewStreamData(nil, first))
		require.NoError(t, err)

		second := []byte(`{"b": 2}`)
		id2, err := channel.PublishMessage(ctx, internal.NewStreamData(nil, second))
		require.NoError(t, err)

		streamMessages, hasData, err := channel.Read(ctx, "0")
		require.NoError(t, err)
		require.True(t, hasData)

		out, err := streamMessages.Decode(streamMessages.Messages[0])
		require.NoError(t, err)
		require.Equal(t, first, out.RawData)
		require.Equal(t, id1, out.Id)

		streamMessages, hasData, err = channel.Read(ctx, out.Id)
		require.True(t, hasData)
		require.NoError(t, err)
		out, err = streamMessages.Decode(streamMessages.Messages[0])
		require.NoError(t, err)
		require.Equal(t, second, out.RawData)
		require.Equal(t, id2, out.Id)

		streamMessages, hasData, err = channel.Read(ctx, out.Id)
		require.NoError(t, err)
		require.False(t, hasData)
		require.Nil(t, streamMessages)
	})
	t.Run("watcher", func(t *testing.T) {
		stream, err := cacheS.CreateStream(ctx, "ch_test")
		require.NoError(t, err)
		channel := NewChannel("ch_test", stream)
		defer channel.Close(ctx)

		watcher, err := channel.GetWatcher(ctx, "watch", cache.ConsumerGroupDefaultCurrentPos)
		require.NoError(t, err)
		require.Equal(t, watcher, channel.watchers["watch"])

		totalEvents := 16
		dummyWatch := &dummyWatch{
			t: t,
		}
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

		dummyWatch.wait(16)

		receivedIds := dummyWatch.atomicIds.Load().([]string)
		for i := 0; i < totalEvents; i++ {
			require.Equal(t, expectedIds[i], receivedIds[i])
		}
	})
	t.Run("watcher-pause-rejoin", func(t *testing.T) {
		stream, err := cacheS.CreateStream(ctx, "ch_test")
		require.NoError(t, err)
		channel := NewChannel("ch_test", stream)
		defer channel.Close(ctx)

		watcher, err := channel.GetWatcher(ctx, "watch", cache.ConsumerGroupDefaultCurrentPos)
		require.NoError(t, err)
		require.Equal(t, watcher, channel.watchers["watch"])

		totalEvents := 16
		publishedEvents := make([][]byte, totalEvents)
		for i := 0; i < totalEvents; i++ {
			publishedEvents[i] = []byte(fmt.Sprintf(`{"a": %d}`, i))
		}

		var idsAtomic atomic.Value
		idsAtomic.Store(make([]string, totalEvents))

		var shouldStartPublishing atomic.Bool
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			watcherTest := func(from int, to int, expEvents int, pos string) {
				watcher, err := channel.GetWatcher(ctx, "watch", pos)
				require.NoError(t, err)
				require.Equal(t, watcher, channel.watchers["watch"])

				watch := &dummyWatch{t: t, expEvents: publishedEvents[from:to]}
				watcher.StartWatching(watch.watch)
				shouldStartPublishing.Store(true)
				watch.wait(int32(expEvents))
				channel.StopWatcher(watcher.name)

				watchIds := watch.atomicIds.Load().([]string)
				j := 0
				for i := from; i < to; i++ {
					require.Equal(t, idsAtomic.Load().([]string)[i], watchIds[j])
					j++
				}
			}

			watcherTest(0, 4, 4, cache.ConsumerGroupDefaultCurrentPos)
			watcherTest(4, 8, 4, idsAtomic.Load().([]string)[3])
			watcherTest(8, 12, 4, idsAtomic.Load().([]string)[7])
			watcherTest(12, 16, 4, idsAtomic.Load().([]string)[11])
		}()

		for !shouldStartPublishing.Load() {
			time.Sleep(1 * time.Millisecond)
		}

		for i := 0; i < totalEvents; i++ {
			id, err := channel.PublishMessage(ctx, internal.NewStreamData(nil, publishedEvents[i]))
			require.NoError(t, err)
			v := idsAtomic.Load().([]string)
			v[i] = id
			idsAtomic.Store(v)
		}

		wg.Wait()
	})
}

type dummyWatch struct {
	sync.RWMutex

	t           *testing.T
	eventNo     atomic.Int32
	expEvents   [][]byte
	receivedIds []string
	atomicIds   atomic.Value
}

func (dummy *dummyWatch) wait(expTotal int32) {
	ticker := time.NewTicker(50 * time.Millisecond)
	for range ticker.C {
		if dummy.eventNo.Load() == expTotal {
			return
		}
	}
}

func (dummy *dummyWatch) watch(events *cache.StreamMessages, err error) ([]string, error) {
	ids := make([]string, len(events.Messages))
	for i, m := range events.Messages {
		if dummy.eventNo.Load() >= int32(len(dummy.expEvents)) {
			// drop the event
			continue
		}

		data, err := events.Decode(m)
		require.NoError(dummy.t, err)
		require.Equal(dummy.t, dummy.expEvents[dummy.eventNo.Load()], data.RawData)
		dummy.eventNo.Add(1)
		dummy.receivedIds = append(dummy.receivedIds, data.Id)
		dummy.atomicIds.Store(dummy.receivedIds)
		ids[i] = data.Id
	}
	return ids, nil
}
