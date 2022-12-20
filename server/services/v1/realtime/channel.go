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
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/store/cache"
)

type Channel struct {
	sync.RWMutex

	encName  string
	tenant   uint32
	project  uint32
	stream   cache.Stream
	watchers map[string]*ChannelWatcher
}

func NewChannel(encName string, stream cache.Stream) *Channel {
	return &Channel{
		encName:  encName,
		stream:   stream,
		watchers: make(map[string]*ChannelWatcher),
	}
}

func (ch *Channel) Name() string {
	return ch.encName
}

func (ch *Channel) Read(ctx context.Context, pos string) (*cache.StreamMessages, bool, error) {
	return ch.stream.Read(ctx, pos)
}

func (ch *Channel) PublishPresence(ctx context.Context, data *internal.StreamData) (string, error) {
	return ch.stream.Add(ctx, data)
}

func (ch *Channel) PublishMessage(ctx context.Context, data *internal.StreamData) (string, error) {
	return ch.stream.Add(ctx, data)
}

func (ch *Channel) getWatcher(watcher string) *ChannelWatcher {
	ch.RLock()
	defer ch.RUnlock()

	return ch.watchers[watcher]
}

func (ch *Channel) getWatcherFromRedis(ctx context.Context, watcherName string, resume string) (*ChannelWatcher, bool, error) {
	group, exists, err := ch.stream.GetConsumerGroup(ctx, watcherName)
	if err != nil {
		return nil, exists, err
	}

	if !exists {
		return nil, exists, err
	}

	watcher, err := CreateWatcher(ctx, watcherName, resume, group.LastDeliveredID, ch.stream)
	if err != nil {
		return nil, false, err
	}
	return watcher, true, nil
}

func (ch *Channel) ListWatchers() []string {
	ch.RLock()
	defer ch.RUnlock()

	watchersList := make([]string, len(ch.watchers))
	i := 0
	for _, w := range ch.watchers {
		watchersList[i] = w.name
		i++
	}

	return watchersList
}

func (ch *Channel) GetWatcher(ctx context.Context, watcherName string, resume string) (*ChannelWatcher, error) {
	if watcher := ch.getWatcher(watcherName); watcher != nil {
		// already cached in-memory
		if err := watcher.move(ctx, resume); err != nil {
			return nil, err
		}

		return watcher, nil
	}

	watcher, exists, err := ch.getWatcherFromRedis(ctx, watcherName, resume)
	if err != nil {
		return nil, err
	}
	if exists {
		ch.Lock()
		ch.watchers[watcherName] = watcher
		ch.Unlock()
		return watcher, nil
	}

	// Create new and attach to the topic
	if watcher, err = CreateAndRegisterWatcher(ctx, watcherName, resume, ch.stream); err != nil {
		return nil, err
	}

	ch.Lock()
	ch.watchers[watcherName] = watcher
	ch.Unlock()

	return watcher, nil
}

// DisconnectWatcher ToDo: call it during leave.
func (ch *Channel) DisconnectWatcher(watcher string) {
	ch.Lock()
	defer ch.Unlock()

	if w, ok := ch.watchers[watcher]; ok {
		w.Disconnect()
		delete(ch.watchers, watcher)
	}
}

func (ch *Channel) StopWatcher(watcher string) {
	ch.Lock()
	defer ch.Unlock()

	if w, ok := ch.watchers[watcher]; ok {
		w.Stop()
		delete(ch.watchers, watcher)
	}
}

func (ch *Channel) Close(ctx context.Context) {
	ch.Lock()
	defer ch.Unlock()

	for _, w := range ch.watchers {
		w.Disconnect()
		delete(ch.watchers, w.name)
	}

	if err := ch.stream.Delete(ctx); err != nil {
		log.Err(err).Str("channel", ch.encName).Msg("deleting stream failed")
		return
	}
}
