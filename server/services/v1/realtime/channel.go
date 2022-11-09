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

func (ch *Channel) getWatcherFromRedis(ctx context.Context, watcher string, resume string) (*ChannelWatcher, bool, error) {
	group, exists, err := ch.stream.GetConsumerGroup(ctx, watcher)
	if err != nil {
		return nil, exists, err
	}

	if !exists {
		return nil, exists, err
	}

	if len(resume) == 0 {
		if group.LastDeliveredID == basePos {
			resume = streamFromCurrentPosRead
		} else {
			resume = group.LastDeliveredID
		}
	}

	return CreateWatcher(ctx, watcher, resume, ch.stream), true, nil
}

func (ch *Channel) ListWatchers() []string {
	ch.RLock()
	defer ch.RUnlock()

	var watchersList []string
	for _, w := range ch.watchers {
		watchersList = append(watchersList, w.name)
	}

	return watchersList
}

func (ch *Channel) GetWatcher(ctx context.Context, watcherName string, resume string) (*ChannelWatcher, error) {
	if watcher := ch.getWatcher(watcherName); watcher != nil {
		// already cached in-memory
		watcher.Move(resume)
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

// DestroyWatcher ToDo: call it during leave
func (ch *Channel) DestroyWatcher(_ context.Context, watcher string) {
	ch.Lock()
	defer ch.Unlock()

	_, ok := ch.watchers[watcher]
	if !ok {
		return
	}

	ch.watchers[watcher].Close()
	delete(ch.watchers, watcher)
}

func (ch *Channel) Close(ctx context.Context) {
	ch.Lock()
	defer ch.Unlock()

	for _, w := range ch.watchers {
		w.Close()
		delete(ch.watchers, w.name)
	}

	if err := ch.stream.Delete(ctx); err != nil {
		log.Err(err).Str("channel", ch.encName).Msg("deleting stream failed")
		return
	}
}
