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

	"github.com/tigrisdata/tigris/store/cache"
)

const (
	basePos = "0-0"

	// streamFromCurrentPos is for creating a consumer group that sets the position as current
	streamFromCurrentPos = "$"

	// streamFromCurrentPosRead is to let stream know that reads needs to happen from current
	streamFromCurrentPosRead = ">"
)

// Watch is called when an event is received by ChannelWatcher.
type Watch func(*cache.StreamMessages, error) ([]string, error)

// ChannelWatcher is to watch events for a single channel. It accepts a watch that will be notified when a new event
// is read from the stream. As ChannelWatcher is mapped to a consumer group on a stream therefore the state is restored
// from the cache during restart which means a watcher is only created if it doesn’t exist otherwise the existing one
// is returned.
type ChannelWatcher struct {
	ctx      context.Context
	name     string
	pos      string
	watch    Watch
	stream   cache.Stream
	sigClose chan struct{}
}

func CreateWatcher(ctx context.Context, id string, pos string, stream cache.Stream) *ChannelWatcher {
	if len(pos) == 0 {
		pos = streamFromCurrentPosRead
	}

	return newWatcher(ctx, id, pos, stream)
}

func CreateAndRegisterWatcher(ctx context.Context, id string, pos string, stream cache.Stream) (*ChannelWatcher, error) {
	if len(pos) == 0 {
		pos = streamFromCurrentPos
	}

	if err := stream.CreateConsumerGroup(ctx, id, pos); err != nil {
		return nil, err
	}

	if pos == streamFromCurrentPos {
		pos = streamFromCurrentPosRead
	}

	return newWatcher(ctx, id, pos, stream), nil
}

func newWatcher(ctx context.Context, id string, pos string, stream cache.Stream) *ChannelWatcher {
	return &ChannelWatcher{
		ctx:      ctx,
		name:     id,
		pos:      pos,
		stream:   stream,
		sigClose: make(chan struct{}),
	}
}

func (watcher *ChannelWatcher) StartWatching(watch Watch) {
	watcher.watch = watch
	go watcher.watchEvents()
}

func (watcher *ChannelWatcher) Move(newPos string) {
	if len(newPos) == 0 {
		return
	}

	watcher.pos = newPos
}

func (watcher *ChannelWatcher) Position() string {
	return watcher.pos
}

func (watcher *ChannelWatcher) Close() {
	close(watcher.sigClose)
}

func (watcher *ChannelWatcher) watchEvents() {
	for {
		select {
		case <-watcher.sigClose:
			_ = watcher.stream.RemoveConsumerGroup(watcher.ctx, watcher.name)
			return
		default:
			resp, hasData, err := watcher.stream.ReadGroup(watcher.ctx, watcher.name, watcher.pos)
			if err != nil {
				continue
			}

			if !hasData && watcher.pos == streamFromCurrentPosRead {
				continue
			}

			if !hasData {
				watcher.pos = streamFromCurrentPosRead
				continue
			}

			if ids, err := watcher.watch(resp, nil); err == nil {
				watcher.ack(watcher.ctx, ids)
			}
		}
	}
}

func (watcher *ChannelWatcher) ack(ctx context.Context, ids []string) error {
	if err := watcher.stream.Ack(ctx, watcher.name, ids...); err != nil {
		return err
	}

	/**
	split := strings.Split(ids[len(ids)-1], "-")
	incrId, _ := strconv.ParseInt(split[1], 10, 64)
	watcher.pos = fmt.Sprintf("%s-%d", split[0], incrId+1)*/
	return nil
}
