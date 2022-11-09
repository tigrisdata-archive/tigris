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
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/store/cache"
)

const monitorChannelDuration = 2 * time.Minute

type ChannelFactory struct {
	sync.RWMutex

	cache      cache.Cache
	encoder    metadata.CacheEncoder
	heartbeatF *HeartbeatFactory
	channels   map[string]*Channel
}

func NewChannelFactory(cache cache.Cache, encoder metadata.CacheEncoder, heartbeatF *HeartbeatFactory) *ChannelFactory {
	factory := &ChannelFactory{
		cache:      cache,
		encoder:    encoder,
		heartbeatF: heartbeatF,
		channels:   make(map[string]*Channel),
	}

	go factory.monitorStreams()
	return factory
}

func (factory *ChannelFactory) monitorStreams() {
	ticker := time.NewTicker(monitorChannelDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, c := range factory.channels {
				_ = factory.deleteChannelIfInactive(c)
			}
		}
	}
}

func (factory *ChannelFactory) deleteChannelIfInactive(c *Channel) error {
	groups, err := c.stream.GetConsumerGroups(context.TODO())
	if err != nil {
		log.Err(err).Str("channel", c.encName).Msg("reading consumers failed")
		return err
	}

	var groupsName []string
	for _, g := range groups {
		groupsName = append(groupsName, g.Name)
	}

	heartbeat := factory.heartbeatF.GetHeartbeatTable(c.tenant, c.project)
	if heartbeat.GroupsExpired(groupsName) {
		factory.DeleteChannel(context.TODO(), c)
	}

	return nil
}

func (factory *ChannelFactory) getChannel(encStream string) (*Channel, bool) {
	factory.RLock()
	defer factory.RUnlock()

	ch, ok := factory.channels[encStream]
	return ch, ok
}

func (factory *ChannelFactory) getOrCreateChannelFromCache(ctx context.Context, encStream string) (*Channel, error) {
	stream, err := factory.cache.CreateOrGetStream(ctx, encStream)
	if err != nil {
		return nil, err
	}

	return NewChannel(encStream, stream), nil
}

func (factory *ChannelFactory) ListChannels(ctx context.Context, tenantId uint32, projId uint32, prefix string) ([]string, error) {
	encProj, err := factory.encoder.EncodeCacheTableName(tenantId, projId, prefix)
	if err != nil {
		return nil, err
	}

	return factory.cache.ListStreams(ctx, encProj)
}

func (factory *ChannelFactory) GetChannel(ctx context.Context, tenantId uint32, projId uint32, channelName string) (*Channel, error) {
	encStream, err := factory.encoder.EncodeCacheTableName(tenantId, projId, channelName)
	if err != nil {
		return nil, err
	}

	if ch, ok := factory.getChannel(encStream); ok {
		return ch, nil
	}

	stream, err := factory.cache.GetStream(ctx, encStream)
	if err != nil {
		return nil, err
	}

	ch := NewChannel(encStream, stream)

	factory.Lock()
	factory.channels[encStream] = ch
	factory.Unlock()
	return ch, nil
}

func (factory *ChannelFactory) GetOrCreateChannel(ctx context.Context, tenantId uint32, projId uint32, channelName string) (*Channel, error) {
	encStream, err := factory.encoder.EncodeCacheTableName(tenantId, projId, channelName)
	if err != nil {
		return nil, err
	}

	if ch, ok := factory.getChannel(encStream); ok {
		return ch, nil
	}

	factory.Lock()
	defer factory.Unlock()

	ch, err := factory.getOrCreateChannelFromCache(ctx, encStream)
	if err != nil {
		return nil, err
	}

	factory.channels[encStream] = ch
	return ch, nil
}

// CreateChannel will throw an error if stream already exists. Use CreateOrGet to create if not exists primitive.
func (factory *ChannelFactory) CreateChannel(ctx context.Context, tenantId uint32, projId uint32, channelName string) (*Channel, error) {
	encStream, err := factory.encoder.EncodeCacheTableName(tenantId, projId, channelName)
	if err != nil {
		return nil, err
	}

	if _, ok := factory.getChannel(encStream); ok {
		return nil, cache.ErrStreamAlreadyExists
	}

	factory.Lock()
	defer factory.Unlock()
	stream, err := factory.cache.CreateStream(ctx, encStream)
	if err != nil {
		return nil, err
	}

	ch := NewChannel(encStream, stream)
	factory.channels[ch.encName] = ch
	return ch, nil
}

func (factory *ChannelFactory) DeleteChannel(ctx context.Context, ch *Channel) {
	factory.Lock()
	defer factory.Unlock()

	ch.Close(ctx)
	delete(factory.channels, ch.encName)
}
