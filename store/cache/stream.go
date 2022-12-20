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

package cache

import (
	"context"
	"time"

	xredis "github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/tigrisdata/tigris/internal"
)

const (
	DefaultGroup    = "_tigris_group"
	DefaultConsumer = "_tigris_consumer"
)

const (
	payloadKey = "_s"
)

type ReadGroupPos string

const (
	// ReadGroupPosStart is to let steam know that reads needs to happen since beginning.
	ReadGroupPosStart ReadGroupPos = "0-0"
	// ReadGroupPosCurrent is to let stream know that reads needs to happen from current.
	ReadGroupPosCurrent ReadGroupPos = ">"
)

const (
	// ConsumerGroupDefaultCurrentPos is for creating a consumer group that sets the position as current.
	ConsumerGroupDefaultCurrentPos = "$"
)

var BlockReadGroupDuration = 180 * time.Second

type StreamMessages struct {
	xredis.XStream
}

func (sm *StreamMessages) Decode(message xredis.XMessage) (*internal.StreamData, error) {
	return decodeFromStreamValue(message)
}

type stream struct {
	cache *cache
	name  string
}

// NewStream creates a stream object 'streamName' is the stream name that is already prefixed with tenant and
// project id already prepended.
func NewStream(cache *cache, streamName string) Stream {
	return &stream{
		cache: cache,
		name:  streamName,
	}
}

func (s *stream) Name() string {
	return s.name
}

func (s *stream) Add(ctx context.Context, value *internal.StreamData) (string, error) {
	data, err := encodeToStreamValue(value)
	if err != nil {
		return "", err
	}

	cmd := s.cache.Client.XAdd(ctx, &xredis.XAddArgs{
		Stream: s.name,
		Values: data,
	})
	return cmd.Result()
}

func (s *stream) Read(ctx context.Context, pos string) (*StreamMessages, bool, error) {
	resp := s.cache.Client.XRead(ctx, &xredis.XReadArgs{
		Streams: []string{s.name, pos},
		Block:   1 * time.Second,
	})

	stream, err := resp.Result()
	if err == xredis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if len(stream[0].Messages) == 0 {
		return nil, true, nil
	}

	return &StreamMessages{
		XStream: stream[0],
	}, true, nil
}

func (s *stream) ReadGroup(ctx context.Context, group string, pos ReadGroupPos) (*StreamMessages, bool, error) {
	resp := s.cache.Client.XReadGroup(ctx, &xredis.XReadGroupArgs{
		Group:    group,
		Consumer: DefaultConsumer,
		Streams:  []string{s.name, string(pos)},
		Block:    BlockReadGroupDuration,
	})

	stream, err := resp.Result()
	if err == xredis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if len(stream[0].Messages) == 0 {
		return nil, true, nil
	}

	return &StreamMessages{
		XStream: stream[0],
	}, true, nil
}

func (s *stream) CreateConsumerGroup(ctx context.Context, group string, pos string) error {
	_, err := s.cache.Client.XGroupCreate(ctx, s.name, group, pos).Result()
	return err
}

func (s *stream) RemoveConsumerGroup(ctx context.Context, group string) error {
	_, err := s.cache.Client.XGroupDestroy(ctx, s.name, group).Result()
	return err
}

func (s *stream) GetConsumerGroups(ctx context.Context) ([]xredis.XInfoGroup, error) {
	groups, err := s.cache.Client.XInfoGroups(ctx, s.name).Result()
	if err != nil {
		if err.Error() == errStrNoSuchKey {
			return nil, nil
		}
		return nil, err
	}

	return groups, nil
}

func (s *stream) GetConsumerGroup(ctx context.Context, group string) (*xredis.XInfoGroup, bool, error) {
	groups, err := s.cache.Client.XInfoGroups(ctx, s.name).Result()
	if err != nil {
		if err.Error() == errStrNoSuchKey {
			return nil, false, nil
		}
		return nil, false, err
	}

	for i := range groups {
		if group == groups[i].Name {
			return &groups[i], true, nil
		}
	}

	return nil, false, nil
}

func (s *stream) SetID(ctx context.Context, group string, pos string) error {
	_, err := s.cache.Client.XGroupSetID(ctx, s.name, group, pos).Result()
	return err
}

func (s *stream) Ack(ctx context.Context, group string, ids ...string) error {
	_, err := s.cache.Client.XAck(ctx, s.name, group, ids...).Result()
	return err
}

func (s *stream) Delete(ctx context.Context) error {
	_, err := s.cache.Client.Del(ctx, s.name).Result()
	return err
}

func encodeToStreamValue(event *internal.StreamData) (map[string]interface{}, error) {
	enc, err := internal.EncodeStreamData(event)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		payloadKey: enc,
	}, nil
}

func decodeFromStreamValue(message xredis.XMessage) (*internal.StreamData, error) {
	enc, ok := message.Values[payloadKey]
	if !ok {
		return nil, errors.New("encoding issue")
	}

	streamData, err := internal.DecodeStreamData([]byte(enc.(string)))
	if err != nil {
		return nil, err
	}

	streamData.Id = message.ID
	return streamData, nil
}
