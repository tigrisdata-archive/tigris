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
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

type Stream interface {
	Name() string
	// Add is to add streamData to a stream
	Add(ctx context.Context, value *internal.StreamData) (string, error)
	// Read data from the stream, returns data ID greater than position. To read from current use "$"
	Read(ctx context.Context, pos string) (*StreamMessages, bool, error)
	// ReadGroup is similar to Read but with support for reading from a group. We don't have multiple consumers in a
	// single group. Currently, it creates an internal _tigris_consumer.
	ReadGroup(ctx context.Context, group string, pos ReadGroupPos) (*StreamMessages, bool, error)
	// CreateConsumerGroup creates a consumer group and attach it to the stream. The pos is used to specify the position
	// for this consumer group.
	CreateConsumerGroup(ctx context.Context, group string, pos string) error
	// RemoveConsumerGroup removes consumer group from this stream.
	RemoveConsumerGroup(ctx context.Context, group string) error
	// GetConsumerGroups returns all the consumer groups attached to this stream
	GetConsumerGroups(ctx context.Context) ([]xredis.XInfoGroup, error)
	// GetConsumerGroup returns only information about the consumer group passed in the API.
	GetConsumerGroup(ctx context.Context, group string) (*xredis.XInfoGroup, bool, error)
	// SetID is used to set the position of the group again.
	SetID(ctx context.Context, group string, pos string) error
	// Ack is to acknowledge messages once they are read by the consumer group. This is required to be called in case
	// ReadGroup is used so that messages doesn't end up in pending entries list.
	Ack(ctx context.Context, group string, ids ...string) error
	// Delete is to delete this stream. it removes all the associated consumer group as well.
	Delete(ctx context.Context) error
}

type SetOptions struct {
	// NX is SetIfNotExists i.e. only set the key if it does not already exist.
	NX bool
	// XX is SetIfExists i.e. only set the key if it already exists.
	XX bool
	// EX sets the Expiry time of the key in seconds
	EX time.Duration
}

type GetOptions struct {
	// Expiry set the expiration time of the key
	Expiry    time.Duration
	GetDelete bool
}

type Cache interface {
	Set(ctx context.Context, tableName string, key string, value *internal.CacheData, options *SetOptions) error
	// GetSet is to get the previous value and set the new value
	GetSet(ctx context.Context, tableName string, key string, value *internal.CacheData) (*internal.CacheData, error)
	// Get the value of key
	Get(ctx context.Context, tableName string, key string, options *GetOptions) (*internal.CacheData, error)
	// Delete deletes one or more keys
	Delete(ctx context.Context, tableName string, keys ...string) (int64, error)
	// Exists returns if the key exists, for multiple keys it returns the count of the number of keys that exists
	Exists(ctx context.Context, tableName string, key ...string) (int64, error)
	Keys(ctx context.Context, tableName string, pattern string) ([]string, error)

	// CreateStream creates and returns a stream object, throws an error if stream already exists
	CreateStream(ctx context.Context, streamName string) (Stream, error)
	// CreateOrGetStream creates or returns an existing stream
	CreateOrGetStream(ctx context.Context, streamName string) (Stream, error)
	// GetStream only returns a stream if the stream exists in the Cache
	GetStream(ctx context.Context, streamName string) (Stream, error)
	// ListStreams returns the all the streams with the prefix
	ListStreams(ctx context.Context, streamNamePrefix string) ([]string, error)
	// DeleteStream to delete a stream if exists
	DeleteStream(ctx context.Context, streamName string) error
}

func NewCache(cfg *config.CacheConfig) Cache {
	return newCache(cfg)
}
