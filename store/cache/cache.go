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

package cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	xredis "github.com/go-redis/redis/v8"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

type cache struct {
	*xredis.Client
}

func newCache(cfg *config.CacheConfig) *cache {
	rdb := xredis.NewClient(&xredis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		DB:   0, // use default DB
	})

	return &cache{
		Client: rdb,
	}
}

func (c *cache) Set(ctx context.Context, tableName string, key string, value *internal.CacheData, options *SetOptions) error {
	cacheKey := encodeToCacheKey(tableName, key)
	enc, err := internal.EncodeCacheData(value)
	if err != nil {
		return err
	}

	args := xredis.SetArgs{}

	if options != nil && options.EX > 0 {
		args.TTL = time.Duration(options.EX) * time.Second
	} else if options != nil && options.PX > 0 {
		args.TTL = time.Duration(options.PX) * time.Millisecond
	}

	if options != nil && options.XX {
		args.Mode = "XX"

		if _, err = c.Client.SetArgs(ctx, cacheKey, enc, args).Result(); err != nil && err == xredis.Nil {
			return ErrKeyNotFound
		}
		return err
	} else if options != nil && options.NX {
		args.Mode = "NX"

		if _, err = c.Client.SetArgs(ctx, cacheKey, enc, args).Result(); err != nil && err == xredis.Nil {
			return ErrKeyAlreadyExists
		}
		return err
	}

	_, err = c.Client.SetArgs(ctx, cacheKey, enc, args).Result()
	return err
}

func (c *cache) GetSet(ctx context.Context, tableName string, key string, value *internal.CacheData) (*internal.CacheData, error) {
	cacheKey := encodeToCacheKey(tableName, key)
	enc, err := internal.EncodeCacheData(value)
	if err != nil {
		return nil, err
	}

	val, err := c.Client.GetSet(ctx, cacheKey, enc).Bytes()
	if err != nil && err != xredis.Nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}

	return internal.DecodeCacheData(val)
}

func (c *cache) Get(ctx context.Context, tableName string, key string, options *GetOptions) (*internal.CacheData, error) {
	cacheKey := encodeToCacheKey(tableName, key)

	var (
		value []byte
		err   error
	)
	switch {
	case options != nil && options.Expiry > 0:
		value, err = c.Client.GetEx(ctx, cacheKey, options.Expiry).Bytes()
	case options != nil && options.GetDelete:
		value, err = c.Client.GetDel(ctx, cacheKey).Bytes()
	default:
		value, err = c.Client.Get(ctx, cacheKey).Bytes()
	}
	if err != nil && err == xredis.Nil {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	return internal.DecodeCacheData(value)
}

func (c *cache) Delete(ctx context.Context, tableName string, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, ErrEmptyKey
	}

	cacheKeys := make([]string, len(keys))
	for i, k := range keys {
		cacheKeys[i] = encodeToCacheKey(tableName, k)
	}

	return c.Client.Del(ctx, cacheKeys...).Result()
}

func (c *cache) Exists(ctx context.Context, tableName string, keys ...string) (int64, error) {
	var cacheKeys []string
	if len(keys) == 0 {
		cacheKeys = append(cacheKeys, tableName)
	} else {
		for _, k := range keys {
			cacheKeys = append(cacheKeys, encodeToCacheKey(tableName, k))
		}
	}

	return c.Client.Exists(ctx, cacheKeys...).Result()
}

func (c *cache) Keys(ctx context.Context, tableName string, pattern string) ([]string, error) {
	return c.Client.Keys(ctx, encodeToCacheKey(tableName, pattern)).Result()
}

func (c *cache) ListStreams(ctx context.Context, streamNamePrefix string) ([]string, error) {
	return c.Client.Keys(ctx, streamNamePrefix).Result()
}

func (c *cache) GetStream(ctx context.Context, streamName string) (Stream, error) {
	exists, err := c.Exists(ctx, streamName)
	if err != nil {
		return nil, err
	}

	if exists == 0 {
		return nil, ErrStreamNotFound
	}

	return NewStream(c, streamName), nil
}

func (c *cache) DeleteStream(ctx context.Context, streamName string) error {
	_, err := c.Client.Del(ctx, streamName).Result()
	return err
}

// CreateOrGetStream will create a stream in Redis. 'streamName' is full qualified name similar to tableName in other
// Apis i.e. caller should be responsible for prepending it with tenant/project etc.
func (c *cache) CreateOrGetStream(ctx context.Context, streamName string) (Stream, error) {
	_, err := c.Client.XGroupCreateMkStream(ctx, streamName, DefaultGroup, "0").Result()
	if err != nil && !strings.Contains(err.Error(), errStrConsGroupAlreadyExists) {
		return nil, err
	}

	return NewStream(c, streamName), nil
}

// CreateStream will throw an error if stream already exists.
func (c *cache) CreateStream(ctx context.Context, streamName string) (Stream, error) {
	if _, err := c.Client.XGroupCreateMkStream(ctx, streamName, DefaultGroup, "0").Result(); err != nil {
		if strings.Contains(err.Error(), errStrConsGroupAlreadyExists) {
			return nil, ErrStreamAlreadyExists
		}

		return nil, err
	}

	return NewStream(c, streamName), nil
}

func encodeToCacheKey(tableName string, key string) string {
	return tableName + ":" + key
}

func decodeCacheKey(key string) (string, string) {
	splits := strings.Split(key, ":")
	return splits[0], splits[1]
}
