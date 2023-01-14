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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

func dropCacheTable(t *testing.T, c *cache, tableName string) {
	keys, err := c.Keys(context.TODO(), tableName, "*")
	require.NoError(t, err)

	for _, k := range keys {
		tableName, key := decodeCacheKey(k)
		_, err := c.Delete(context.TODO(), tableName, key)
		require.NoError(t, err)
	}
}

func TestRedis(t *testing.T) {
	c := newCache(config.GetTestCacheConfig())
	ctx := context.TODO()
	tableName := "cache_test"

	t.Run("set", func(t *testing.T) {
		defer dropCacheTable(t, c, tableName)

		s1 := []byte(`{"a": "b"}`)
		require.NoError(t, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), nil))
		g1, err := c.Get(ctx, tableName, "key1", nil)
		require.NoError(t, err)
		require.Equal(t, s1, g1.RawData)
	})
	t.Run("set_get", func(t *testing.T) {
		defer dropCacheTable(t, c, tableName)

		s1 := []byte(`{"a": "b"}`)
		require.NoError(t, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), nil))

		s2 := []byte(`{"a1": "b1"}`)
		g, err := c.GetSet(ctx, tableName, "key1", internal.NewCacheData(s2))
		require.NoError(t, err)
		require.Equal(t, s1, g.RawData)

		g, err = c.Get(ctx, tableName, "key1", nil)
		require.NoError(t, err)
		require.Equal(t, s2, g.RawData)
	})
	t.Run("set_nx", func(t *testing.T) {
		defer dropCacheTable(t, c, tableName)

		s1 := []byte(`{"a": "b"}`)
		require.Equal(t, ErrKeyNotFound, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), &SetOptions{
			XX: true,
		}))

		require.NoError(t, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), nil))
		require.NoError(t, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), &SetOptions{
			XX: true,
		}))
	})
	t.Run("set_xx", func(t *testing.T) {
		defer dropCacheTable(t, c, tableName)

		s1 := []byte(`{"a": "b"}`)
		require.NoError(t, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), &SetOptions{
			NX: true,
		}))

		require.Equal(t, ErrKeyAlreadyExists, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), &SetOptions{
			NX: true,
		}))
	})
	t.Run("delete", func(t *testing.T) {
		defer dropCacheTable(t, c, tableName)

		s1 := []byte(`{"a": "b"}`)
		require.NoError(t, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), nil))
		g1, err := c.Get(ctx, tableName, "key1", nil)
		require.NoError(t, err)
		require.Equal(t, s1, g1.RawData)
		deleted, err := c.Delete(ctx, tableName, "key1")
		require.NoError(t, err)
		require.Equal(t, int64(1), deleted)
		g1, err = c.Get(ctx, tableName, "key1", nil)
		require.Nil(t, g1)
		require.Equal(t, ErrKeyNotFound, err)
	})
	t.Run("keys", func(t *testing.T) {
		defer dropCacheTable(t, c, tableName)

		s1 := []byte(`{"a": "b"}`)
		require.NoError(t, c.Set(ctx, tableName, "key1", internal.NewCacheData(s1), nil))
		require.NoError(t, c.Set(ctx, tableName, "key2", internal.NewCacheData(s1), nil))

		keys, err := c.Keys(ctx, tableName, "*")
		require.NoError(t, err)
		sort.Strings(keys)
		require.Equal(t, []string{encodeToCacheKey(tableName, "key1"), encodeToCacheKey(tableName, "key2")}, keys)
	})

	t.Run("scan", func(t *testing.T) {
		defer dropCacheTable(t, c, tableName)

		s1 := []byte(`{"a": "b"}`)
		for i := 1; i <= 50; i++ {
			require.NoError(t, c.Set(ctx, tableName, fmt.Sprintf("key%d", i), internal.NewCacheData(s1), nil))
		}

		var totalKeys []string
		var cursor uint64 = 0

		keys, cursor := c.Scan(ctx, tableName, cursor, 10, "*")
		totalKeys = append(totalKeys, keys...)

		for cursor != 0 {
			keys, cursor = c.Scan(ctx, tableName, cursor, 10, "*")
			totalKeys = append(totalKeys, keys...)
		}
		require.Equal(t, 50, len(totalKeys))

		for i := 1; i <= 50; i++ {
			contains := false
			keyToSearch := fmt.Sprintf("key%d", i)
			for _, key := range totalKeys {
				if key == encodeToCacheKey(tableName, keyToSearch) {
					contains = true
					break
				}
			}
			require.Truef(t, contains, "key %s not found", keyToSearch)
		}
	})
}
