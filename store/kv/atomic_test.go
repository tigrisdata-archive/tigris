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

package kv

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
)

func TestShardedAtomics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg, err := config.GetTestFDBConfig("../..")
	require.NoError(t, err)

	bkv, err := newFoundationDB(cfg)
	require.NoError(t, err)

	kv := NewTxStore(bkv)

	sa := NewShardedAtomics(kv)

	testTable := []byte(`atomic_test_table`)
	testKey := BuildKey("atomic_test_key")

	err = bkv.Delete(ctx, testTable, testKey)
	require.NoError(t, err)

	err = sa.AtomicAdd(ctx, testTable, testKey, 10)
	require.NoError(t, err)

	err = sa.AtomicAdd(ctx, testTable, testKey, 100)
	require.NoError(t, err)

	err = sa.AtomicAdd(ctx, testTable, testKey, -20)
	require.NoError(t, err)

	val, err := sa.AtomicRead(ctx, testTable, testKey)
	require.NoError(t, err)
	assert.Equal(t, int64(90), val)

	testKey.AddPart("composite").AddPart(123)

	err = bkv.Delete(ctx, testTable, testKey)
	require.NoError(t, err)

	err = sa.AtomicAdd(ctx, testTable, testKey, 100000)
	require.NoError(t, err)

	err = sa.AtomicAdd(ctx, testTable, testKey, -2000)
	require.NoError(t, err)

	val, err = sa.AtomicRead(ctx, testTable, testKey)
	require.NoError(t, err)
	assert.Equal(t, int64(98000), val)
}
