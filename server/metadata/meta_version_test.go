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

package metadata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
)

func TestMetaVersion(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	t.Run("read versions", func(t *testing.T) {
		m := &MetaVersion{}
		ctx := context.TODO()
		tm := transaction.NewManager(kv)
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		first, err := m.Read(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		second, err := m.Read(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, first, second)
	})
	t.Run("bump and read", func(t *testing.T) {
		m := &MetaVersion{}
		ctx := context.TODO()
		tm := transaction.NewManager(kv)
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		first, err := m.Read(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		require.NoError(t, m.Increment(ctx, tx))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		second, err := m.Read(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.NotEqual(t, first, second)
	})
}
