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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

func checkStats(t *testing.T, ctx context.Context, table []byte, store TxStore, sb int64, rc int64) {
	stats, err := store.GetTableStats(ctx, table)
	require.NoError(t, err)
	require.Equal(t, sb, stats.StoredBytes)
	require.Equal(t, rc, stats.RowCount)
}

func tx(t *testing.T, ctx context.Context, s TxStore, fn func(tx Tx)) {
	tx, err := s.BeginTx(ctx)
	require.NoError(t, err)

	fn(tx)

	err = tx.Commit(ctx)
	require.NoError(t, err)
}

func TestStatsStore(t *testing.T) {
	cfg, err := config.GetTestFDBConfig("../..")
	require.NoError(t, err)

	kv, err := newFoundationDB(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
	defer cancel()

	store := NewTxStore(kv)

	statsStore := NewStatsStore(store)

	table := []byte("t1")
	require.NoError(t, statsStore.DropTable(ctx, table))
	require.NoError(t, kv.CreateTable(ctx, table))

	checkStats(t, ctx, table, statsStore, 0, 0)

	payload23 := strings.Repeat("a", 23)

	tx(t, ctx, statsStore, func(tx Tx) {
		// insert data 23 bytes long
		err = tx.Insert(ctx, table, BuildKey("p1_", 1), &internal.TableData{RawData: []byte(payload23)})
		require.NoError(t, err)
	})

	checkStats(t, ctx, table, statsStore, 23, 1)

	payload37 := strings.Repeat("a", 37)

	tx(t, ctx, statsStore, func(tx Tx) {
		// increase size to 37
		err = tx.Replace(ctx, table, BuildKey("p1_", 1), &internal.TableData{RawData: []byte(payload37)}, false)
		require.NoError(t, err)
	})

	checkStats(t, ctx, table, statsStore, 37, 1)

	tx(t, ctx, statsStore, func(tx Tx) {
		// decrease size back to 23
		err = tx.Replace(ctx, table, BuildKey("p1_", 1), &internal.TableData{RawData: []byte(payload23)}, false)
		require.NoError(t, err)
	})

	checkStats(t, ctx, table, statsStore, 23, 1)

	tx(t, ctx, statsStore, func(tx Tx) {
		// insert another row. 37 bytes long
		err = tx.Replace(ctx, table, BuildKey("p2_", 1), &internal.TableData{RawData: []byte(payload37)}, false)
		require.NoError(t, err)
	})

	checkStats(t, ctx, table, statsStore, 60, 2)

	tx(t, ctx, statsStore, func(tx Tx) {
		err = tx.Delete(ctx, table, BuildKey("pnon_existing", 1))
		require.NoError(t, err)
	})

	checkStats(t, ctx, table, statsStore, 60, 2)

	tx(t, ctx, statsStore, func(tx Tx) {
		err = tx.Delete(ctx, table, BuildKey("p2_", 1))
		require.NoError(t, err)
	})

	checkStats(t, ctx, table, statsStore, 23, 1)

	tx(t, ctx, statsStore, func(tx Tx) {
		// existing value size is passed in the context
		// results is 23-5+23
		err = tx.Replace(context.WithValue(ctx, CtxValueSize{}, 5), table, BuildKey("p1_", 1), &internal.TableData{RawData: []byte(payload23)}, false)
		require.NoError(t, err)
	})

	checkStats(t, ctx, table, statsStore, 41, 1)

	tx(t, ctx, statsStore, func(tx Tx) {
		err = tx.Delete(context.WithValue(ctx, CtxValueSize{}, 5), table, BuildKey("p2_", 1))
		require.NoError(t, err)
	})

	checkStats(t, ctx, table, statsStore, 36, 0)
}
