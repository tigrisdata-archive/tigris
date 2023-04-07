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

	"github.com/tigrisdata/tigris/internal"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
)

func checkStats(t *testing.T, ctx context.Context, table []byte, store TxStore, sb int32, rc int32) {
	stats, err := store.GetTableStats(ctx, table)
	require.NoError(t, err)
	require.Equal(t, sb, stats.StoredBytes)
	require.Equal(t, rc, stats.RowCount)
}

func TestStatsStore(t *testing.T) {
	cfg, err := config.GetTestFDBConfig("../..")
	require.NoError(t, err)

	kv, err := newFoundationDB(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	table := []byte("t1")
	require.NoError(t, kv.DropTable(ctx, table))
	require.NoError(t, kv.CreateTable(ctx, table))

	store, err := NewTxStore(kv)
	require.NoError(t, err)

	statsStore := NewStatsStore(store, kv)

	tx, err := statsStore.BeginTx(ctx)
	require.NoError(t, err)

	checkStats(t, ctx, table, store, 100, 100)

	payload23 := strings.Repeat("a", 23)
	err = tx.Insert(ctx, table, BuildKey("p1_", 1), &internal.TableData{RawData: []byte(payload23)})
	require.NoError(t, err)

	checkStats(t, ctx, table, store, 100, 100)

	// increase size
	payload37 := strings.Repeat("a", 37)
	err = tx.Replace(ctx, table, BuildKey("p1_", 1), &internal.TableData{RawData: []byte(payload37)}, false)
	require.NoError(t, err)

	checkStats(t, ctx, table, store, 100, 100)

	// decrease size
	err = tx.Replace(ctx, table, BuildKey("p1_", 1), &internal.TableData{RawData: []byte(payload23)}, false)
	require.NoError(t, err)

	checkStats(t, ctx, table, store, 100, 100)

	err = tx.Replace(ctx, table, BuildKey("p2_", 1), &internal.TableData{RawData: []byte(payload37)}, false)
	require.NoError(t, err)

	checkStats(t, ctx, table, store, 100, 100)

	err = tx.Delete(ctx, table, BuildKey("pnon_existing", 1))
	require.NoError(t, err)

	checkStats(t, ctx, table, store, 100, 100)

	err = tx.Delete(ctx, table, BuildKey("p2_", 1))
	require.NoError(t, err)

	checkStats(t, ctx, table, store, 100, 100)
}
