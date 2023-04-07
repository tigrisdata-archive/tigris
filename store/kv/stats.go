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

	"github.com/tigrisdata/tigris/internal"
)

const (
	DefaultNumShards = 64
	StatsSizeKey     = "__SIZE__"
	StatsRowCountKey = "__COUNT__"

	StatsSearchIndexKey = "__SEARCH_SIZE__"
)

type CtxValueSize struct{}
type CtxSearchSize struct{}

type StatsTxStore struct {
	TxStore
	base baseKVStore
	sa   ShardedAtomics
}

func NewStatsStore(store TxStore, kv baseKVStore) TxStore {
	return &StatsTxStore{TxStore: store, base: kv, sa: NewShardedAtomics(kv)}
}

func (store *StatsTxStore) BeginTx(ctx context.Context) (Tx, error) {
	btx, err := store.TxStore.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	return &StatsTx{
		KeyValueTx: btx.(*KeyValueTx),
		sa:         store.sa,
	}, nil
}

func (store *StatsTxStore) GetTableStats(ctx context.Context, table []byte) (*TableStats, error) {
	stats, err := store.TxStore.GetTableStats(ctx, table)
	if err != nil {
		return nil, err
	}

	tx, err := store.base.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	defer func() { _ = tx.Rollback(ctx) }()

	stats.RowCount, err = store.sa.AtomicRead(ctx, tx, table, BuildKey(StatsRowCountKey))
	if err != nil {
		return nil, err
	}

	stats.StoredBytes, err = store.sa.AtomicRead(ctx, tx, table, BuildKey(StatsSizeKey))
	if err != nil {
		return nil, err
	}

	stats.SearchIndexSize, err = store.sa.AtomicRead(ctx, tx, table, BuildKey(StatsSearchIndexKey))
	if err != nil {
		return nil, err
	}

	return stats, nil
}

type StatsTx struct {
	*KeyValueTx
	sa ShardedAtomics
}

func (tx *StatsTx) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	data.ValueSize = int32(len(data.RawData))

	if err := tx.KeyValueTx.Insert(ctx, table, key, data); err != nil {
		return err
	}

	if err := tx.sa.AtomicAdd(ctx, tx.ftx, table, BuildKey(StatsRowCountKey), 1); err != nil {
		return err
	}

	return tx.sa.AtomicAdd(ctx, tx.ftx, table, BuildKey(StatsSizeKey), int64(data.ValueSize))
}

func (tx *StatsTx) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error {
	prevSize := GetSizeFromCtx(ctx)
	if prevSize == 0 {
		prevSize = data.ValueSize
	}

	data.ValueSize = int32(len(data.RawData))

	// do not read from disk if value is known and set in the data parameter
	if prevSize == 0 {
		prev, err := tx.KeyValueTx.GetMetadata(ctx, table, key)
		if err != nil {
			return err
		}

		prevSize = prev.ValueSize
	}

	if err := tx.KeyValueTx.Replace(ctx, table, key, data, isUpdate); err != nil {
		return err
	}

	return tx.sa.AtomicAdd(ctx, tx.ftx, table, BuildKey(StatsSizeKey), int64(data.ValueSize-prevSize))
}

func (tx *StatsTx) Delete(ctx context.Context, table []byte, key Key) error {
	prevSize := GetSizeFromCtx(ctx)
	if prevSize == 0 {
		prev, err := tx.KeyValueTx.GetMetadata(ctx, table, key)
		if err != nil {
			return err
		}

		prevSize = prev.ValueSize
	}

	if err := tx.KeyValueTx.Delete(ctx, table, key); err != nil {
		return err
	}

	if err := tx.sa.AtomicAdd(ctx, tx.ftx, table, BuildKey(StatsRowCountKey), -1); err != nil {
		return err
	}

	return tx.sa.AtomicAdd(ctx, tx.ftx, table, BuildKey(StatsSizeKey), int64(-prevSize))
}

func GetSizeFromCtx(ctx context.Context) int32 {
	if v := ctx.Value(CtxValueSize{}); v != nil {
		return v.(int32)
	}

	return 0
}

func GetSearchSizeFromCtx(ctx context.Context) int32 {
	if v := ctx.Value(CtxSearchSize{}); v != nil {
		return v.(int32)
	}

	return 0
}
