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
	"reflect"

	"github.com/tigrisdata/tigris/internal"
	ulog "github.com/tigrisdata/tigris/util/log"
)

const (
	StatsSizeKey     = "size"
	StatsRowCountKey = "count"

	StatsSearchFieldsKey = "search_size"
)

var StatsTable = []byte("stats")

type (
	CtxValueSize  struct{}
	CtxSearchSize struct{}
)

type StatsTxStore struct {
	TxStore
	sa ShardedAtomics
}

func NewStatsStore(store TxStore) TxStore {
	return &StatsTxStore{TxStore: store, sa: NewShardedAtomics(store)}
}

func (store *StatsTxStore) BeginTx(ctx context.Context) (Tx, error) {
	btx, err := store.TxStore.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	return &StatsTx{
		Tx: btx,
		sa: store.sa,
	}, nil
}

func (store *StatsTxStore) DropTable(ctx context.Context, name []byte) error {
	tx, err := store.TxStore.BeginTx(ctx)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback(ctx) }()

	if err = tx.Delete(ctx, name, nil); err != nil {
		return err
	}

	if err = tx.Delete(ctx, StatsTable, BuildKey(name)); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (store *StatsTxStore) GetTableStats(ctx context.Context, table []byte) (*TableStats, error) {
	stats, err := store.TxStore.GetTableStats(ctx, table)
	if err != nil {
		return nil, err
	}

	tx, err := store.TxStore.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	defer func() { _ = tx.Rollback(ctx) }()

	stats.RowCount, err = store.sa.AtomicReadTx(ctx, tx, StatsTable, BuildKey(table, StatsRowCountKey))
	if err != nil {
		return nil, err
	}

	stats.StoredBytes, err = store.sa.AtomicReadTx(ctx, tx, StatsTable, BuildKey(table, StatsSizeKey))
	if err != nil {
		return nil, err
	}

	stats.SearchFieldsSize, err = store.sa.AtomicReadTx(ctx, tx, StatsTable, BuildKey(table, StatsSearchFieldsKey))
	if err != nil {
		return nil, err
	}

	// TODO: Temporary. Used to restore statistics for existing collections.
	// TODO: Can be removed for all new installations or after it run at least once
	// TODO: for every existing collection.
	if stats.RowCount == 0 && stats.StoredBytes == 0 {
		stats.StoredBytes = stats.OnDiskSize
		err = store.sa.AtomicAdd(ctx, StatsTable, BuildKey(table, StatsSizeKey), stats.OnDiskSize)
		if err != nil {
			return nil, err
		}
	}

	return stats, nil
}

type StatsTx struct {
	Tx
	sa ShardedAtomics
}

func (tx *StatsTx) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	data.RawSize = data.Size()

	if err := tx.Tx.Insert(ctx, table, key, data); err != nil {
		return err
	}

	if err := tx.sa.AtomicAddTx(ctx, tx, StatsTable, BuildKey(table, StatsRowCountKey), 1); err != nil {
		return err
	}

	return tx.sa.AtomicAddTx(ctx, tx, StatsTable, BuildKey(table, StatsSizeKey), int64(data.RawSize))
}

func (tx *StatsTx) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error {
	data.RawSize = data.Size()

	prevSize := GetSizeFromCtx(ctx)
	// do not read from disk if value is known and set in the data parameter
	if prevSize == 0 {
		prev, err := tx.GetMetadata(ctx, table, key)
		switch {
		case err == nil:
			prevSize = prev.RawSize
		case err == ErrNotFound:
			// adding new row
			if err := tx.sa.AtomicAddTx(ctx, tx, StatsTable, BuildKey(table, StatsRowCountKey), 1); err != nil {
				return err
			}
		default:
			return convertFDBToStoreErr(err)
		}
	}

	if err := tx.Tx.Replace(ctx, table, key, data, isUpdate); err != nil {
		return err
	}

	return tx.sa.AtomicAddTx(ctx, tx, StatsTable, BuildKey(table, StatsSizeKey), int64(data.RawSize-prevSize))
}

func (tx *StatsTx) Delete(ctx context.Context, table []byte, key Key) error {
	prevSize := GetSizeFromCtx(ctx)
	if prevSize == 0 {
		prev, err := tx.GetMetadata(ctx, table, key)
		switch {
		case err == nil:
			prevSize = prev.RawSize
		case err == ErrNotFound:
			return tx.Tx.Delete(ctx, table, key) // try prefix delete
		default:
			return err
		}
	}

	if err := tx.Tx.Delete(ctx, table, key); err != nil {
		return err
	}

	if err := tx.sa.AtomicAddTx(ctx, tx, StatsTable, BuildKey(table, StatsRowCountKey), -1); err != nil {
		return err
	}

	return tx.sa.AtomicAddTx(ctx, tx, StatsTable, BuildKey(table, StatsSizeKey), int64(-prevSize))
}

func GetSizeFromCtx(ctx context.Context) int32 {
	if v := ctx.Value(CtxValueSize{}); v != nil {
		if vv, ok := v.(int32); ok {
			return vv
		}

		if vv, ok := v.(int); ok {
			return int32(vv)
		}

		_ = ulog.CE("unexpected ctx value %v", reflect.TypeOf(v))
	}

	return 0
}

func CtxWithSize(ctx context.Context, size int32) context.Context {
	return context.WithValue(ctx, CtxValueSize{}, size)
}
