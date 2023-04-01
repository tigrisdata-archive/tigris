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

package metadata

import (
	"context"

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

const (
	// generatorSubspaceKey is used to store ids in storage so that we can guarantee uniqueness.
	generatorSubspaceKey = "generator"
	// int32IdKey is the prefix after generator subspace to store int32 counters.
	int32IdKey = "int32_id"
)

// TableKeyGenerator is used to generated keys that may need persistence like counter.
type TableKeyGenerator struct{}

func NewTableKeyGenerator() *TableKeyGenerator {
	return &TableKeyGenerator{}
}

// GenerateCounter is used to generate an id in a transaction for int32 field only. This is mainly used to guarantee
// uniqueness with auto-incremented ids, so what we are doing is reserving this id in storage before returning to the
// caller so that only one id is assigned to one caller.
func (g *TableKeyGenerator) GenerateCounter(ctx context.Context, txMgr *transaction.Manager, table []byte) (int32, error) {
	for {
		tx, err := txMgr.StartTx(ctx)
		if err != nil {
			return -1, err
		}

		var valueI32 int32
		if valueI32, err = g.generateCounter(ctx, tx, table); err != nil {
			_ = tx.Rollback(ctx)
		}

		if err = tx.Commit(ctx); err == nil {
			return valueI32, nil
		}
		if err != kv.ErrConflictingTransaction {
			return -1, err
		}
	}
}

// generateCounter as it is used to generate int32 value, we are simply maintaining a counter. There is a contention to
// generate a counter if it is concurrently getting executed but the generation should be fast then it is best to start
// with this approach.
func (*TableKeyGenerator) generateCounter(ctx context.Context, tx transaction.Tx, table []byte) (int32, error) {
	key := keys.NewKey([]byte(generatorSubspaceKey), table, int32IdKey)
	it, err := tx.Read(ctx, key, false)
	if err != nil {
		return 0, err
	}

	id := uint32(1)
	var row kv.KeyValue
	if it.Next(&row) {
		id = ByteToUInt32(row.Data.RawData) + uint32(1)
	}
	if err := it.Err(); err != nil {
		return 0, err
	}

	if err := tx.Replace(ctx, key, internal.NewTableData(UInt32ToByte(id)), false); err != nil {
		return 0, err
	}

	return int32(id), nil
}

func (*TableKeyGenerator) removeCounter(ctx context.Context, tx transaction.Tx, table []byte) error {
	key := keys.NewKey([]byte(generatorSubspaceKey), table, int32IdKey)
	return tx.Delete(ctx, key)
}
