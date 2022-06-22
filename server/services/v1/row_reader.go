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

package v1

import (
	"context"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type Row struct {
	Key  []byte
	Data *internal.TableData
}

type RowReader interface {
	Next(context.Context, *Row) bool
	Err() error
}

type DatabaseRowReader struct {
	idx        int
	err        error
	keys       []keys.Key
	tx         transaction.Tx
	ctx        context.Context
	kvIterator kv.Iterator
}

func MakeDatabaseRowReader(ctx context.Context, tx transaction.Tx, keys []keys.Key) (*DatabaseRowReader, error) {
	d := &DatabaseRowReader{
		idx:  0,
		tx:   tx,
		ctx:  ctx,
		keys: keys,
	}
	if d.idx >= len(d.keys) {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "no keys to read")
	}
	if d.kvIterator, d.err = d.readNextKey(d.ctx, d.keys[d.idx]); d.err != nil {
		return nil, d.err
	}

	return d, nil
}

func (d *DatabaseRowReader) Next(_ context.Context, row *Row) bool {
	if d.err != nil {
		return false
	}

	for {
		var keyValue kv.KeyValue
		if d.kvIterator.Next(&keyValue) {
			row.Key = keyValue.FDBKey
			row.Data = keyValue.Data
			return true
		}
		if d.kvIterator.Err() != nil {
			d.err = d.kvIterator.Err()
			return false
		}

		d.idx++
		if d.idx == len(d.keys) {
			return false
		}

		d.kvIterator, d.err = d.readNextKey(d.ctx, d.keys[d.idx])
	}
}

func (d *DatabaseRowReader) readNextKey(ctx context.Context, key keys.Key) (kv.Iterator, error) {
	it, err := d.tx.Read(ctx, key)
	if ulog.E(err) {
		return nil, err
	}
	return it, nil
}

func (d *DatabaseRowReader) Err() error { return d.err }
