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
	"github.com/tigrisdata/tigris/server/config"
)

type KeyValueTxStore struct {
	*fdbkv
}

func NewTxStore(cfg *config.FoundationDBConfig) (TxStore, error) {
	return newTxStore(cfg)
}

func newTxStore(cfg *config.FoundationDBConfig) (*KeyValueTxStore, error) {
	kv, err := newFoundationDB(cfg)
	if err != nil {
		return nil, err
	}
	return &KeyValueTxStore{fdbkv: kv}, nil
}

func (k *KeyValueTxStore) BeginTx(ctx context.Context) (Tx, error) {
	btx, err := k.fdbkv.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	return &KeyValueTx{
		ftx: btx.(*ftx),
	}, nil
}

func (k *KeyValueTxStore) GetInternalDatabase() (interface{}, error) {
	return k.db, nil
}

type KeyValueTx struct {
	*ftx
}

func (tx *KeyValueTx) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return tx.ftx.Insert(ctx, table, key, enc)
}

func (tx *KeyValueTx) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return tx.ftx.Replace(ctx, table, key, enc, isUpdate)
}

func (tx *KeyValueTx) Read(ctx context.Context, table []byte, key Key) (Iterator, error) {
	iter, err := tx.ftx.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}
	return &KeyValueIterator{
		baseIterator: iter,
	}, nil
}

func (tx *KeyValueTx) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool) (Iterator, error) {
	iter, err := tx.ftx.ReadRange(ctx, table, lkey, rkey, isSnapshot)
	if err != nil {
		return nil, err
	}
	return &KeyValueIterator{
		baseIterator: iter,
	}, nil
}

type KeyValueIterator struct {
	baseIterator
	err error
}

func (i *KeyValueIterator) Next(value *KeyValue) bool {
	var v baseKeyValue
	hasNext := i.baseIterator.Next(&v)
	if hasNext {
		value.Key = v.Key
		value.FDBKey = v.FDBKey
		decoded, err := internal.Decode(v.Value)
		if err != nil {
			i.err = err
			return false
		}
		value.Data = decoded
	}
	return hasNext
}

func (i *KeyValueIterator) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.baseIterator.Err()
}

type AtomicIteratorImpl struct {
	baseIterator
	err error
}

func (i *AtomicIteratorImpl) Next(value *FdbBaseKeyValue[int64]) bool {
	var v baseKeyValue
	hasNext := i.baseIterator.Next(&v)
	if hasNext {
		value.Key = v.Key
		value.FDBKey = v.FDBKey
		num, err := fdbByteToInt64(&v.Value)
		if err != nil {
			i.err = err
			return false
		}
		value.Data = num
	}
	return hasNext
}

func (i *AtomicIteratorImpl) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.baseIterator.Err()
}
