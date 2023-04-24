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

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/internal"
)

type KeyValueTxStore struct {
	*fdbkv
}

func NewTxStore(kv *fdbkv) TxStore {
	return newTxStore(kv)
}

func newTxStore(kv *fdbkv) *KeyValueTxStore {
	return &KeyValueTxStore{fdbkv: kv}
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

func (k *KeyValueTxStore) GetTableStats(ctx context.Context, table []byte) (*TableStats, error) {
	sz, err := k.TableSize(ctx, table)
	if err != nil {
		return nil, err
	}

	return &TableStats{OnDiskSize: sz}, nil
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

func (tx *KeyValueTx) Read(ctx context.Context, table []byte, key Key, reverse bool) (Iterator, error) {
	iter, err := tx.ftx.Read(ctx, table, key, false, reverse)
	if err != nil {
		return nil, err
	}

	return NewKeyValueIterator(ctx, iter), nil
}

func (tx *KeyValueTx) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool, reverse bool) (Iterator, error) {
	iter, err := tx.ftx.ReadRange(ctx, table, lkey, rkey, isSnapshot, reverse)
	if err != nil {
		return nil, err
	}

	return NewKeyValueIterator(ctx, iter), nil
}

func (tx *KeyValueTx) GetMetadata(ctx context.Context, table []byte, key Key) (*internal.TableData, error) {
	b, err := tx.ftx.Get(ctx, getFDBKey(table, key), true).Get()
	if err != nil {
		return nil, err
	}

	if len(b) == 0 {
		return nil, ErrNotFound
	}

	log.Debug().Int("length", len(b)).Msg("getMetadata")

	return internal.Decode(b)
}

type KeyValueIterator struct {
	ctx context.Context
	baseIterator
	err error
}

func NewKeyValueIterator(ctx context.Context, iter baseIterator) *KeyValueIterator {
	return &KeyValueIterator{ctx: ctx, baseIterator: iter}
}

func (i *KeyValueIterator) Next(value *KeyValue) bool {
	var v baseKeyValue

	if !i.baseIterator.Next(&v) {
		i.err = i.baseIterator.Err()
		return false
	}

	value.Key = v.Key
	value.FDBKey = v.FDBKey
	value.Data, i.err = internal.Decode(v.Value)

	return i.err == nil
}

func (i *KeyValueIterator) Err() error {
	if i.err != nil {
		return i.err
	}

	return i.baseIterator.Err()
}
