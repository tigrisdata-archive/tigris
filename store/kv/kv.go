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

package kv

import (
	"context"
	"unsafe"

	"github.com/tigrisdata/tigrisdb/internal"
	"github.com/tigrisdata/tigrisdb/server/config"
)

type KeyValue struct {
	Key    Key
	FDBKey []byte
	Data   *internal.TableData
}

type KV interface {
	Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error
	Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) error
	Delete(ctx context.Context, table []byte, key Key) error
	DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error
	Read(ctx context.Context, table []byte, key Key) (Iterator, error)
	ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (Iterator, error)
	Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) error
	UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) error
	SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
}

type Tx interface {
	KV
	Commit(context.Context) error
	Rollback(context.Context) error
}

type KeyValueStore interface {
	KV
	Tx(ctx context.Context) (Tx, error)
	CreateTable(ctx context.Context, name []byte) error
	DropTable(ctx context.Context, name []byte) error
}

type Iterator interface {
	Next(value *KeyValue) bool
	Err() error
}

type KeyValueStoreImpl struct {
	*fdbkv
}

func NewKeyValueStore(cfg *config.FoundationDBConfig) (KeyValueStore, error) {
	kv, err := newFoundationDB(cfg)
	if err != nil {
		return nil, err
	}
	return &KeyValueStoreImpl{
		fdbkv: kv,
	}, nil
}

func (k *KeyValueStoreImpl) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return k.fdbkv.Insert(ctx, table, key, enc)
}

func (k *KeyValueStoreImpl) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return k.fdbkv.Replace(ctx, table, key, enc)
}

func (k *KeyValueStoreImpl) Read(ctx context.Context, table []byte, key Key) (Iterator, error) {
	iter, err := k.fdbkv.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}
	return &IteratorImpl{
		baseIterator: iter,
	}, nil
}

func (k *KeyValueStoreImpl) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (Iterator, error) {
	iter, err := k.fdbkv.ReadRange(ctx, table, lkey, rkey)
	if err != nil {
		return nil, err
	}
	return &IteratorImpl{
		baseIterator: iter,
	}, nil
}

func (k *KeyValueStoreImpl) Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) error {
	return k.fdbkv.Update(ctx, table, key, func(existing []byte) ([]byte, error) {
		decoded, err := internal.Decode(existing)
		if err != nil {
			return nil, err
		}

		newData, err := apply(decoded)
		if err != nil {
			return nil, err
		}

		encoded, err := internal.Encode(newData)
		if err != nil {
			return nil, err
		}

		return encoded, nil
	})
}

func (k *KeyValueStoreImpl) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) error {
	return k.fdbkv.UpdateRange(ctx, table, lKey, rKey, func(existing []byte) ([]byte, error) {
		decoded, err := internal.Decode(existing)
		if err != nil {
			return nil, err
		}

		newData, err := apply(decoded)
		if err != nil {
			return nil, err
		}

		encoded, err := internal.Encode(newData)
		if err != nil {
			return nil, err
		}

		return encoded, nil
	})
}

func (k *KeyValueStoreImpl) Tx(ctx context.Context) (Tx, error) {
	btx, err := k.fdbkv.Tx(ctx)
	if err != nil {
		return nil, err
	}

	return &TxImpl{
		ftx: btx.(*ftx),
	}, nil
}

type TxImpl struct {
	*ftx
}

func (tx *TxImpl) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return tx.ftx.Insert(ctx, table, key, enc)
}

func (tx *TxImpl) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return tx.ftx.Replace(ctx, table, key, enc)
}

func (tx *TxImpl) Read(ctx context.Context, table []byte, key Key) (Iterator, error) {
	iter, err := tx.ftx.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}
	return &IteratorImpl{
		baseIterator: iter,
	}, nil
}

func (tx *TxImpl) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (Iterator, error) {
	iter, err := tx.ftx.ReadRange(ctx, table, lkey, rkey)
	if err != nil {
		return nil, err
	}
	return &IteratorImpl{
		baseIterator: iter,
	}, nil
}

func (tx *TxImpl) Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) error {
	return tx.ftx.Update(ctx, table, key, func(existing []byte) ([]byte, error) {
		decoded, err := internal.Decode(existing)
		if err != nil {
			return nil, err
		}

		newData, err := apply(decoded)
		if err != nil {
			return nil, err
		}

		encoded, err := internal.Encode(newData)
		if err != nil {
			return nil, err
		}

		return encoded, nil
	})
}

func (tx *TxImpl) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) error {
	return tx.ftx.UpdateRange(ctx, table, lKey, rKey, func(existing []byte) ([]byte, error) {
		decoded, err := internal.Decode(existing)
		if err != nil {
			return nil, err
		}

		newData, err := apply(decoded)
		if err != nil {
			return nil, err
		}

		encoded, err := internal.Encode(newData)
		if err != nil {
			return nil, err
		}

		return encoded, nil
	})
}

type IteratorImpl struct {
	baseIterator
	err error
}

func (i *IteratorImpl) Next(value *KeyValue) bool {
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

func (i *IteratorImpl) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.baseIterator.Err()
}

type KeyPart interface{}
type Key []KeyPart

func BuildKey(parts ...interface{}) Key {
	ptr := unsafe.Pointer(&parts)
	return *(*Key)(ptr)
}

func (k *Key) AddPart(part interface{}) {
	*k = append(*k, KeyPart(part))
}
