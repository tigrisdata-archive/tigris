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
	"unsafe"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/tigrisdata/tigris/internal"
)

type KeyValue struct {
	Key    Key
	FDBKey []byte
	Data   *internal.TableData
}

type fdbBaseType interface {
	[]byte | int64
}

// FdbBaseKeyValue type for when we are not iterating over TableData.
type FdbBaseKeyValue[T fdbBaseType] struct {
	Key    Key
	FDBKey []byte
	Data   T
}

type Future fdb.FutureByteSlice

type Tx interface {
	Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error
	Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error
	Delete(ctx context.Context, table []byte, key Key) error
	DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error
	Read(ctx context.Context, table []byte, key Key) (Iterator, error)
	ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool) (Iterator, error)
	Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error)
	UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error)
	SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error
	SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte, isSnapshot bool) (Future, error)
	AtomicAdd(ctx context.Context, table []byte, key Key, value int64) error
	AtomicRead(ctx context.Context, table []byte, key Key) (int64, error)
	AtomicReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool) (AtomicIterator, error)
	Commit(context.Context) error
	Rollback(context.Context) error
	IsRetriable() bool
	RangeSize(ctx context.Context, table []byte, lkey Key, rkey Key) (int64, error)
}

type TxStore interface {
	BeginTx(ctx context.Context) (Tx, error)

	CreateTable(ctx context.Context, name []byte) error
	DropTable(ctx context.Context, name []byte) error
	GetInternalDatabase() (interface{}, error) // TODO: CDC remove workaround
	TableSize(ctx context.Context, name []byte) (int64, error)
}

type Iterator interface {
	Next(value *KeyValue) bool
	Err() error
}

type AtomicIterator interface {
	Next(value *FdbBaseKeyValue[int64]) bool
	Err() error
}

type (
	KeyPart interface{}
	Key     []KeyPart
)

func BuildKey(parts ...interface{}) Key {
	ptr := unsafe.Pointer(&parts)
	return *(*Key)(ptr)
}

func (k *Key) AddPart(part interface{}) {
	*k = append(*k, KeyPart(part))
}

type KeyValueStore interface {
	TxStore

	Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error
	Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error
	Delete(ctx context.Context, table []byte, key Key) error
	DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error
	Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error)
	UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error)
}

type KeyValueStoreImpl struct {
	TxStore
}

func (d *KeyValueStoreImpl) tx(ctx context.Context, fn func(tr Tx) error) error {
	tr, err := d.BeginTx(ctx)
	if err != nil {
		return err
	}

	defer func() { _ = tr.Rollback(ctx) }()

	if err = fn(tr); err != nil {
		return err
	}

	return tr.Commit(ctx)
}

func (d *KeyValueStoreImpl) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	return d.tx(ctx, func(tr Tx) error {
		return tr.Insert(ctx, table, key, data)
	})
}

func (d *KeyValueStoreImpl) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error {
	return d.tx(ctx, func(tr Tx) error {
		return tr.Replace(ctx, table, key, data, isUpdate)
	})
}

func (d *KeyValueStoreImpl) Delete(ctx context.Context, table []byte, key Key) error {
	return d.tx(ctx, func(tr Tx) error {
		return tr.Delete(ctx, table, key)
	})
}

func (d *KeyValueStoreImpl) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error {
	return d.tx(ctx, func(tr Tx) error {
		return tr.DeleteRange(ctx, table, lKey, rKey)
	})
}

func (d *KeyValueStoreImpl) Update(ctx context.Context, table []byte, key Key, apply func(data *internal.TableData) (*internal.TableData, error)) (int32, error) {
	var i int32
	var err error

	err = d.tx(ctx, func(tr Tx) error {
		i, err = tr.Update(ctx, table, key, apply)
		return err
	})

	return i, err
}

func (d *KeyValueStoreImpl) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(data *internal.TableData) (*internal.TableData, error)) (int32, error) {
	var i int32
	var err error

	err = d.tx(ctx, func(tr Tx) error {
		i, err = tr.UpdateRange(ctx, table, lKey, rKey, apply)
		return err
	})

	return i, err
}
