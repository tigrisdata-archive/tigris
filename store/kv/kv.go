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

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
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

type TableStats struct {
	StoredBytes     int64
	OnDiskSize      int64
	RowCount        int64
	SearchIndexSize int64
}

type KV interface {
	Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error
	Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error
	Delete(ctx context.Context, table []byte, key Key) error
	Read(ctx context.Context, table []byte, key Key) (Iterator, error)
	ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool) (Iterator, error)

	GetMetadata(ctx context.Context, table []byte, key Key) (*internal.TableData, error)

	SetVersionstampedKey(_ context.Context, key []byte, value []byte) error
	SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte, isSnapshot bool) Future
	AtomicAdd(ctx context.Context, table []byte, key Key, value int64) error
	AtomicRead(ctx context.Context, table []byte, key Key) (int64, error)
}

type Tx interface {
	KV
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
	GetTableStats(ctx context.Context, name []byte) (*TableStats, error)
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

type Builder struct {
	isCompression bool
	isChunking    bool
	isMeasure     bool
	isListener    bool
	isStats       bool
}

func NewBuilder() *Builder {
	return &Builder{}
}

// Build will create the TxStore in an order. For example, a simple kv is created first then chunk store is created
// using this simple kv. Listener enabled will be added after chunking so that it is called before chunking. Finally,
// the measure at the end.
func (b *Builder) Build(cfg *config.FoundationDBConfig) (TxStore, error) {
	kv, err := newFoundationDB(cfg)
	if err != nil {
		return nil, err
	}

	store, err := NewTxStore(kv)
	if err != nil {
		return nil, err
	}
	if b.isChunking {
		store = NewChunkStore(store)
	}
	if b.isListener {
		store = NewListenerStore(store)
	}
	if b.isStats {
		store = NewStatsStore(store, kv)
	}
	if b.isMeasure {
		store = NewKeyValueStoreWithMetrics(store)
	}

	return store, nil
}

func (b *Builder) WithMeasure() *Builder {
	b.isMeasure = true
	return b
}

func (b *Builder) WithListener() *Builder {
	b.isListener = true
	return b
}

func (b *Builder) WithCompression() *Builder {
	b.isCompression = true
	return b
}

func (b *Builder) WithChunking() *Builder {
	b.isChunking = true
	return b
}

func (b *Builder) WithStats() *Builder {
	b.isStats = true
	return b
}
