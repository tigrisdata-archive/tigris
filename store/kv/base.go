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
)

type baseKeyValue struct {
	Key    Key
	FDBKey []byte
	Value  []byte
}

type baseKV interface {
	Insert(ctx context.Context, table []byte, key Key, data []byte) error
	Replace(ctx context.Context, table []byte, key Key, data []byte) error
	Delete(ctx context.Context, table []byte, key Key) error
	DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error
	Read(ctx context.Context, table []byte, key Key) (baseIterator, error)
	ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (baseIterator, error)
	Update(ctx context.Context, table []byte, key Key, apply func([]byte) ([]byte, error)) (int32, error)
	UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) (int32, error)
	SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte, isSnapshot bool) (Future, error)
}

type baseIterator interface {
	Next(*baseKeyValue) bool
	Err() error
}

type baseTx interface {
	baseKV
	Commit(context.Context) error
	Rollback(context.Context) error
	IsRetriable() bool
}

type baseKVStore interface {
	baseKV
	BeginTx(ctx context.Context) (baseTx, error)
	Batch() (baseTx, error)
	CreateTable(ctx context.Context, name []byte) error
	DropTable(ctx context.Context, name []byte) error
}
