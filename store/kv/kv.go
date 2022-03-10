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
)

type KeyValue struct {
	Key    Key
	FDBKey []byte
	Value  []byte
}

type crud interface {
	Insert(ctx context.Context, table string, key Key, data []byte) error
	Replace(ctx context.Context, table string, key Key, data []byte) error
	Delete(ctx context.Context, table string, key Key) error
	DeleteRange(ctx context.Context, table string, lKey Key, rKey Key) error
	Read(ctx context.Context, table string, key Key) (Iterator, error)
	ReadRange(ctx context.Context, table string, lkey Key, rkey Key) (Iterator, error)
	Update(ctx context.Context, table string, key Key, apply func([]byte) ([]byte, error)) error
	UpdateRange(ctx context.Context, table string, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) error
}

type Tx interface {
	crud
	Commit(context.Context) error
	Rollback(context.Context) error
}

type KV interface {
	crud
	Tx(ctx context.Context) (Tx, error)
	Batch() (Tx, error)
	CreateTable(ctx context.Context, name string) error
	DropTable(ctx context.Context, name string) error
}

type Iterator interface {
	More() bool
	Next() (*KeyValue, error)
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
