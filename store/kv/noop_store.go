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

	"github.com/tigrisdata/tigris/internal"
)

type NoopIterator struct{}

func (n *NoopIterator) Next(value *KeyValue) bool { return false }
func (n *NoopIterator) Err() error                { return nil }

type NoopTx struct {
	*NoopKV
}

func (n *NoopTx) Commit(context.Context) error   { return nil }
func (n *NoopTx) Rollback(context.Context) error { return nil }
func (n *NoopTx) IsRetriable() bool              { return false }

// NoopKVStore is a noop store, useful if we need to profile/debug only compute and not with the storage. This can be
// initialized in main.go instead of using default kvStore.
type NoopKVStore struct {
	*NoopKV
}

func (n *NoopKVStore) BeginTx(_ context.Context) (Tx, error)                { return &NoopTx{}, nil }
func (n *NoopKVStore) CreateTable(_ context.Context, _ []byte) error        { return nil }
func (n *NoopKVStore) DropTable(_ context.Context, _ []byte) error          { return nil }
func (n *NoopKVStore) GetInternalDatabase() (interface{}, error)            { return nil, nil }
func (n *NoopKVStore) TableSize(_ context.Context, _ []byte) (int64, error) { return 0, nil }

type NoopKV struct{}

func (n *NoopKV) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	return nil
}
func (n *NoopKV) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	return nil
}
func (n *NoopKV) Delete(ctx context.Context, table []byte, key Key) error                 { return nil }
func (n *NoopKV) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error { return nil }
func (n *NoopKV) Read(ctx context.Context, table []byte, key Key) (Iterator, error) {
	return &NoopIterator{}, nil
}
func (n *NoopKV) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool) (Iterator, error) {
	return &NoopIterator{}, nil
}
func (n *NoopKV) Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	return 0, nil
}
func (n *NoopKV) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	return 0, nil
}
func (n *NoopKV) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error {
	return nil
}
func (n *NoopKV) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error {
	return nil
}
func (n *NoopKV) Get(ctx context.Context, key []byte, isSnapshot bool) (Future, error) {
	return nil, nil
}
