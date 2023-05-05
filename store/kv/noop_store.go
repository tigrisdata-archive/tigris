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
)

type NoopIterator struct{}

func (*NoopIterator) Next(_ *KeyValue) bool { return false }
func (*NoopIterator) Err() error            { return nil }

type NoopFDBTypeIterator struct{}

func (*NoopFDBTypeIterator) Next(_ *FdbBaseKeyValue[int64]) bool { return false }
func (*NoopFDBTypeIterator) Err() error                          { return nil }

type NoopTx struct {
	*NoopKV
}

func (*NoopTx) Commit(context.Context) error   { return nil }
func (*NoopTx) Rollback(context.Context) error { return nil }
func (*NoopTx) IsRetriable() bool              { return false }

// NoopKVStore is a noop store, useful if we need to profile/debug only compute and not with the storage. This can be
// initialized in main.go instead of using default kvStore.
type NoopKVStore struct {
	*NoopKV
}

func (*NoopKVStore) BeginTx(_ context.Context) (Tx, error)                { return &NoopTx{}, nil }
func (*NoopKVStore) CreateTable(_ context.Context, _ []byte) error        { return nil }
func (*NoopKVStore) DropTable(_ context.Context, _ []byte) error          { return nil }
func (*NoopKVStore) GetInternalDatabase() (any, error)                    { return nil, nil }
func (*NoopKVStore) TableSize(_ context.Context, _ []byte) (int64, error) { return 0, nil }

func (*NoopKVStore) GetTableStats(_ context.Context, _ []byte) (*TableStats, error) {
	return &TableStats{}, nil
}

type NoopKV struct{}

func (*NoopKV) Insert(_ context.Context, _ []byte, _ Key, _ *internal.TableData) error {
	return nil
}

func (*NoopKV) Replace(_ context.Context, _ []byte, _ Key, _ *internal.TableData, _ bool) error {
	return nil
}
func (*NoopKV) Delete(_ context.Context, _ []byte, _ Key) error { return nil }
func (*NoopKV) Read(_ context.Context, _ []byte, _ Key, _ bool) (Iterator, error) {
	return &NoopIterator{}, nil
}

func (*NoopKV) ReadRange(_ context.Context, _ []byte, _ Key, _ Key, _ bool, _ bool) (Iterator, error) {
	return &NoopIterator{}, nil
}

func (*NoopKV) SetVersionstampedValue(_ context.Context, _ []byte, _ []byte) error {
	return nil
}

func (*NoopKV) SetVersionstampedKey(_ context.Context, _ []byte, _ []byte) error {
	return nil
}

func (*NoopKV) AtomicAdd(_ context.Context, _ []byte, _ Key, _ int64) error {
	return nil
}

func (*NoopKV) AtomicRead(_ context.Context, _ []byte, _ Key) (int64, error) {
	return 0, nil
}

func (*NoopKV) AtomicReadRange(_ context.Context, _ []byte, _ Key, _ Key, _ bool) (AtomicIterator, error) {
	return &NoopFDBTypeIterator{}, nil
}

func (*NoopKV) AtomicReadPrefix(_ context.Context, _ []byte, _ Key, _ bool) (AtomicIterator, error) {
	return &NoopFDBTypeIterator{}, nil
}

func (*NoopKV) Get(_ context.Context, _ []byte, _ bool) Future {
	return nil
}

func (*NoopKV) GetMetadata(_ context.Context, _ []byte, _ Key) (*internal.TableData, error) {
	return &internal.TableData{}, nil
}

func (*NoopKV) RangeSize(_ context.Context, _ []byte, _ Key, _ Key) (int64, error) {
	return 0, nil
}
