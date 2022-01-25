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

	"github.com/tigrisdata/tigrisdb/types"
)

var (
	PartitionKey = "partition_key"
	PrimaryKey   = "primary_key"
	DataField    = "data"
)

type Doc struct {
	Key   types.Key
	Value []byte
}

type crud interface {
	Insert(ctx context.Context, table string, key types.Key, data []byte) error
	Update(ctx context.Context, table string, key types.Key, data []byte) error // full body replace of existing document
	Replace(ctx context.Context, table string, key types.Key, data []byte) error
	Delete(ctx context.Context, table string, key types.Key) error
	Read(ctx context.Context, table string, key types.Key) ([]Doc, error)
	ReadRange(ctx context.Context, table string, partitionKey []byte, lkey types.Key, rkey types.Key, limit int) ([]Doc, error)
}

type Tx interface {
	Replace(ctx context.Context, table string, key types.Key, data []byte) error
	Delete(ctx context.Context, table string, key types.Key) error
	Commit(context.Context) error
	Rollback(context.Context) error
}

type KV interface {
	crud
	Tx() Tx
	Batch() Tx
	CreateTable(ctx context.Context, name string) error
	DropTable(ctx context.Context, name string) error
}
