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

package encoding

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"google.golang.org/grpc/codes"
)

func TestSchemaSubspace(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	t.Run("put_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = kv.DropTable(ctx, schemaSubspaceKey)
		tm := transaction.NewManager(kv)
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		s := SchemaSubspace{}
		require.Equal(t, api.Errorf(codes.InvalidArgument, "invalid schema version %d", 0), s.Put(ctx, tx, 1, 2, 3, nil, 0))
		require.Equal(t, api.Errorf(codes.InvalidArgument, "empty schema"), s.Put(ctx, tx, 1, 2, 3, nil, 1))
		require.NoError(t, tx.Rollback(ctx))
	})
	t.Run("put_duplicate_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		schema := []byte(`{"title": "test schema1"}`)

		_ = kv.DropTable(ctx, schemaSubspaceKey)
		tm := transaction.NewManager(kv)
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		s := SchemaSubspace{}
		require.NoError(t, s.Put(ctx, tx, 1, 2, 3, schema, 1))
		require.Equal(t, fmt.Errorf("file already exists"), s.Put(ctx, tx, 1, 2, 3, schema, 1))
		require.NoError(t, tx.Rollback(ctx))
	})
	t.Run("put_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = kv.DropTable(ctx, schemaSubspaceKey)

		schema := []byte(`{
		"title": "test schema1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "int"
			},
			"D1": {
				"type": "string",
				"max_length": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)

		tm := transaction.NewManager(kv)
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		s := SchemaSubspace{}
		require.NoError(t, s.Put(ctx, tx, 1, 2, 3, schema, 1))
		sch, rev, err := s.GetLatest(ctx, tx, 1, 2, 3)
		require.NoError(t, err)
		require.Equal(t, schema, sch)
		require.Equal(t, 1, rev)

		schemas, revisions, err := s.Get(ctx, tx, 1, 2, 3)
		require.NoError(t, err)
		require.Equal(t, schema, schemas[0])
		require.Equal(t, 1, revisions[0])
		require.NoError(t, tx.Commit(ctx))

		_ = kv.DropTable(ctx, schemaSubspaceKey)
	})
	t.Run("put_get_multiple", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = kv.DropTable(ctx, schemaSubspaceKey)

		schema1 := []byte(`{
		"title": "test schema1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "int"
			},
			"D1": {
				"type": "string",
				"max_length": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)
		schema2 := []byte(`{
		"title": "test schema2",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "int"
			}
		},
		"primary_key": ["K1"]
	}`)

		tm := transaction.NewManager(kv)
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		s := SchemaSubspace{}
		require.NoError(t, s.Put(ctx, tx, 1, 2, 3, schema1, 1))
		require.NoError(t, s.Put(ctx, tx, 1, 2, 3, schema2, 2))
		sch, rev, err := s.GetLatest(ctx, tx, 1, 2, 3)
		require.NoError(t, err)
		require.Equal(t, schema2, sch)
		require.Equal(t, 2, rev)

		schemas, revisions, err := s.Get(ctx, tx, 1, 2, 3)
		require.NoError(t, err)
		require.Equal(t, schema1, schemas[0])
		require.Equal(t, schema2, schemas[1])
		require.Equal(t, 1, revisions[0])
		require.Equal(t, 2, revisions[1])
		require.NoError(t, tx.Commit(ctx))
		_ = kv.DropTable(ctx, schemaSubspaceKey)
	})
}
