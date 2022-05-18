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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

func TestSchemaSubspace(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kvStore, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	t.Run("put_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		s := NewSchemaStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, s.SchemaSubspaceName())
		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.Equal(t, api.Errorf(api.Code_INVALID_ARGUMENT, "invalid schema version %d", 0), s.Put(ctx, tx, 1, 2, 3, nil, 0))
		require.Equal(t, api.Errorf(api.Code_INVALID_ARGUMENT, "empty schema"), s.Put(ctx, tx, 1, 2, 3, nil, 1))
		require.NoError(t, tx.Rollback(ctx))
	})
	t.Run("put_duplicate_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		schema := []byte(`{"title": "test schema1"}`)

		s := NewSchemaStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, s.SchemaSubspaceName())
		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, s.Put(ctx, tx, 1, 2, 3, schema, 1))
		require.Equal(t, kv.ErrDuplicateKey, s.Put(ctx, tx, 1, 2, 3, schema, 1))
		require.NoError(t, tx.Rollback(ctx))
	})
	t.Run("put_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		s := NewSchemaStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, s.SchemaSubspaceName())

		schema := []byte(`{
		"title": "collection1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "integer"
			},
			"D1": {
				"type": "string",
				"max_length": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
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

		_ = kvStore.DropTable(ctx, s.SchemaSubspaceName())
	})
	t.Run("put_get_multiple", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		s := NewSchemaStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, s.SchemaSubspaceName())

		schema1 := []byte(`{
		"title": "collection1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "integer"
			},
			"D1": {
				"type": "string",
				"max_length": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)
		schema2 := []byte(`{
		"title": "collection1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "integer"
			}
		},
		"primary_key": ["K1"]
	}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
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
	})
	t.Run("put_delete_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		s := NewSchemaStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, s.SchemaSubspaceName())

		schema1 := []byte(`{
		"title": "collection1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "integer"
			},
			"D1": {
				"type": "string",
				"max_length": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)
		schema2 := []byte(`{
		"title": "collection1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "integer"
			}
		},
		"primary_key": ["K1"]
	}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, s.Put(ctx, tx, 1, 2, 3, schema1, 1))
		require.NoError(t, s.Put(ctx, tx, 1, 2, 3, schema2, 2))

		schemas, revisions, err := s.Get(ctx, tx, 1, 2, 3)
		require.NoError(t, err)
		require.Equal(t, schema1, schemas[0])
		require.Equal(t, schema2, schemas[1])
		require.Len(t, schemas, 2)
		require.Len(t, revisions, 2)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, s.Delete(ctx, tx, 1, 2, 3))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		schemas, revisions, err = s.Get(ctx, tx, 1, 2, 3)
		require.NoError(t, err)
		require.Len(t, schemas, 0)
		require.Len(t, revisions, 0)
		require.NoError(t, tx.Commit(ctx))
	})
}
