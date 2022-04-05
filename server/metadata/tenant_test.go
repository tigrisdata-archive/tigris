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

package metadata

import (
	"context"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/schema"
	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/server/metadata/encoding"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
)

func TestTenantManager_CreateTenant(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	tm := transaction.NewManager(kv)
	t.Run("create_tenant", func(t *testing.T) {
		ctx := context.TODO()
		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)

		m := NewTenantManager()
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		require.NoError(t, m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2}))

		tenant := m.GetTenant("ns-test1")
		require.Equal(t, "ns-test1", tenant.namespace.Name())
		require.Equal(t, uint32(2), tenant.namespace.Id())
		require.Empty(t, tenant.databases)
		require.NoError(t, tx.Commit(ctx))
		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
	})
	t.Run("create_multiple_tenants", func(t *testing.T) {
		ctx := context.TODO()
		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)

		m := NewTenantManager()
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		require.NoError(t, m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2}))
		require.NoError(t, m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test2", 3}))

		tenant := m.GetTenant("ns-test1")
		require.Equal(t, "ns-test1", tenant.namespace.Name())
		require.Equal(t, uint32(2), tenant.namespace.Id())
		require.Empty(t, tenant.databases)

		tenant = m.GetTenant("ns-test2")
		require.Equal(t, "ns-test2", tenant.namespace.Name())
		require.Equal(t, uint32(3), tenant.namespace.Id())
		require.Empty(t, tenant.databases)

		require.NoError(t, tx.Commit(ctx))

		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
	})
	t.Run("create_duplicate_tenant_error", func(t *testing.T) {
		ctx := context.TODO()
		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)

		m := NewTenantManager()
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		require.NoError(t, m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2}))
		require.NoError(t, tx.Commit(ctx))

		// should fail now
		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		err = m.CreateTenant(context.TODO(), tx, &TenantNamespace{"ns-test1", 3})
		require.Equal(t, "id is already assigned to 'ns-test1'", err.(*api.TigrisDBError).Error())
		require.NoError(t, tx.Commit(context.TODO()))

		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
	})
	t.Run("create_duplicate_tenant_id_error", func(t *testing.T) {
		ctx := context.TODO()
		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)

		m := NewTenantManager()
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		require.NoError(t, m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2}))
		require.NoError(t, tx.Commit(ctx))

		// should fail now
		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		err = m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test2", 2})
		require.Equal(t, "id is already assigned to the namespace 'ns-test1'", err.(*api.TigrisDBError).Error())
		require.NoError(t, tx.Rollback(ctx))

		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
	})
}

func TestTenantManager_CreateDatabases(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	tm := transaction.NewManager(kv)
	t.Run("create_databases", func(t *testing.T) {
		ctx := context.TODO()
		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
		_ = kv.DropTable(ctx, encoding.EncodingSubspaceKey)

		m := NewTenantManager()
		tx, err := tm.StartTxWithoutTracking(context.TODO())
		require.NoError(t, err)

		require.NoError(t, m.CreateTenant(context.TODO(), tx, &TenantNamespace{"ns-test1", 2}))
		tenant := m.GetTenant("ns-test1")
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "db1"))
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "db2"))

		db1, err := tenant.GetDatabase(ctx, tx, "db1")
		require.NoError(t, err)
		require.Equal(t, "db1", db1.name)

		db2, err := tenant.GetDatabase(ctx, tx, "db2")
		require.NoError(t, err)
		require.Equal(t, "db2", db2.name)
		require.NoError(t, tx.Commit(context.TODO()))

		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
		_ = kv.DropTable(ctx, encoding.EncodingSubspaceKey)
	})
}

func TestTenantManager_CreateCollections(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	tm := transaction.NewManager(kv)
	t.Run("create_collections", func(t *testing.T) {
		ctx := context.TODO()
		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
		_ = kv.DropTable(ctx, encoding.EncodingSubspaceKey)
		_ = kv.DropTable(ctx, encoding.SchemaSubspaceKey)

		m := NewTenantManager()
		tx, err := tm.StartTxWithoutTracking(context.TODO())
		require.NoError(t, err)

		require.NoError(t, m.CreateTenant(context.TODO(), tx, &TenantNamespace{"ns-test1", 2}))
		tenant := m.GetTenant("ns-test1")
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "db1"))
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "db2"))

		db1, err := tenant.GetDatabase(ctx, tx, "db1")
		require.NoError(t, err)
		require.Equal(t, "db1", db1.name)

		db2, err := tenant.GetDatabase(ctx, tx, "db2")
		require.NoError(t, err)
		require.Equal(t, "db2", db2.name)

		jsSchema := []byte(`{
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "int"
			},
			"D1": {
				"type": "string",
				"maxLength": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)

		factory, err := schema.Build("test_collection", jsoniter.RawMessage(jsSchema))
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(context.TODO(), tx, db2, factory))
		coll, err := db2.GetCollection("test_collection")
		require.NoError(t, err)
		require.Equal(t, "test_collection", coll.Name)
		require.NoError(t, tx.Commit(context.TODO()))

		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
		_ = kv.DropTable(ctx, encoding.EncodingSubspaceKey)
		_ = kv.DropTable(ctx, encoding.SchemaSubspaceKey)
	})
}

func TestTenantManager_DropCollection(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	tm := transaction.NewManager(kv)
	t.Run("drop_collection", func(t *testing.T) {
		ctx := context.TODO()
		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
		_ = kv.DropTable(ctx, encoding.EncodingSubspaceKey)
		_ = kv.DropTable(ctx, encoding.SchemaSubspaceKey)

		m := NewTenantManager()
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		require.NoError(t, m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2}))
		tenant := m.GetTenant("ns-test1")
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "db1"))
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "db2"))

		db1, err := tenant.GetDatabase(ctx, tx, "db1")
		require.NoError(t, err)
		require.Equal(t, "db1", db1.name)

		db2, err := tenant.GetDatabase(ctx, tx, "db2")
		require.NoError(t, err)
		require.Equal(t, "db2", db2.name)

		jsSchema := []byte(`{
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

		factory, err := schema.Build("test_collection", jsoniter.RawMessage(jsSchema))
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(ctx, tx, db2, factory))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		coll, err := db2.GetCollection("test_collection")
		require.NoError(t, err)
		require.Equal(t, "test_collection", coll.Name)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		require.NoError(t, tenant.DropCollection(ctx, tx, db2, "test_collection"))
		require.Equal(t, "test_collection", coll.Name)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		coll, err = db2.GetCollection("test_collection")
		require.Equal(t, "collection doesn't exists 'test_collection'", err.Error())
		require.Nil(t, coll)

		_ = kv.DropTable(ctx, encoding.ReservedSubspaceKey)
		_ = kv.DropTable(ctx, encoding.EncodingSubspaceKey)
		_ = kv.DropTable(ctx, encoding.SchemaSubspaceKey)
	})
}
