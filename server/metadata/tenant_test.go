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

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata/encoding"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

func TestTenantManager_CreateTenant(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kvStore, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	tm := transaction.NewManager(kvStore)
	t.Run("create_tenant", func(t *testing.T) {
		m := newTenantManager(&encoding.TestMDNameRegistry{
			ReserveSB:  "test_tenant_reserve",
			EncodingSB: "test_tenant_encoding",
			SchemaSB:   "test_tenant_schema",
		})

		ctx := context.TODO()
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())

		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		_, err = m.CreateOrGetTenant(ctx, tx, &TenantNamespace{"ns-test1", 2})
		require.NoError(t, err)

		tenant := m.GetTenant("ns-test1")
		require.Equal(t, "ns-test1", tenant.namespace.Name())
		require.Equal(t, uint32(2), tenant.namespace.Id())
		require.Equal(t, "ns-test1", m.idToTenantMap[uint32(2)])
		require.Empty(t, tenant.databases)
		require.NoError(t, tx.Commit(ctx))
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_multiple_tenants", func(t *testing.T) {
		m := newTenantManager(&encoding.TestMDNameRegistry{
			ReserveSB:  "test_tenant_reserve",
			EncodingSB: "test_tenant_encoding",
			SchemaSB:   "test_tenant_schema",
		})

		ctx := context.TODO()
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())

		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		_, err = m.CreateOrGetTenant(ctx, tx, &TenantNamespace{"ns-test1", 2})
		require.NoError(t, err)

		_, err = m.CreateOrGetTenant(ctx, tx, &TenantNamespace{"ns-test2", 3})
		require.NoError(t, err)

		tenant := m.GetTenant("ns-test1")
		require.Equal(t, "ns-test1", tenant.namespace.Name())
		require.Equal(t, uint32(2), tenant.namespace.Id())
		require.Equal(t, "ns-test1", m.idToTenantMap[uint32(2)])

		require.Empty(t, tenant.databases)
		require.Empty(t, tenant.idToDatabaseMap)

		tenant = m.GetTenant("ns-test2")
		require.Equal(t, "ns-test2", tenant.namespace.Name())
		require.Equal(t, uint32(3), tenant.namespace.Id())
		require.Equal(t, "ns-test2", m.idToTenantMap[uint32(3)])
		require.Empty(t, tenant.databases)
		require.NoError(t, tx.Commit(ctx))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_error", func(t *testing.T) {
		m := newTenantManager(&encoding.TestMDNameRegistry{
			ReserveSB:  "test_tenant_reserve",
			EncodingSB: "test_tenant_encoding",
			SchemaSB:   "test_tenant_schema",
		})

		ctx := context.TODO()
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())

		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		_, err = m.CreateOrGetTenant(ctx, tx, &TenantNamespace{"ns-test1", 2})
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		// should fail now
		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		_, err = m.CreateOrGetTenant(context.TODO(), tx, &TenantNamespace{"ns-test1", 3})
		require.Equal(t, "id is already assigned to 'ns-test1'", err.(*api.TigrisError).Error())
		require.NoError(t, tx.Commit(context.TODO()))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_id_error", func(t *testing.T) {
		m := newTenantManager(&encoding.TestMDNameRegistry{
			ReserveSB:  "test_tenant_reserve",
			EncodingSB: "test_tenant_encoding",
			SchemaSB:   "test_tenant_schema",
		})

		ctx := context.TODO()
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())

		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		_, err = m.CreateOrGetTenant(ctx, tx, &TenantNamespace{"ns-test1", 2})
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		// should fail now
		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		_, err = m.CreateOrGetTenant(ctx, tx, &TenantNamespace{"ns-test2", 2})
		require.Equal(t, "id is already assigned to the namespace 'ns-test1'", err.(*api.TigrisError).Error())
		require.NoError(t, tx.Rollback(ctx))
		require.Equal(t, "ns-test1", m.idToTenantMap[uint32(2)])
		require.Equal(t, 1, len(m.idToTenantMap))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
}

func TestTenantManager_CreateDatabases(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kvStore, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	tm := transaction.NewManager(kvStore)
	t.Run("create_databases", func(t *testing.T) {
		m := newTenantManager(&encoding.TestMDNameRegistry{
			ReserveSB:  "test_tenant_reserve",
			EncodingSB: "test_tenant_encoding",
			SchemaSB:   "test_tenant_schema",
		})

		ctx := context.TODO()
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())

		tx, err := tm.StartTxWithoutTracking(context.TODO())
		require.NoError(t, err)

		_, err = m.CreateOrGetTenant(context.TODO(), tx, &TenantNamespace{"ns-test1", 2})
		require.NoError(t, err)
		tenant := m.GetTenant("ns-test1")
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "tenant_db1"))
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "tenant_db2"))

		db1, err := tenant.GetDatabase(ctx, tx, "tenant_db1")
		require.NoError(t, err)
		require.Equal(t, "tenant_db1", db1.name)
		require.Equal(t, "tenant_db1", tenant.idToDatabaseMap[db1.id])

		db2, err := tenant.GetDatabase(ctx, tx, "tenant_db2")
		require.NoError(t, err)
		require.Equal(t, "tenant_db2", db2.name)
		require.NoError(t, tx.Commit(context.TODO()))
		require.Equal(t, "tenant_db2", tenant.idToDatabaseMap[db2.id])

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
	})
}

func TestTenantManager_CreateCollections(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kvStore, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	tm := transaction.NewManager(kvStore)
	t.Run("create_collections", func(t *testing.T) {
		m := newTenantManager(&encoding.TestMDNameRegistry{
			ReserveSB:  "test_tenant_reserve",
			EncodingSB: "test_tenant_encoding",
			SchemaSB:   "test_tenant_schema",
		})

		ctx := context.TODO()
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())

		tx, err := tm.StartTxWithoutTracking(context.TODO())
		require.NoError(t, err)

		_, err = m.CreateOrGetTenant(context.TODO(), tx, &TenantNamespace{"ns-test1", 2})
		require.NoError(t, err)
		tenant := m.GetTenant("ns-test1")
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "tenant_db1"))
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "tenant_db2"))

		db1, err := tenant.GetDatabase(ctx, tx, "tenant_db1")
		require.NoError(t, err)
		require.Equal(t, "tenant_db1", db1.name)
		require.Equal(t, "tenant_db1", tenant.idToDatabaseMap[db1.id])

		db2, err := tenant.GetDatabase(ctx, tx, "tenant_db2")
		require.NoError(t, err)
		require.Equal(t, "tenant_db2", db2.name)
		require.Equal(t, "tenant_db2", tenant.idToDatabaseMap[db2.id])
		require.Equal(t, 2, len(tenant.idToDatabaseMap))
		require.Equal(t, 2, len(tenant.databases))

		jsSchema := []byte(`{
        "title": "test_collection",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "integer"
			},
			"D1": {
				"type": "string",
				"maxLength": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)

		factory, err := schema.Build("test_collection", jsSchema)
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(context.TODO(), tx, db2, factory))
		tenant.InvalidateDBCache(db2.Name())

		db2, err = tenant.GetDatabase(ctx, tx, "tenant_db2")
		require.NoError(t, err)
		collection := db2.GetCollection("test_collection")
		require.Equal(t, "test_collection", collection.Name)
		require.Equal(t, "test_collection", db2.idToCollectionMap[collection.Id])
		require.Equal(t, 1, len(db2.idToCollectionMap))

		require.NoError(t, tx.Commit(context.TODO()))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())
	})
}

func TestTenantManager_DropCollection(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kvStore, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	tm := transaction.NewManager(kvStore)
	t.Run("drop_collection", func(t *testing.T) {
		m := newTenantManager(&encoding.TestMDNameRegistry{
			ReserveSB:  "test_tenant_reserve",
			EncodingSB: "test_tenant_encoding",
			SchemaSB:   "test_tenant_schema",
		})

		ctx := context.TODO()
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())

		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		_, err = m.CreateOrGetTenant(ctx, tx, &TenantNamespace{"ns-test1", 2})
		require.NoError(t, err)
		tenant := m.GetTenant("ns-test1")
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "tenant_db1"))
		require.NoError(t, tenant.CreateDatabase(ctx, tx, "tenant_db2"))

		db1, err := tenant.GetDatabase(ctx, tx, "tenant_db1")
		require.NoError(t, err)
		require.Equal(t, "tenant_db1", db1.name)

		db2, err := tenant.GetDatabase(ctx, tx, "tenant_db2")
		require.NoError(t, err)
		require.Equal(t, "tenant_db2", db2.name)

		jsSchema := []byte(`{
		"title": "test_collection",
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

		factory, err := schema.Build("test_collection", jsSchema)
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(ctx, tx, db2, factory))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		coll1 := db2.GetCollection("test_collection")
		require.Equal(t, "test_collection", coll1.Name)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		require.NoError(t, tenant.DropCollection(ctx, tx, db2, "test_collection"))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		coll := db2.GetCollection("test_collection")
		require.Nil(t, coll)
		require.Empty(t, db2.idToCollectionMap[coll1.Id])

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())
	})
}
