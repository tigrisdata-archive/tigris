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

package metadata

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

var (
	kvStore   kv.KeyValueStore
	tenantDb1 = NewDatabaseName("tenant_db1")
	tenantDb2 = NewDatabaseName("tenant_db2")
)

func TestTenantManager_CreateOrGetTenant(t *testing.T) {
	t.Run("create_tenant", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		tenant := m.tenants["ns-test1"]
		require.Equal(t, "ns-test1", tenant.namespace.StrId())
		require.Equal(t, uint32(2), tenant.namespace.Id())
		require.Equal(t, "ns-test1", m.idToTenantMap[uint32(2)])
		require.Empty(t, tenant.databases)
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})

	t.Run("create_multiple_tenants", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		_, err = m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test2", 3, NewNamespaceMetadata(3, "ns-test2", "ns-test2-display_name")})
		require.NoError(t, err)

		tenant := m.tenants["ns-test1"]
		require.Equal(t, "ns-test1", tenant.namespace.StrId())
		require.Equal(t, uint32(2), tenant.namespace.Id())
		require.Equal(t, "ns-test1", m.idToTenantMap[uint32(2)])

		require.Empty(t, tenant.databases)
		require.Empty(t, tenant.idToDatabaseMap)

		tenant = m.tenants["ns-test2"]
		require.Equal(t, "ns-test2", tenant.namespace.StrId())
		require.Equal(t, uint32(3), tenant.namespace.Id())
		require.Equal(t, "ns-test2", m.idToTenantMap[uint32(3)])
		require.Empty(t, tenant.databases)

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_error", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		// should fail now
		_, err = m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 3, NewNamespaceMetadata(3, "ns-test1", "ns-test1-display_name")})
		require.Equal(t, "id is already assigned to strId='ns-test1'", err.(*api.TigrisError).Error())

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_id_error", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)
		require.Equal(t, 1, len(m.idToTenantMap))

		// should fail now
		_, err = m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test2", 2, NewNamespaceMetadata(2, "ns-test2", "ns-test2-display_name")})
		require.Equal(t, "id is already assigned to the namespace 'ns-test1'", err.(*api.TigrisError).Error())
		require.Equal(t, "ns-test1", m.idToTenantMap[uint32(2)])
		require.Equal(t, 1, len(m.idToTenantMap))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
}

func TestTenantManager_CreateTenant(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	t.Run("create_tenant", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		tx, e := tm.StartTx(ctx)
		require.NoError(t, e)
		_, err := m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)
		namespaces, err := m.metaStore.GetNamespaces(ctx, tx)
		require.NoError(t, err)
		metadata := namespaces["ns-test1"]
		require.Equal(t, uint32(2), metadata.Id)
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_multiple_tenants", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		tx, e := tm.StartTx(ctx)
		require.NoError(t, e)

		_, err := m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		_, err = m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test2", 3, NewNamespaceMetadata(3, "ns-test2", "ns-test2-display_name")})
		require.NoError(t, err)
		namespaces, err := m.metaStore.GetNamespaces(ctx, tx)
		require.NoError(t, err)

		metadata := namespaces["ns-test1"]
		require.Equal(t, uint32(2), metadata.Id)

		metadata = namespaces["ns-test2"]
		require.Equal(t, uint32(3), metadata.Id)
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_error", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		tx, e := tm.StartTx(ctx)
		require.NoError(t, e)
		_, err := m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		// should fail now
		_, err = m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 3, NewNamespaceMetadata(3, "ns-test1", "ns-test1-display_name")})
		require.Equal(t, "namespace with same name already exists with id '2'", err.(*api.TigrisError).Error())

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_id_error", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		tx, e := tm.StartTx(ctx)
		require.NoError(t, e)
		_, err := m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		// should fail now
		_, err = m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test2", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.Equal(t, "namespace with same id already exists with name 'ns-test1'", err.(*api.TigrisError).Error())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
}

func TestTenantManager_CreateDatabases(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	t.Run("create_databases", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NamespaceMetadata{
			Id:    2,
			StrId: "ns-test1",
			Name:  "ns-test1-displayName",
		}})
		require.NoError(t, err)
		tenant := m.tenants["ns-test1"]

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		_, err = tenant.CreateDatabase(ctx, tx, tenantDb1.Name(), nil)
		require.NoError(t, err)
		_, err = tenant.CreateDatabase(ctx, tx, tenantDb2.Name(), nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil))
		db1, err := tenant.GetDatabase(ctx, tenantDb1)
		require.NoError(t, err)
		require.Equal(t, tenantDb1.Name(), db1.Name())
		require.Equal(t, tenantDb1.Name(), tenant.idToDatabaseMap[db1.id])

		db2, err := tenant.GetDatabase(ctx, tenantDb2)
		require.NoError(t, err)
		require.Equal(t, tenantDb2.Name(), db2.Name())
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, tenantDb2.Name(), tenant.idToDatabaseMap[db2.id])

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
	})
}

func TestTenantManager_CreateCollections(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	t.Run("create_collections", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		tenant := m.tenants["ns-test1"]
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		_, err = tenant.CreateDatabase(ctx, tx, tenantDb1.Name(), nil)
		require.NoError(t, err)
		_, err = tenant.CreateDatabase(ctx, tx, tenantDb2.Name(), nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil))

		db1, err := tenant.GetDatabase(ctx, tenantDb1)
		require.NoError(t, err)
		require.Equal(t, tenantDb1.Name(), db1.Name())
		require.Equal(t, tenantDb1.Name(), tenant.idToDatabaseMap[db1.id])

		db2, err := tenant.GetDatabase(ctx, tenantDb2)
		require.NoError(t, err)
		require.Equal(t, tenantDb2.Name(), db2.Name())
		require.Equal(t, tenantDb2.Name(), tenant.idToDatabaseMap[db2.id])
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
		require.NoError(t, tenant.CreateCollection(ctx, tx, db2, factory))

		require.NoError(t, tenant.reload(ctx, tx, nil, nil))

		db2, err = tenant.GetDatabase(ctx, tenantDb2)
		require.NoError(t, err)
		collection := db2.GetCollection("test_collection")
		require.Equal(t, "test_collection", collection.Name)
		require.Equal(t, "test_collection", db2.idToCollectionMap[collection.Id])
		require.Equal(t, 1, len(db2.idToCollectionMap))

		require.NoError(t, tx.Commit(ctx))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())
	})
}

func TestTenantManager_DropCollection(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	t.Run("drop_collection", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		tenant := m.tenants["ns-test1"]

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		_, err = tenant.CreateDatabase(ctx, tx, tenantDb1.Name(), nil)
		require.NoError(t, err)
		_, err = tenant.CreateDatabase(ctx, tx, tenantDb2.Name(), nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil))

		db1, err := tenant.GetDatabase(ctx, tenantDb1)
		require.NoError(t, err)
		require.Equal(t, tenantDb1.Name(), db1.Name())

		db2, err := tenant.GetDatabase(ctx, tenantDb2)
		require.NoError(t, err)
		require.Equal(t, tenantDb2.Name(), db2.Name())

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
		require.NoError(t, tenant.reload(ctx, tx, nil, nil))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		coll1 := db2.GetCollection("test_collection")
		require.Equal(t, "test_collection", coll1.Name)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, tenant.DropCollection(ctx, tx, db2, "test_collection"))
		require.NoError(t, tx.Commit(ctx))

		_, err = tm.StartTx(ctx)
		require.NoError(t, err)
		coll := db2.GetCollection("test_collection")
		require.Nil(t, coll)
		require.Empty(t, db2.idToCollectionMap[coll1.Id])

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())
	})
}

func TestTenantManager_DataSize(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	m, ctx, cancel := NewTestTenantMgr(kvStore)
	defer cancel()

	_, err := m.CreateOrGetTenant(context.TODO(), &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
	require.NoError(t, err)

	_, err = m.CreateOrGetTenant(context.TODO(), &TenantNamespace{"ns-test2", 3, NewNamespaceMetadata(3, "ns-test2", "ns-test2-display_name")})
	require.NoError(t, err)

	tenant := m.tenants["ns-test1"]
	tx, err := tm.StartTx(context.TODO())
	require.NoError(t, err)

	_, err = tenant.CreateDatabase(ctx, tx, tenantDb1.Name(), nil)
	require.NoError(t, err)
	_, err = tenant.CreateDatabase(ctx, tx, tenantDb2.Name(), nil)
	require.NoError(t, err)

	tenant2 := m.tenants["ns-test2"]

	_, err = tenant2.CreateDatabase(ctx, tx, tenantDb1.Name(), nil)
	require.NoError(t, err)
	_, err = tenant2.CreateDatabase(ctx, tx, tenantDb2.Name(), nil)
	require.NoError(t, err)

	require.NoError(t, tenant.reload(ctx, tx, nil, nil))
	require.NoError(t, tenant2.reload(ctx, tx, nil, nil))

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

	db1, err := tenant.GetDatabase(ctx, tenantDb1)
	require.NoError(t, err)
	db2, err := tenant.GetDatabase(ctx, tenantDb2)
	require.NoError(t, err)

	require.NoError(t, tenant.CreateCollection(ctx, tx, db1, factory))
	require.NoError(t, err)
	require.NoError(t, tenant.CreateCollection(ctx, tx, db2, factory))
	require.NoError(t, err)

	// create tenant2 dbs and collections
	db21, err := tenant2.GetDatabase(ctx, tenantDb1)
	require.NoError(t, err)
	db22, err := tenant2.GetDatabase(ctx, tenantDb2)
	require.NoError(t, err)

	require.NoError(t, tenant2.CreateCollection(ctx, tx, db21, factory))
	require.NoError(t, err)
	require.NoError(t, tenant2.CreateCollection(ctx, tx, db22, factory))
	require.NoError(t, err)

	require.NoError(t, tenant.reload(ctx, tx, nil, nil))
	require.NoError(t, tenant2.reload(ctx, tx, nil, nil))

	require.NoError(t, tx.Commit(context.TODO()))

	coll1 := db2.GetCollection("test_collection")
	docSize := 10 * 1024
	table, err := m.encoder.EncodeTableName(tenant.GetNamespace(), db1, coll1)
	require.NoError(t, err)

	err = tenant.kvStore.DropTable(ctx, table)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		err = tenant.kvStore.Insert(ctx, table, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
		require.NoError(t, err)
	}

	coll21 := db21.GetCollection("test_collection")
	table21, err := m.encoder.EncodeTableName(tenant2.GetNamespace(), db21, coll21)
	require.NoError(t, err)

	err = tenant2.kvStore.DropTable(ctx, table21)
	require.NoError(t, err)

	for i := 0; i < 150; i++ {
		err = tenant2.kvStore.Insert(ctx, table21, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
		require.NoError(t, err)
	}

	coll22 := db22.GetCollection("test_collection")
	table22, err := m.encoder.EncodeTableName(tenant2.GetNamespace(), db22, coll22)
	require.NoError(t, err)

	err = tenant2.kvStore.DropTable(ctx, table22)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		err = tenant2.kvStore.Insert(ctx, table22, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
		require.NoError(t, err)
	}

	sz, err := tenant.Size(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(983500), sz)

	sz, err = tenant.DatabaseSize(ctx, db1)
	require.NoError(t, err)
	assert.Equal(t, int64(983500), sz)

	sz, err = tenant.CollectionSize(ctx, db1, coll1)
	require.NoError(t, err)
	assert.Equal(t, int64(983500), sz)

	// db2 is empty
	sz, err = tenant.DatabaseSize(ctx, db2)
	require.NoError(t, err)
	assert.Equal(t, int64(0), sz)

	// Tenant 2
	// db21
	sz, err = tenant2.Size(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2186250), sz) // sum of db21 and db22

	sz, err = tenant2.DatabaseSize(ctx, db21)
	require.NoError(t, err)
	assert.Equal(t, int64(1694750), sz)

	sz, err = tenant2.CollectionSize(ctx, db21, coll21)
	require.NoError(t, err)
	assert.Equal(t, int64(1694750), sz)

	// db22
	sz, err = tenant2.DatabaseSize(ctx, db22)
	require.NoError(t, err)
	assert.Equal(t, int64(491500), sz)

	sz, err = tenant2.CollectionSize(ctx, db22, coll22)
	require.NoError(t, err)
	assert.Equal(t, int64(491500), sz)

	// cleanup
	err = tenant2.kvStore.DropTable(ctx, table)
	require.NoError(t, err)
	err = tenant2.kvStore.DropTable(ctx, table21)
	require.NoError(t, err)
	err = tenant2.kvStore.DropTable(ctx, table22)
	require.NoError(t, err)

	_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())
}

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled"})

	fdbCfg, err := config.GetTestFDBConfig("../..")
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB config: %v", err))
	}

	kvStore, err = kv.NewKeyValueStore(fdbCfg)
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB KV %v", err))
	}

	os.Exit(m.Run())
}
