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
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

var (
	kvStore     kv.KeyValueStore
	tenantProj1 = "tenant_db1"
	tenantProj2 = "tenant_db2"
	tenantDb1   = NewDatabaseName("tenant_db1")
	tenantDb2   = NewDatabaseName("tenant_db2")
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
		require.Empty(t, tenant.projects)
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

		require.Empty(t, tenant.projects)
		require.Empty(t, tenant.idToDatabaseMap)

		tenant = m.tenants["ns-test2"]
		require.Equal(t, "ns-test2", tenant.namespace.StrId())
		require.Equal(t, uint32(3), tenant.namespace.Id())
		require.Equal(t, "ns-test2", m.idToTenantMap[uint32(3)])
		require.Empty(t, tenant.projects)

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

func TestTenantManager_CreateProjects(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	t.Run("create_projects", func(t *testing.T) {
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
		_, err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
		require.NoError(t, err)
		_, err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil))
		proj1, err := tenant.GetProject(tenantProj1)
		require.NoError(t, err)
		require.Equal(t, tenantProj1, proj1.Name())
		require.Equal(t, proj1.id, proj1.database.id)
		require.Equal(t, proj1, tenant.projects[tenantProj1])
		require.Equal(t, tenantProj1, tenant.idToDatabaseMap[proj1.id].Name())

		proj2, err := tenant.GetProject(tenantProj2)
		require.NoError(t, err)
		require.Equal(t, tenantProj2, proj2.Name())
		require.Equal(t, proj2.id, proj2.database.id)
		require.NoError(t, tx.Commit(ctx))

		require.Equal(t, proj2, tenant.projects[tenantProj2])
		require.Equal(t, tenantProj2, tenant.idToDatabaseMap[proj2.id].Name())

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
		_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
	})
}

func TestTenantManager_DatabaseBranches(t *testing.T) {
	tm := transaction.NewManager(kvStore)
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
	_, err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
	require.NoError(t, err)
	_, err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
	require.NoError(t, err)

	require.NoError(t, tenant.reload(ctx, tx, nil, nil))

	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj1, NewDatabaseNameWithBranch(tenantProj1, "branch1")))
	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj2, NewDatabaseNameWithBranch(tenantProj2, "branch1")))
	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj1, NewDatabaseNameWithBranch(tenantProj1, "branch2")))
	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj2, NewDatabaseNameWithBranch(tenantProj2, "branch2")))
	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj1, NewDatabaseNameWithBranch(tenantProj1, "branch3")))

	// reload again to get all the branches
	require.NoError(t, tenant.reload(ctx, tx, nil, nil))

	// list all branches
	branches := tenant.ListDatabaseBranches(tenantProj1)
	require.ElementsMatch(t, []string{"main", "branch1", "branch2", "branch3"}, branches)

	proj1, err := tenant.GetProject(tenantProj1)
	require.NoError(t, err)
	require.Equal(t, proj1.id, proj1.database.id)
	require.False(t, proj1.database.IsBranch())
	branch1, err := proj1.GetDatabase(NewDatabaseNameWithBranch(tenantProj1, "branch1"))
	require.NoError(t, err)
	require.True(t, branch1.IsBranch())
	require.Equal(t, tenantProj1+BranchNameSeparator+"branch1", branch1.Name())

	branch2, err := proj1.GetDatabase(NewDatabaseNameWithBranch(tenantProj1, "branch2"))
	require.NoError(t, err)
	require.True(t, branch2.IsBranch())
	require.Equal(t, tenantProj1+BranchNameSeparator+"branch2", branch2.Name())

	branch3, err := proj1.GetDatabase(NewDatabaseNameWithBranch(tenantProj1, "branch3"))
	require.NoError(t, err)
	require.True(t, branch3.IsBranch())
	require.Equal(t, tenantProj1+BranchNameSeparator+"branch3", branch3.Name())

	databases := proj1.GetDatabaseWithBranches()
	require.Len(t, databases, 4)
	require.Equal(t, proj1.database, databases[0])

	proj2, err := tenant.GetProject(tenantProj2)
	require.NoError(t, err)

	branch1, err = proj2.GetDatabase(NewDatabaseNameWithBranch(tenantProj2, "branch1"))
	require.NoError(t, err)
	require.True(t, branch1.IsBranch())

	branch2, err = proj2.GetDatabase(NewDatabaseNameWithBranch(tenantProj2, "branch2"))
	require.NoError(t, err)
	require.True(t, branch2.IsBranch())

	databases = proj2.GetDatabaseWithBranches()
	require.Len(t, databases, 3)
	require.Equal(t, proj2.database, databases[0])

	require.NoError(t, tx.Commit(ctx))

	require.Equal(t, tenantProj1, tenant.idToDatabaseMap[proj1.id].Name())
	require.Equal(t, tenantProj2, tenant.idToDatabaseMap[proj2.id].Name())

	// delete a branch now
	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, tenant.DeleteBranch(ctx, tx, tenantProj1, NewDatabaseNameWithBranch(tenantProj1, "branch1")))
	require.NoError(t, tenant.DeleteBranch(ctx, tx, tenantProj2, NewDatabaseNameWithBranch(tenantProj2, "branch1")))
	require.NoError(t, tenant.DeleteBranch(ctx, tx, tenantProj1, NewDatabaseNameWithBranch(tenantProj1, "branch2")))

	require.NoError(t, tenant.reload(ctx, tx, nil, nil))
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)

	proj1, err = tenant.GetProject(tenantProj1)
	require.NoError(t, err)
	require.False(t, proj1.database.IsBranch())

	_, err = proj1.GetDatabase(NewDatabaseNameWithBranch(tenantProj1, "branch1"))
	require.Error(t, NewBranchNotFoundErr("branch1"), err)

	_, err = proj1.GetDatabase(NewDatabaseNameWithBranch(tenantProj1, "branch2"))
	require.Error(t, NewBranchNotFoundErr("branch2"), err)

	branch3, err = proj1.GetDatabase(NewDatabaseNameWithBranch(tenantProj1, "branch3"))
	require.NoError(t, err)
	require.True(t, branch3.IsBranch())
	require.Equal(t, tenantProj1+BranchNameSeparator+"branch3", branch3.Name())

	databases = proj1.GetDatabaseWithBranches()
	require.Len(t, databases, 2)
	require.Equal(t, proj1.database, databases[0])
	require.Equal(t, branch3, databases[1])

	proj2, err = tenant.GetProject(tenantProj2)
	require.NoError(t, err)

	_, err = proj2.GetDatabase(NewDatabaseNameWithBranch(tenantProj2, "branch1"))
	require.Error(t, NewBranchNotFoundErr("branch1"), err)

	branch2, err = proj2.GetDatabase(NewDatabaseNameWithBranch(tenantProj2, "branch2"))
	require.NoError(t, err)
	require.True(t, branch2.IsBranch())

	databases = proj2.GetDatabaseWithBranches()
	require.Len(t, databases, 2)
	require.Equal(t, proj2.database, databases[0])
	require.Equal(t, branch2, databases[1])

	require.NoError(t, tx.Commit(ctx))

	_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
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
		_, err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
		require.NoError(t, err)
		_, err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil))

		proj1, err := tenant.GetProject(tenantProj1)
		require.NoError(t, err)
		require.Equal(t, tenantProj1, proj1.Name())
		db1 := proj1.database
		require.NoError(t, err)
		require.Equal(t, tenantDb1.Name(), db1.Name())
		require.Equal(t, tenantDb1.Name(), tenant.idToDatabaseMap[db1.id].Name())

		proj2, err := tenant.GetProject(tenantProj2)
		require.NoError(t, err)
		require.Equal(t, tenantProj2, proj2.Name())
		db2 := proj2.database
		require.Equal(t, tenantDb2.Name(), db2.Name())
		require.Equal(t, tenantDb2.Name(), tenant.idToDatabaseMap[db2.id].Name())
		require.Equal(t, 2, len(tenant.idToDatabaseMap))
		require.Equal(t, 2, len(tenant.projects))

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

		proj2, err = tenant.GetProject(tenantProj2)
		require.NoError(t, err)
		db2 = proj2.database
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
		_, err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
		require.NoError(t, err)
		_, err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil))

		proj1, err := tenant.GetProject(tenantProj1)
		require.NoError(t, err)
		db1 := proj1.database
		require.NoError(t, err)
		require.Equal(t, tenantDb1.Name(), db1.Name())

		proj2, err := tenant.GetProject(tenantProj2)
		require.NoError(t, err)
		db2 := proj2.database
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

func TestTenantManager_SearchIndexes(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	m, ctx, cancel := NewTestTenantMgr(kvStore)
	defer cancel()

	var err error
	searchConfig := config.GetTestSearchConfig()
	searchConfig.AuthKey = "ts_test_key"
	m.searchStore, err = search.NewStore(searchConfig)
	require.NoError(t, err)

	_, err = m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
	require.NoError(t, err)

	tenant := m.tenants["ns-test1"]
	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	_, err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
	require.NoError(t, err)

	require.NoError(t, tenant.reload(ctx, tx, nil, nil))

	proj1, err := tenant.GetProject(tenantProj1)
	require.NoError(t, err)

	jsSchema := []byte(`{
        "title": "test_index",
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
		}
	}`)

	factory, err := schema.BuildSearch("test_index", jsSchema)
	require.NoError(t, err)
	require.NoError(t, tenant.CreateSearchIndex(ctx, tx, proj1, factory))

	indexesInSearchStore, err := tenant.searchStore.AllCollections(ctx)
	require.NoError(t, err)
	require.NotNil(t, indexesInSearchStore[tenant.Encoder.EncodeSearchTableName(tenant.namespace.Id(), proj1.Id(), factory.Name)])

	require.NoError(t, tenant.reload(ctx, tx, nil, indexesInSearchStore))

	proj1, err = tenant.GetProject(tenantProj1)
	require.NoError(t, err)

	index, ok := proj1.search.GetIndex("test_index")
	require.True(t, ok)
	require.Equal(t, "test_index", index.Name)
	require.Equal(t, 1, len(proj1.search.GetIndexes()))

	require.NoError(t, tenant.DeleteSearchIndex(ctx, tx, proj1, "test_index"))
	indexesInSearchStore, err = tenant.searchStore.AllCollections(ctx)
	require.NoError(t, err)
	require.Nil(t, indexesInSearchStore[tenant.getSearchCollName(proj1.Name(), factory.Name)])

	require.NoError(t, tx.Commit(ctx))

	_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())
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

	_, err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
	require.NoError(t, err)
	_, err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
	require.NoError(t, err)

	tenant2 := m.tenants["ns-test2"]

	_, err = tenant2.CreateProject(ctx, tx, tenantProj1, nil)
	require.NoError(t, err)
	_, err = tenant2.CreateProject(ctx, tx, tenantProj2, nil)
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

	proj1, err := tenant.GetProject(tenantProj1)
	require.NoError(t, err)
	db1 := proj1.database

	proj2, err := tenant.GetProject(tenantProj2)
	require.NoError(t, err)
	db2 := proj2.database

	require.NoError(t, tenant.CreateCollection(ctx, tx, db1, factory))
	require.NoError(t, err)
	require.NoError(t, tenant.CreateCollection(ctx, tx, db2, factory))
	require.NoError(t, err)

	// create tenant2 dbs and collections
	proj21, err := tenant2.GetProject(tenantProj1)
	require.NoError(t, err)
	proj22, err := tenant2.GetProject(tenantProj2)
	require.NoError(t, err)

	require.NoError(t, tenant2.CreateCollection(ctx, tx, proj21.database, factory))
	require.NoError(t, err)
	require.NoError(t, tenant2.CreateCollection(ctx, tx, proj22.database, factory))
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

	coll21 := proj21.database.GetCollection("test_collection")
	table21, err := m.encoder.EncodeTableName(tenant2.GetNamespace(), proj21.database, coll21)
	require.NoError(t, err)

	err = tenant2.kvStore.DropTable(ctx, table21)
	require.NoError(t, err)

	for i := 0; i < 150; i++ {
		err = tenant2.kvStore.Insert(ctx, table21, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
		require.NoError(t, err)
	}

	coll22 := proj22.database.GetCollection("test_collection")
	table22, err := m.encoder.EncodeTableName(tenant2.GetNamespace(), proj22.database, coll22)
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

	sz, err = tenant2.DatabaseSize(ctx, proj21.database)
	require.NoError(t, err)
	assert.Equal(t, int64(1694750), sz)

	sz, err = tenant2.CollectionSize(ctx, proj21.database, coll21)
	require.NoError(t, err)
	assert.Equal(t, int64(1694750), sz)

	// db22
	sz, err = tenant2.DatabaseSize(ctx, proj22.database)
	require.NoError(t, err)
	assert.Equal(t, int64(491500), sz)

	sz, err = tenant2.CollectionSize(ctx, proj22.database, coll22)
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
