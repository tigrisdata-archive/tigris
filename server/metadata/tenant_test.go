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
	kvStore     kv.TxStore
	tenantProj1 = "tenant_db1"
	tenantProj2 = "tenant_db2"
	tenantDb1   = NewDatabaseName("tenant_db1")
	tenantDb2   = NewDatabaseName("tenant_db2")
)

func TestTenantManager_CreateOrGetTenant(t *testing.T) {
	t.Run("create_tenant", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
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
		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
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
		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		// should fail now
		_, err = m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 3, NewNamespaceMetadata(3, "ns-test1", "ns-test1-display_name")})
		require.Equal(t, "id is already assigned to strId='ns-test1'", err.(*api.TigrisError).Error())

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_id_error", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
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
	t.Run("create_tenant", func(t *testing.T) {
		tm := transaction.NewManager(kvStore)

		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
		defer cancel()

		tx, e := tm.StartTx(ctx)
		require.NoError(t, e)
		_, err := m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)
		namespaces, err := m.metaStore.GetNamespaces(ctx, tx)
		require.NoError(t, err)
		metadata := namespaces["ns-test1"]
		require.Equal(t, uint32(2), metadata.Id)
		require.NoError(t, tx.Commit(ctx))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})

	t.Run("create_multiple_tenants", func(t *testing.T) {
		tm := transaction.NewManager(kvStore)

		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
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

		require.NoError(t, tx.Commit(ctx))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_error", func(t *testing.T) {
		tm := transaction.NewManager(kvStore)

		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
		defer cancel()

		tx, e := tm.StartTx(ctx)
		require.NoError(t, e)
		_, err := m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		// should fail now
		_, err = m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 3, NewNamespaceMetadata(3, "ns-test1", "ns-test1-display_name")})
		require.Equal(t, "namespace with same name already exists with id '2'", err.(*api.TigrisError).Error())

		require.NoError(t, tx.Commit(ctx))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
	t.Run("create_duplicate_tenant_id_error", func(t *testing.T) {
		tm := transaction.NewManager(kvStore)

		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
		defer cancel()

		tx, e := tm.StartTx(ctx)
		require.NoError(t, e)
		_, err := m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		// should fail now
		_, err = m.CreateTenant(ctx, tx, &TenantNamespace{"ns-test2", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.Equal(t, "namespace with same id already exists with name 'ns-test1'", err.(*api.TigrisError).Error())

		require.NoError(t, tx.Commit(ctx))

		_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	})
}

func TestTenantManager_CreateProjects(t *testing.T) {
	t.Run("create_projects", func(t *testing.T) {
		tm := transaction.NewManager(kvStore)

		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
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
		err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
		require.NoError(t, err)
		err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = tenant.reload(ctx, tx, nil, nil, tm)
		require.NoError(t, err)
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

		testClearDictionary(ctx, m.metaStore, m.kvStore)
	})
}

func TestTenantManager_DatabaseBranches(t *testing.T) {
	tm := transaction.NewManager(kvStore)

	m, ctx, cancel := NewTestTenantMgr(t, kvStore)
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
	err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
	require.NoError(t, err)
	err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
	require.NoError(t, err)
	unknownProject := "project_not_exists"

	databases := (&Project{}).GetDatabaseWithBranches()
	require.Len(t, databases, 0)

	require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj1, NewDatabaseNameWithBranch(tenantProj1, "branch1")))
	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj2, NewDatabaseNameWithBranch(tenantProj2, "branch1")))
	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj1, NewDatabaseNameWithBranch(tenantProj1, "branch2")))
	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj2, NewDatabaseNameWithBranch(tenantProj2, "branch2")))
	require.NoError(t, tenant.CreateBranch(ctx, tx, tenantProj1, NewDatabaseNameWithBranch(tenantProj1, "branch3")))
	require.ErrorContains(t, tenant.CreateBranch(ctx, tx, unknownProject, NewDatabaseNameWithBranch(unknownProject, "branch1")), "project doesn't exist")

	// reload again to get all the branches
	require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

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

	databases = proj1.GetDatabaseWithBranches()
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
	require.ErrorContains(t, tenant.DeleteBranch(ctx, tx, unknownProject, NewDatabaseNameWithBranch(unknownProject, "branch1")), "project doesn't exist")

	require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))
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

	testClearDictionary(ctx, m.metaStore, m.kvStore)
}

func TestTenantManager_CreateCollections(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	t.Run("create_collections", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		tenant := m.tenants["ns-test1"]
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
		require.NoError(t, err)
		err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

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
				"type": "integer",
				"index": true
			},
			"D1": {
				"type": "string",
				"index": true,
				"maxLength": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)

		factory, err := schema.NewFactoryBuilder(true).Build("test_collection", jsSchema)
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(ctx, tx, db2, factory))

		require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

		proj2, err = tenant.GetProject(tenantProj2)
		require.NoError(t, err)
		db2 = proj2.database
		collection := db2.GetCollection("test_collection")
		require.Equal(t, "test_collection", collection.Name)
		require.Equal(t, "test_collection", db2.idToCollectionMap[collection.Id])

		require.NoError(t, tx.Commit(ctx))

		testClearDictionary(ctx, m.metaStore, m.kvStore)
	})
}

func TestTenantManager_DropCollection(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	t.Run("drop_collection", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		tenant := m.tenants["ns-test1"]

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
		require.NoError(t, err)
		err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

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

		factory, err := schema.NewFactoryBuilder(true).Build("test_collection", jsSchema)
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(ctx, tx, db2, factory))
		require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))
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

		coll := db2.GetCollection("test_collection")
		require.Nil(t, coll)
		require.Empty(t, db2.idToCollectionMap[coll1.Id])

		testClearDictionary(ctx, m.metaStore, m.kvStore)
	})
}

func TestTenantManager_SearchIndexes(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	m, ctx, cancel := NewTestTenantMgr(t, kvStore)
	defer cancel()

	var err error
	searchConfig := config.GetTestSearchConfig()
	searchConfig.AuthKey = "ts_test_key"
	m.searchStore = search.NewStore(searchConfig, false)
	require.NoError(t, err)

	_, err = m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
	require.NoError(t, err)

	tenant := m.tenants["ns-test1"]
	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
	require.NoError(t, err)

	require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

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

	factory, err := schema.NewFactoryBuilder(true).BuildSearch("test_index", jsSchema)
	require.NoError(t, err)
	require.NoError(t, tenant.CreateSearchIndex(ctx, tx, proj1, factory))

	indexesInSearchStore, err := tenant.searchStore.AllCollections(ctx)
	require.NoError(t, err)
	require.NotNil(t, indexesInSearchStore[tenant.Encoder.EncodeSearchTableName(tenant.namespace.Id(), proj1.Id(), factory.Name)])

	require.NoError(t, tenant.reload(ctx, tx, nil, indexesInSearchStore, tm))

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

	testClearDictionary(ctx, m.metaStore, m.kvStore)
}

func TestTenantManager_SecondaryIndexes(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	t.Run("create_collections", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		tenant := m.tenants["ns-test1"]
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
		require.NoError(t, err)
		err = tenant.CreateProject(ctx, tx, tenantProj2, nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

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
				"type": "string",
				"index": true
			},
			"K2": {
				"type": "integer",
				"index": true
			},
			"D1": {
				"type": "string",
				"maxLength": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`)

		factory, err := schema.NewFactoryBuilder(true).Build("test_collection", jsSchema)
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(ctx, tx, db2, factory))

		require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

		proj2, err = tenant.GetProject(tenantProj2)
		require.NoError(t, err)
		db2 = proj2.database
		collection := db2.GetCollection("test_collection")
		require.Equal(t, "test_collection", collection.Name)
		require.Equal(t, "test_collection", db2.idToCollectionMap[collection.Id])
		require.Len(t, db2.idToCollectionMap, 1)
		// K1, K2, _tigris_created_at, _tigris_updated_at
		require.Len(t, collection.GetActiveIndexedFields(), 4)
		require.Len(t, collection.GetIndexedFields(), 4)

		require.NoError(t, tx.Commit(ctx))

		testClearDictionary(ctx, m.metaStore, m.kvStore)
	})

	t.Run("update_collections", func(t *testing.T) {
		m, ctx, cancel := NewTestTenantMgr(t, kvStore)
		defer cancel()

		_, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 2, NewNamespaceMetadata(2, "ns-test1", "ns-test1-display_name")})
		require.NoError(t, err)

		tenant := m.tenants["ns-test1"]
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		err = tenant.CreateProject(ctx, tx, tenantProj1, nil)
		require.NoError(t, err)

		require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

		proj1, err := tenant.GetProject(tenantProj1)
		require.NoError(t, err)
		require.Equal(t, tenantProj1, proj1.Name())
		db1 := proj1.database
		require.NoError(t, err)
		require.Equal(t, tenantDb1.Name(), db1.Name())
		require.Equal(t, tenantDb1.Name(), tenant.idToDatabaseMap[db1.id].Name())

		jsSchema := []byte(`{
        "title": "test_collection",
		"properties": {
			"K1": {
				"type": "string",
				"index": true
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

		factory, err := schema.NewFactoryBuilder(true).Build("test_collection", jsSchema)
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(ctx, tx, db1, factory))

		require.NoError(t, tenant.reload(ctx, tx, nil, nil, tm))

		proj1, err = tenant.GetProject(tenantProj1)
		require.NoError(t, err)
		db1 = proj1.database
		collection := db1.GetCollection("test_collection")
		require.Equal(t, "test_collection", collection.Name)
		require.Equal(t, "test_collection", db1.idToCollectionMap[collection.Id])
		require.Len(t, db1.idToCollectionMap, 1)
		// K1, _tigris_created_at, _tigris_updated_at
		require.Len(t, collection.GetActiveIndexedFields(), 3)
		require.Len(t, collection.GetIndexedFields(), 3)

		jsSchemaUpdate := []byte(`{
        "title": "test_collection",
		"properties": {
			"K1": {
				"type": "string",
				"index": true
			},
			"K2": {
				"type": "integer",
				"index": true
			},
			"D1": {
				"type": "string",
				"maxLength": 128,
				"index": true
			}
		},
		"primary_key": ["K1", "K2"]
	}`)

		factory, err = schema.NewFactoryBuilder(true).Build("test_collection", jsSchemaUpdate)
		require.NoError(t, err)
		require.NoError(t, tenant.CreateCollection(ctx, tx, db1, factory))
		collection = db1.GetCollection("test_collection")
		require.Len(t, collection.GetActiveIndexedFields(), 3)
		// K1, K2, D1, _tigris_created_at, _tigris_updated_at
		require.Len(t, collection.GetIndexedFields(), 5)

		require.NoError(t, tx.Commit(ctx))

		testClearDictionary(ctx, m.metaStore, m.kvStore)
	})
}

func TestTenantManager_DataSize(t *testing.T) {
	m, ctx, cancel := NewTestTenantMgr(t, kvStore)
	defer cancel()

	ns1id := uint32(2)
	ns2id := uint32(3)

	ns1 := &TenantNamespace{"ns-test1", ns1id, NewNamespaceMetadata(ns1id, "ns-test1", "ns-test1-display_name")}
	ns2 := &TenantNamespace{"ns-test2", ns2id, NewNamespaceMetadata(ns2id, "ns-test2", "ns-test2-display_name")}

	_, err := m.CreateOrGetTenant(ctx, ns1)
	require.NoError(t, err)

	_, err = m.CreateOrGetTenant(ctx, ns2)
	require.NoError(t, err)

	tenant := m.tenants["ns-test1"]
	tenant2 := m.tenants["ns-test2"]

	docSize := 10 * 1024

	db1 := &Database{id: 10, collections: make(map[string]*collectionHolder)}
	db2 := &Database{id: 11}

	tenant.projects = make(map[string]*Project)
	tenant.projects["db1"] = &Project{database: db1}
	tenant.projects["db2"] = &Project{database: db2}

	coll1 := &schema.DefaultCollection{Id: 256}
	coll2 := &schema.DefaultCollection{Id: 512}
	db1.collections["coll1"] = &collectionHolder{collection: coll1}
	db1.collections["coll2"] = &collectionHolder{collection: coll2}

	coll21 := &schema.DefaultCollection{Id: 1024}
	coll22 := &schema.DefaultCollection{Id: 2048}
	db21 := &Database{id: 20, collections: make(map[string]*collectionHolder)}
	db22 := &Database{id: 30, collections: make(map[string]*collectionHolder)}

	tenant2.projects = make(map[string]*Project)
	tenant2.projects["db21"] = &Project{database: db21}
	tenant2.projects["db22"] = &Project{database: db22}

	db21.collections["coll21"] = &collectionHolder{collection: coll21}
	db22.collections["coll22"] = &collectionHolder{collection: coll22}

	t.Run("coll_size", func(t *testing.T) {
		table, err := m.encoder.EncodeTableName(ns1, db1, coll1)
		require.NoError(t, err)
		coll1.EncodedName = table

		err = kvStore.DropTable(ctx, table)
		require.NoError(t, err)

		tx, err := kvStore.BeginTx(ctx)
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			err = tx.Insert(ctx, table, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		table2, err := m.encoder.EncodeTableName(ns1, db1, coll2)
		require.NoError(t, err)
		coll2.EncodedName = table2

		err = kvStore.DropTable(ctx, table2)
		require.NoError(t, err)

		tx, err = kvStore.BeginTx(ctx)
		require.NoError(t, err)
		for i := 0; i < 200; i++ {
			err = tx.Insert(ctx, table2, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		// for second namespace insert 150 in one project and 50 record in to other
		table21, err := m.encoder.EncodeTableName(ns2, db21, coll21)
		require.NoError(t, err)
		coll21.EncodedName = table21

		err = kvStore.DropTable(ctx, table21)
		require.NoError(t, err)

		tx, err = kvStore.BeginTx(ctx)
		require.NoError(t, err)
		for i := 0; i < 110; i++ {
			err = tx.Insert(ctx, table21, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		table22, err := m.encoder.EncodeTableName(ns2, db22, coll22)
		require.NoError(t, err)
		coll22.EncodedName = table22

		err = kvStore.DropTable(ctx, table22)
		require.NoError(t, err)

		tx, err = kvStore.BeginTx(ctx)
		require.NoError(t, err)
		for i := 0; i < 150; i++ {
			err = tx.Insert(ctx, table22, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		// Tenant 1
		// db1
		sz, err := tenant.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(3072000), sz.StoredBytes)

		sz, err = tenant.DatabaseSize(ctx, db1)
		require.NoError(t, err)
		assert.Equal(t, int64(3072000), sz.StoredBytes)

		sz, err = tenant.CollectionSize(ctx, db1, coll1)
		require.NoError(t, err)
		assert.Equal(t, int64(1024000), sz.StoredBytes)

		sz, err = tenant.CollectionSize(ctx, db1, coll2)
		require.NoError(t, err)
		assert.Equal(t, int64(2048000), sz.StoredBytes)

		// db2 is empty
		sz, err = tenant.DatabaseSize(ctx, db2)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		// Tenant 2
		// db21
		sz, err = tenant2.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2662400), sz.StoredBytes)

		sz, err = tenant2.DatabaseSize(ctx, db21)
		require.NoError(t, err)
		assert.Equal(t, int64(1126400), sz.StoredBytes)

		sz, err = tenant2.CollectionSize(ctx, db21, coll21)
		require.NoError(t, err)
		assert.Equal(t, int64(1126400), sz.StoredBytes)

		// db22
		sz, err = tenant2.DatabaseSize(ctx, db22)
		require.NoError(t, err)
		assert.Equal(t, int64(1536000), sz.StoredBytes)

		sz, err = tenant2.CollectionSize(ctx, db22, coll22)
		require.NoError(t, err)
		assert.Equal(t, int64(1536000), sz.StoredBytes)

		// cleanup
		tns1, err := m.encoder.EncodeTableName(tenant.GetNamespace(), nil, nil)
		require.NoError(t, err)
		err = tenant2.kvStore.DropTable(ctx, tns1)
		require.NoError(t, err)

		tns2, err := m.encoder.EncodeTableName(tenant2.GetNamespace(), nil, nil)
		require.NoError(t, err)
		err = tenant2.kvStore.DropTable(ctx, tns2)
		require.NoError(t, err)

		sz, err = tenant.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		sz, err = tenant2.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		testClearDictionary(ctx, m.metaStore, m.kvStore)
	})

	t.Run("index_size", func(t *testing.T) {
		index, err := m.encoder.EncodeSecondaryIndexTableName(ns1, db1, coll1)
		require.NoError(t, err)
		coll1.EncodedTableIndexName = index

		err = kvStore.DropTable(ctx, index)
		require.NoError(t, err)

		tx, err := kvStore.BeginTx(ctx)
		require.NoError(t, err)

		for i := 0; i < 300; i++ {
			err = tx.Insert(ctx, index, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		index2, err := m.encoder.EncodeSecondaryIndexTableName(ns1, db1, coll2)
		require.NoError(t, err)
		coll2.EncodedTableIndexName = index2

		err = kvStore.DropTable(ctx, index2)
		require.NoError(t, err)

		tx, err = kvStore.BeginTx(ctx)
		require.NoError(t, err)
		for i := 0; i < 600; i++ {
			err = tx.Insert(ctx, index2, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		// for second namespace insert 150 in one project and 50 record in to other
		index21, err := m.encoder.EncodeSecondaryIndexTableName(ns2, db21, coll21)
		require.NoError(t, err)
		coll21.EncodedTableIndexName = index21

		err = kvStore.DropTable(ctx, index21)
		require.NoError(t, err)

		tx, err = kvStore.BeginTx(ctx)
		require.NoError(t, err)
		for i := 0; i < 330; i++ {
			err = tx.Insert(ctx, index21, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		index22, err := m.encoder.EncodeSecondaryIndexTableName(ns2, db22, coll22)
		require.NoError(t, err)
		coll22.EncodedTableIndexName = index22

		err = kvStore.DropTable(ctx, index22)
		require.NoError(t, err)

		tx, err = kvStore.BeginTx(ctx)
		require.NoError(t, err)
		for i := 0; i < 450; i++ {
			err = tx.Insert(ctx, index22, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		// Tenant 1
		// db1
		sz, err := tenant.IndexSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(9216000), sz.StoredBytes)

		sz, err = tenant.DatabaseIndexSize(ctx, db1)
		require.NoError(t, err)
		assert.Equal(t, int64(9216000), sz.StoredBytes)

		sz, err = tenant.CollectionIndexSize(ctx, db1, coll1)
		require.NoError(t, err)
		assert.Equal(t, int64(3072000), sz.StoredBytes)

		sz, err = tenant.CollectionIndexSize(ctx, db1, coll2)
		require.NoError(t, err)
		assert.Equal(t, int64(6144000), sz.StoredBytes)

		// db2 is empty
		sz, err = tenant.DatabaseIndexSize(ctx, db2)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		// Tenant 2
		// db21
		sz, err = tenant2.IndexSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(7987200), sz.StoredBytes)

		sz, err = tenant2.DatabaseIndexSize(ctx, db21)
		require.NoError(t, err)
		assert.Equal(t, int64(3379200), sz.StoredBytes)

		sz, err = tenant2.CollectionIndexSize(ctx, db21, coll21)
		require.NoError(t, err)
		assert.Equal(t, int64(3379200), sz.StoredBytes)

		// db22
		sz, err = tenant2.DatabaseIndexSize(ctx, db22)
		require.NoError(t, err)
		assert.Equal(t, int64(4608000), sz.StoredBytes)

		sz, err = tenant2.CollectionIndexSize(ctx, db22, coll22)
		require.NoError(t, err)
		assert.Equal(t, int64(4608000), sz.StoredBytes)

		// cleanup
		tns1, err := m.encoder.EncodeSecondaryIndexTableName(tenant.GetNamespace(), nil, nil)
		require.NoError(t, err)
		err = tenant2.kvStore.DropTable(ctx, tns1)
		require.NoError(t, err)

		tns2, err := m.encoder.EncodeSecondaryIndexTableName(tenant2.GetNamespace(), nil, nil)
		require.NoError(t, err)
		err = tenant2.kvStore.DropTable(ctx, tns2)
		require.NoError(t, err)

		sz, err = tenant.IndexSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		sz, err = tenant2.IndexSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		testClearDictionary(ctx, m.metaStore, m.kvStore)
	})
}

func TestTenantManager_SearchDataSize(t *testing.T) {
	tm := transaction.NewManager(kvStore)
	m, ctx, cancel := NewTestTenantMgr(t, kvStore)
	defer cancel()

	docSize := 10 * 1024

	var err error
	searchConfig := config.GetTestSearchConfig()
	searchConfig.AuthKey = "ts_test_key"
	m.searchStore = search.NewStore(searchConfig, false)
	require.NoError(t, err)

	tenant, err := m.CreateOrGetTenant(ctx, &TenantNamespace{"ns-test1", 1, NewNamespaceMetadata(1, "ns-test1", "ns-test1-display_name")})
	require.NoError(t, err)

	tmTx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	err = tenant.CreateProject(ctx, tmTx, tenantProj1, nil)
	require.NoError(t, err)

	err = tenant.CreateProject(ctx, tmTx, tenantProj2, nil)
	require.NoError(t, err)
	require.NoError(t, tenant.reload(ctx, tmTx, nil, nil, tm))

	// proj1
	proj1, err := tenant.GetProject(tenantProj1)
	require.NoError(t, err)

	// proj2
	proj2, err := tenant.GetProject(tenantProj2)
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
	factory1, err := schema.NewFactoryBuilder(true).BuildSearch("prj1_test_index_1", jsSchema)
	require.NoError(t, err)

	require.NoError(t, tenant.CreateSearchIndex(ctx, tmTx, proj1, factory1))

	factory2, err := schema.NewFactoryBuilder(true).BuildSearch("prj1_test_index_2", jsSchema)
	require.NoError(t, err)

	require.NoError(t, tenant.CreateSearchIndex(ctx, tmTx, proj1, factory2))

	factory3, err := schema.NewFactoryBuilder(true).BuildSearch("prj2_test_index", jsSchema)
	require.NoError(t, err)

	require.NoError(t, tenant.CreateSearchIndex(ctx, tmTx, proj2, factory3))

	indexesInSearchStore, err := tenant.searchStore.AllCollections(ctx)
	require.NoError(t, err)
	require.NotNil(t, indexesInSearchStore[tenant.Encoder.EncodeSearchTableName(tenant.namespace.Id(), proj1.Id(), factory1.Name)])
	require.NotNil(t, indexesInSearchStore[tenant.Encoder.EncodeSearchTableName(tenant.namespace.Id(), proj1.Id(), factory2.Name)])
	require.NotNil(t, indexesInSearchStore[tenant.Encoder.EncodeSearchTableName(tenant.namespace.Id(), proj2.Id(), factory3.Name)])

	t.Run("search_index_size", func(t *testing.T) {
		sz, err := tenant.ProjectSearchSize(ctx, tmTx, proj1)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		sz, err = tenant.ProjectSearchSize(ctx, tmTx, proj2)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		sz, err = tenant.SearchSize(ctx, tmTx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), sz.StoredBytes)

		// proj1 , "prj1_test_index_1"
		index, ok := proj1.search.GetIndex("prj1_test_index_1")
		require.True(t, ok)
		table := m.encoder.EncodeFDBSearchTableName(index.StoreIndexName())
		err = kvStore.DropTable(ctx, table)
		require.NoError(t, err)

		tx, err := kvStore.BeginTx(ctx)
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			err = tx.Insert(ctx, table, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		sz, err = tenant.SearchIndexSize(ctx, index)
		require.NoError(t, err)
		assert.Equal(t, int64(1024000), sz.StoredBytes)

		index, ok = proj2.search.GetIndex("prj2_test_index")
		require.True(t, ok)
		table = m.encoder.EncodeFDBSearchTableName(index.StoreIndexName())
		err = kvStore.DropTable(ctx, table)
		require.NoError(t, err)

		tx, err = kvStore.BeginTx(ctx)
		require.NoError(t, err)

		for i := 0; i < 150; i++ {
			err = tx.Insert(ctx, table, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		sz, err = tenant.SearchIndexSize(ctx, index)
		require.NoError(t, err)
		assert.Equal(t, int64(1536000), sz.StoredBytes)

		index, ok = proj1.search.GetIndex("prj1_test_index_2")
		require.True(t, ok)
		table = m.encoder.EncodeFDBSearchTableName(index.StoreIndexName())
		err = kvStore.DropTable(ctx, table)
		require.NoError(t, err)

		tx, err = kvStore.BeginTx(ctx)
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			err = tx.Insert(ctx, table, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
			require.NoError(t, err)
		}
		_ = tx.Commit(ctx)

		sz, err = tenant.ProjectSearchSize(ctx, tmTx, proj1)
		require.NoError(t, err)
		assert.Equal(t, int64(2048000), sz.StoredBytes)

		sz, err = tenant.ProjectSearchSize(ctx, tmTx, proj2)
		require.NoError(t, err)
		assert.Equal(t, int64(1536000), sz.StoredBytes)

		// search size
		sz, err = tenant.SearchSize(ctx, tmTx)
		require.NoError(t, err)
		assert.Equal(t, int64(3584000), sz.StoredBytes)

		testClearDictionary(ctx, m.metaStore, m.kvStore)
	})
}

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled", Format: "console"})

	fdbCfg, err := config.GetTestFDBConfig("../..")
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB config: %v", err))
	}

	kvStore, err = kv.NewBuilder().WithStats().Build(fdbCfg)
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB KV %v", err))
	}

	os.Exit(m.Run())
}
