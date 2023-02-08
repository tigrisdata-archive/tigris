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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/transaction"
)

func TestDatabaseName(t *testing.T) {
	t.Run("with empty branch", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb")

		require.Equal(t, "myDb", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with main branch", func(t *testing.T) {
		databaseBranch := NewDatabaseNameWithBranch("myDb", "main")

		require.Equal(t, "myDb", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with other branch", func(t *testing.T) {
		databaseBranch := NewDatabaseNameWithBranch("myDb", "staging")

		require.Equal(t, "myDb_$branch$_staging", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, "staging", databaseBranch.Branch())
		require.False(t, databaseBranch.IsMainBranch())
	})
}

func TestBranchFromDbName(t *testing.T) {
	t.Run("with db name only", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb")

		require.Equal(t, "myDb", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with main branch name", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$_main")

		require.Equal(t, "myDb", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with some other branch", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$_staging")

		require.Equal(t, "myDb_$branch$_staging", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, "staging", databaseBranch.Branch())
		require.False(t, databaseBranch.IsMainBranch())
	})

	t.Run("with empty string", func(t *testing.T) {
		databaseBranch := NewDatabaseName("")

		require.Equal(t, "", databaseBranch.Name())
		require.Equal(t, "", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with multiple separators", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$__prod_staging_2")

		require.Equal(t, "myDb_$branch$__prod_staging_2", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, "_prod_staging_2", databaseBranch.Branch())
		require.False(t, databaseBranch.IsMainBranch())
	})
}

var testDatabasePayload = &DatabaseMetadata{
	ID: 1,
}

func initDatabaseTest(t *testing.T, ctx context.Context) (*DatabaseSubspace, *transaction.Manager) {
	c := newDatabaseStore(newTestNameRegistry(t))

	_ = kvStore.DropTable(ctx, c.SubspaceName)

	tm := transaction.NewManager(kvStore)

	return c, tm
}

func TestDatabaseSubspace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d, tm := initDatabaseTest(t, ctx)

	defer func() {
		_ = kvStore.DropTable(ctx, d.SubspaceName)
	}()

	t.Run("put_error", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.Equal(t, errors.InvalidArgument("invalid id"), d.insert(ctx, tx, 0, "", testDatabasePayload))
		require.Equal(t, errors.InvalidArgument("database name is empty"), d.insert(ctx, tx, 1, "", testDatabasePayload))
		require.Equal(t, errors.InvalidArgument("invalid nil payload"), d.insert(ctx, tx, 1, "name1", nil))
	})

	t.Run("put_get_1", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, d.insert(ctx, tx, 1, "name2", testDatabasePayload))
		database, err := d.Get(ctx, tx, 1, "name2")
		require.NoError(t, err)
		require.Equal(t, testDatabasePayload, database)

		// already exists
		require.Error(t, d.insert(ctx, tx, 1, "name2", testDatabasePayload))

		// empty metadata
		require.Error(t, d.insert(ctx, tx, 1, "name2", nil))
	})

	t.Run("put_get_2", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		appPayload := &DatabaseMetadata{
			ID: 20,
		}

		require.NoError(t, d.insert(ctx, tx, 1, "name3", appPayload))
		database, err := d.Get(ctx, tx, 1, "name3")
		require.NoError(t, err)
		require.Equal(t, appPayload, database)
	})

	t.Run("put_get_update_get", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, d.insert(ctx, tx, 1, "name4", testDatabasePayload))
		database, err := d.Get(ctx, tx, 1, "name4")
		require.NoError(t, err)
		require.Equal(t, testDatabasePayload, database)

		updatedPayload := &DatabaseMetadata{
			ID: 1,
		}

		require.NoError(t, d.Update(ctx, tx, 1, "name4", updatedPayload))

		database, err = d.Get(ctx, tx, 1, "name4")
		require.NoError(t, err)
		require.Equal(t, updatedPayload, database)
	})

	t.Run("put_get_delete_get", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, d.insert(ctx, tx, 1, "name6", testDatabasePayload))
		database, err := d.Get(ctx, tx, 1, "name6")
		require.NoError(t, err)
		require.Equal(t, testDatabasePayload, database)

		require.NoError(t, d.delete(ctx, tx, 1, "name6"))
		_, err = d.Get(ctx, tx, 1, "name6")
		require.Equal(t, errors.ErrNotFound, err)
	})

	t.Run("list", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, d.insert(ctx, tx, 1, "name8", testDatabasePayload))
		require.NoError(t, d.insert(ctx, tx, 1, "name9", testDatabasePayload))

		colls, err := d.list(ctx, tx, 1)
		require.NoError(t, err)

		require.Equal(t, map[string]*DatabaseMetadata{
			"name8": {ID: 1},
			"name9": {ID: 1},
		}, colls)
	})
}

func TestDatabaseSubspaceNegative(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d, tm := initDatabaseTest(t, ctx)

	t.Run("invalid_short_key_len", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		shortKey := keys.NewKey(d.SubspaceName, d.KeyVersion, UInt32ToByte(1), dbKey)

		err := d.insertMetadata(ctx, tx, nil, shortKey, dbMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = d.list(ctx, tx, 1)
		require.Equal(t, errors.Internal("not a valid key %v", shortKey.IndexParts()), err) //nolint:asasalint
	})

	t.Run("invalid_long_key_len", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		longKey := keys.NewKey(d.SubspaceName, d.KeyVersion, UInt32ToByte(1), dbKey, "name8", "name8", keyEnd)

		err := d.insertMetadata(ctx, tx, nil, longKey, dbMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = d.list(ctx, tx, 1)
		require.Equal(t, errors.Internal("not a valid key %v", longKey.IndexParts()), err) //nolint:asasalint
	})

	t.Run("invalid_suffix_key", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		key := keys.NewKey(d.SubspaceName, d.KeyVersion, UInt32ToByte(1), dbKey, "name8", "some_suffix")

		err := d.insertMetadata(ctx, tx, nil, key, dbMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = d.list(ctx, tx, 1)
		require.Equal(t, errors.Internal("key trailer is missing %v", key.IndexParts()), err) //nolint:asasalint
	})
}

func TestDatabaseSubspaceMigrationV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d, tm := initDatabaseTest(t, ctx)
	defer func() {
		_ = kvStore.DropTable(ctx, d.SubspaceName)
	}()

	tx, cleanupTx := initTx(t, ctx, tm)
	defer cleanupTx()

	// Create collection with ID 1 in legacy v0 format
	key := d.getKey(1, "name7")
	err := d.insertPayload(ctx, tx, nil, key, 0, UInt32ToByte(123))
	require.NoError(t, err)

	// New code is able to read legacy version
	meta, err := d.Get(ctx, tx, 1, "name7")
	require.NoError(t, err)
	require.Equal(t, &DatabaseMetadata{ID: 123}, meta)

	updatedMetadata := &DatabaseMetadata{
		ID: 123,
	}

	// Updating should overwrite with new format
	require.NoError(t, d.Update(ctx, tx, 1, "name7", updatedMetadata))

	// We are able to read in new format
	meta, err = d.Get(ctx, tx, 1, "name7")
	require.NoError(t, err)
	require.Equal(t, &DatabaseMetadata{ID: 123}, meta)
}

func TestUpdateSchemaVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	testDB := "version_test_db"

	k := NewMetadataDictionary(newTestNameRegistry(t))
	testClearDictionary(ctx, k, kvStore)

	tm := transaction.NewManager(kvStore)

	t.Run("first_version", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		err := k.Database().delete(ctx, tx, 1, testDB)
		require.NoError(t, err)

		fb := schema.NewFactoryBuilder(true)

		db := &Database{name: NewDatabaseName(testDB)}

		schFactory, err := fb.Build("", []byte(`{"primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		err = UpdateSchemaVersion(ctx, k, tx, 1, db, schFactory)
		require.NoError(t, err)

		assert.Equal(t, uint32(0), db.PendingSchemaVersion) // no version set

		schFactory, err = fb.Build("", []byte(`{"version":1, "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		err = UpdateSchemaVersion(ctx, k, tx, 1, db, schFactory)
		require.NoError(t, err)

		assert.Equal(t, uint32(1), db.PendingSchemaVersion)
	})

	t.Run("first_version", func(t *testing.T) {
		tx, _ := initTx(t, ctx, tm)

		err := k.Database().delete(ctx, tx, 1, testDB)
		require.NoError(t, err)

		fb := schema.NewFactoryBuilder(true)

		db := &Database{name: NewDatabaseName(testDB)}

		schFactory, err := fb.Build("test_coll1", []byte(`{"title":"test_coll1", "version":1, "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		assert.Equal(t, uint32(0), db.PendingSchemaVersion)
		assert.Equal(t, uint32(0), db.CurrentSchemaVersion)

		err = UpdateSchemaVersion(ctx, k, tx, 1, db, schFactory)
		require.NoError(t, err)

		assert.Equal(t, uint32(1), db.PendingSchemaVersion)
		assert.Equal(t, uint32(0), db.CurrentSchemaVersion)

		err = tx.Commit(ctx)
		require.NoError(t, err)
	})

	t.Run("version_must_be_plus_one", func(t *testing.T) {
		tx, _ := initTx(t, ctx, tm)

		fb := schema.NewFactoryBuilder(true)

		err := k.Database().delete(ctx, tx, 1, testDB)
		require.NoError(t, err)

		db := &Database{name: NewDatabaseName(testDB)}

		schFactory, err := fb.Build("test_coll1", []byte(`{"title":"test_coll1", "version":1, "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		assert.Equal(t, uint32(0), db.PendingSchemaVersion)
		assert.Equal(t, uint32(0), db.CurrentSchemaVersion)

		err = UpdateSchemaVersion(ctx, k, tx, 1, db, schFactory)
		require.NoError(t, err)

		assert.Equal(t, uint32(1), db.PendingSchemaVersion)
		assert.Equal(t, uint32(0), db.CurrentSchemaVersion)

		err = tx.Commit(ctx)
		require.NoError(t, err)

		db = &Database{name: NewDatabaseName(testDB)} // db is staged in tx so should be clear on new tx

		tx, _ = initTx(t, ctx, tm)

		schFactory, err = fb.Build("test_coll1", []byte(`{"title":"test_coll1", "version":3, "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		err = UpdateSchemaVersion(ctx, k, tx, 1, db, schFactory)
		require.Error(t, fmt.Errorf("next schema version must be 2"), err)

		schFactory, err = fb.Build("test_coll1", []byte(`{"title":"test_coll1", "version":2, "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		err = UpdateSchemaVersion(ctx, k, tx, 1, db, schFactory)
		require.NoError(t, err)

		err = tx.Commit(ctx)
		require.NoError(t, err)
	})

	// test calculating first explicit schema version
	t.Run("max_schema_version", func(t *testing.T) {
		tx, _ := initTx(t, ctx, tm)

		fb := schema.NewFactoryBuilder(true)

		err := k.Database().delete(ctx, tx, 2, testDB)
		require.NoError(t, err)

		db := &Database{name: NewDatabaseName(testDB), id: 12345}

		err = k.schemaStore.Delete(ctx, tx, 2, db.Id(), 123)
		require.NoError(t, err)

		// simulate existing collections with some schema versions
		// 7 and 10 to be exact
		for i := 1; i <= 7; i++ {
			sch := []byte(`{"title":"test_coll2", "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`)

			err = k.schemaStore.Put(ctx, tx, 2, db.Id(), 123, sch, uint32(i))
			require.NoError(t, err)
		}

		err = k.schemaStore.Delete(ctx, tx, 2, db.Id(), 1230)
		require.NoError(t, err)

		for i := 1; i <= 10; i++ {
			sch := []byte(`{"title":"test_coll3", "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`)

			err = k.schemaStore.Put(ctx, tx, 2, db.Id(), 1230, sch, uint32(i))
			require.NoError(t, err)
		}

		err = tx.Commit(ctx)
		require.NoError(t, err)

		db = &Database{
			name: NewDatabaseName(testDB), id: 12345,
			collections: map[string]*collectionHolder{
				"test_coll2": {collection: &schema.DefaultCollection{Id: 123}},
				"test_coll3": {collection: &schema.DefaultCollection{Id: 1230}},
			},
		} // db is staged in tx so should be clear on new tx

		tx, _ = initTx(t, ctx, tm)

		schFactory, err := fb.Build("test_coll1", []byte(`{"title":"test_coll1", "version":3, "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		err = UpdateSchemaVersion(ctx, k, tx, 2, db, schFactory)
		require.Equal(t, errors.InvalidArgument("next schema version should be 11"), err)

		schFactory, err = fb.Build("test_coll1", []byte(`{"title":"test_coll1", "version":11, "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		err = UpdateSchemaVersion(ctx, k, tx, 2, db, schFactory)
		require.NoError(t, err)

		assert.Equal(t, uint32(11), db.PendingSchemaVersion)

		err = tx.Commit(ctx)
		require.NoError(t, err)
	})

	t.Run("change_version_in_tx", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		err := k.Database().delete(ctx, tx, 1, testDB)
		require.NoError(t, err)

		fb := schema.NewFactoryBuilder(true)
		schFactory, err := fb.Build("", []byte(`{"version":1, "primary_key": ["id"], "properties" : { "id" : { "type" : "integer" } } }`))
		require.NoError(t, err)

		db := &Database{}
		db.PendingSchemaVersion = 2

		err = UpdateSchemaVersion(ctx, k, tx, 1, db, schFactory)
		require.Equal(t,
			errors.InvalidArgument("collection schema version should be the same in the transaction. got 1 expected 2"),
			err,
		)

		db.PendingSchemaVersion = 1
		err = UpdateSchemaVersion(ctx, k, tx, 1, db, schFactory)
		require.NoError(t, err)
	})
}
