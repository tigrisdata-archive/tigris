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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
)

var testCollectionMetadata = &CollectionMetadata{
	ID: 10,
}

func initCollectionTest(t *testing.T, ctx context.Context) (*CollectionSubspace, *transaction.Manager, func()) {
	c := newCollectionStore(newTestNameRegistry(t))

	_ = kvStore.DropTable(ctx, c.SubspaceName)

	tm := transaction.NewManager(kvStore)

	return c, tm, func() {
		_ = kvStore.DropTable(ctx, c.SubspaceName)
	}
}

func initTx(t *testing.T, ctx context.Context, tm *transaction.Manager) (transaction.Tx, func()) {
	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)

	return tx, func() {
		assert.NoError(t, tx.Rollback(ctx))
	}
}

func TestCollectionSubspace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, tm, cleanup := initCollectionTest(t, ctx)
	defer cleanup()

	t.Run("put_error", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.Equal(t, errors.InvalidArgument("invalid id"),
			c.insert(ctx, tx, 0, 1, "", testCollectionMetadata))
		require.Equal(t, errors.InvalidArgument("invalid id"),
			c.insert(ctx, tx, 1, 0, "", testCollectionMetadata))
		require.Equal(t, errors.InvalidArgument("empty collection name"),
			c.insert(ctx, tx, 1, 1, "", testCollectionMetadata))
		require.Equal(t, errors.InvalidArgument("invalid nil payload"),
			c.insert(ctx, tx, 1, 1, "name", nil))
	})

	t.Run("put_get_1", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, c.insert(ctx, tx, 1, 1, "name1", testCollectionMetadata))
		collection, err := c.Get(ctx, tx, 1, 1, "name1")
		require.NoError(t, err)
		require.Equal(t, testCollectionMetadata, collection)

		// already exists
		require.Error(t, c.insert(ctx, tx, 1, 1, "name1", testCollectionMetadata))

		// empty metadata
		require.Error(t, c.insert(ctx, tx, 1, 1, "name1", nil))
	})

	t.Run("put_get_2", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		appPayload := &CollectionMetadata{
			ID: 20,
		}

		require.NoError(t, c.insert(ctx, tx, 1, 1, "name2", appPayload))
		collection, err := c.Get(ctx, tx, 1, 1, "name2")
		require.NoError(t, err)
		require.Equal(t, appPayload, collection)
	})

	t.Run("put_get_update_get", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, c.insert(ctx, tx, 1, 1, "name3", testCollectionMetadata))
		collection, err := c.Get(ctx, tx, 1, 1, "name3")
		require.NoError(t, err)
		require.Equal(t, testCollectionMetadata, collection)

		updatedCollectionPayload := &CollectionMetadata{
			ID: 30,
		}

		require.NoError(t, c.Update(ctx, tx, 1, 1, "name3", updatedCollectionPayload))
		collection, err = c.Get(ctx, tx, 1, 1, "name3")
		require.NoError(t, err)
		require.Equal(t, updatedCollectionPayload, collection)
	})

	t.Run("put_get_delete_get", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, c.insert(ctx, tx, 1, 1, "name5", testCollectionMetadata))
		collection, err := c.Get(ctx, tx, 1, 1, "name5")
		require.NoError(t, err)
		require.Equal(t, testCollectionMetadata, collection)

		require.NoError(t, c.delete(ctx, tx, 1, 1, "name5"))
		_, err = c.Get(ctx, tx, 1, 1, "name5")
		require.Error(t, errors.ErrNotFound, err)
	})

	t.Run("list", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, c.insert(ctx, tx, 1, 1, "name8", testCollectionMetadata))
		require.NoError(t, c.insert(ctx, tx, 1, 1, "name9", testCollectionMetadata))

		colls, err := c.list(ctx, tx, 1, 1)
		require.NoError(t, err)

		require.Equal(t, map[string]*CollectionMetadata{
			"name8": {ID: 10},
			"name9": {ID: 10},
		}, colls)
	})
}

func TestCollectionSubspaceNegative(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, tm, cleanup := initCollectionTest(t, ctx)
	defer cleanup()

	t.Run("invalid_short_key_len", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		shortKey := keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(1), UInt32ToByte(1), collectionKey, keyEnd)

		err := c.insertMetadata(ctx, tx, nil, shortKey, collMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = c.list(ctx, tx, 1, 1)
		require.Equal(t, errors.Internal("not a valid key %v", shortKey.IndexParts()), err) //nolint:asasalint
	})

	t.Run("invalid_long_key_len", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		longKey := keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(1), UInt32ToByte(1), collectionKey, "name8", "name8", keyEnd)

		err := c.insertMetadata(ctx, tx, nil, longKey, collMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = c.list(ctx, tx, 1, 1)
		require.Equal(t, errors.Internal("not a valid key %v", longKey.IndexParts()), err) //nolint:asasalint
	})

	t.Run("invalid_suffix_key", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		key := keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(1), UInt32ToByte(1), collectionKey, "name8", "some_suffix")

		err := c.insertMetadata(ctx, tx, nil, key, collMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = c.list(ctx, tx, 1, 1)
		require.Equal(t, errors.Internal("key trailer is missing %v", key.IndexParts()), err) //nolint:asasalint
	})
}

func TestCollectionSubspaceMigrationV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, tm, cleanup := initCollectionTest(t, ctx)
	defer cleanup()

	tx, cleanupTx := initTx(t, ctx, tm)
	defer cleanupTx()

	// Create collection with ID 1 in legacy v0 format
	key := c.getKey(1, 1, "name7")
	err := c.insertPayload(ctx, tx, nil, key, 0, UInt32ToByte(123))
	require.NoError(t, err)

	// New code is able to read legacy version
	collMeta, err := c.Get(ctx, tx, 1, 1, "name7")
	require.NoError(t, err)
	require.Equal(t, &CollectionMetadata{ID: 123}, collMeta)

	updatedCollectionMetadata := &CollectionMetadata{
		ID: 123,
	}

	// Updating should overwrite with new format
	require.NoError(t, c.Update(ctx, tx, 1, 1, "name7", updatedCollectionMetadata))

	// We are able to read in new format
	collMeta, err = c.Get(ctx, tx, 1, 1, "name7")
	require.NoError(t, err)
	require.Equal(t, &CollectionMetadata{ID: 123}, collMeta)
}
