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

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
)

var testIndexMetadata = &IndexMetadata{}

func initIndexTest(t *testing.T, ctx context.Context) (*IndexSubspace, *transaction.Manager) {
	c := newIndexStore(newTestNameRegistry(t))

	_ = kvStore.DropTable(ctx, c.SubspaceName)

	tm := transaction.NewManager(kvStore)

	return c, tm
}

func TestIndexSubspace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, tm := initIndexTest(t, ctx)
	defer func() {
		_ = kvStore.DropTable(ctx, c.SubspaceName)
	}()

	t.Run("put_error", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.Equal(t, errors.InvalidArgument("invalid id"),
			c.insert(ctx, tx, 0, 1, 1, "", testIndexMetadata))
		require.Equal(t, errors.InvalidArgument("invalid id"),
			c.insert(ctx, tx, 1, 0, 1, "", testIndexMetadata))
		require.Equal(t, errors.InvalidArgument("empty index name"),
			c.insert(ctx, tx, 1, 1, 1, "", testIndexMetadata))
		require.Equal(t, errors.InvalidArgument("invalid nil payload"),
			c.insert(ctx, tx, 1, 1, 1, "name", nil))
	})

	t.Run("put_get_1", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, c.insert(ctx, tx, 1, 1, 1, "name1", testIndexMetadata))
		index, err := c.Get(ctx, tx, 1, 1, 1, "name1")
		require.NoError(t, err)
		require.Equal(t, testIndexMetadata, index)

		// already exists
		require.Error(t, c.insert(ctx, tx, 1, 1, 1, "name1", testIndexMetadata))

		// empty metadata
		require.Error(t, c.insert(ctx, tx, 1, 1, 1, "name1", nil))
	})

	t.Run("put_get_2", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		appPayload := &IndexMetadata{
			Name: "name111",
		}

		require.NoError(t, c.insert(ctx, tx, 1, 1, 1, "name2", appPayload))
		index, err := c.Get(ctx, tx, 1, 1, 1, "name2")
		require.NoError(t, err)
		require.Equal(t, appPayload, index)
	})

	t.Run("put_get_update_get", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, c.insert(ctx, tx, 1, 1, 1, "name3", testIndexMetadata))
		index, err := c.Get(ctx, tx, 1, 1, 1, "name3")
		require.NoError(t, err)
		require.Equal(t, testIndexMetadata, index)

		updatedPayload := &IndexMetadata{
			Name: "name222",
		}

		require.NoError(t, c.Update(ctx, tx, 1, 1, 1, "name3", updatedPayload))
		index, err = c.Get(ctx, tx, 1, 1, 1, "name3")
		require.NoError(t, err)
		require.Equal(t, updatedPayload, index)
	})

	t.Run("put_get_delete_get", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, c.insert(ctx, tx, 1, 1, 1, "name5", testIndexMetadata))
		index, err := c.Get(ctx, tx, 1, 1, 1, "name5")
		require.NoError(t, err)
		require.Equal(t, testIndexMetadata, index)

		require.NoError(t, c.delete(ctx, tx, 1, 1, 1, "name5"))
		_, err = c.Get(ctx, tx, 1, 1, 1, "name5")
		require.Error(t, errors.ErrNotFound, err)
	})

	t.Run("list", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		require.NoError(t, c.insert(ctx, tx, 1, 1, 1, "name8", testIndexMetadata))
		require.NoError(t, c.insert(ctx, tx, 1, 1, 1, "name9", testIndexMetadata))

		colls, err := c.list(ctx, tx, 1, 1, 1)
		require.NoError(t, err)

		require.Equal(t, map[string]*IndexMetadata{
			"name8": {},
			"name9": {},
		}, colls)
	})
}

func TestIndexSubspaceNegative(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, tm := initIndexTest(t, ctx)

	t.Run("invalid_short_key_len", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		shortKey := keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(1), UInt32ToByte(1), UInt32ToByte(1), indexKey, keyEnd)

		err := c.insertMetadata(ctx, tx, nil, shortKey, indexMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = c.list(ctx, tx, 1, 1, 1)
		require.Equal(t, errors.Internal("not a valid key %v", shortKey.IndexParts()), err) //nolint:asasalint
	})

	t.Run("invalid_long_key_len", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		longKey := keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(1), UInt32ToByte(1), UInt32ToByte(1), indexKey, "name8", "name8", keyEnd)

		err := c.insertMetadata(ctx, tx, nil, longKey, indexMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = c.list(ctx, tx, 1, 1, 1)
		require.Equal(t, errors.Internal("not a valid key %v", longKey.IndexParts()), err) //nolint:asasalint
	})

	t.Run("invalid_suffix_key", func(t *testing.T) {
		tx, cleanupTx := initTx(t, ctx, tm)
		defer cleanupTx()

		key := keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(1), UInt32ToByte(1), UInt32ToByte(1), indexKey, "name8", "some_suffix")

		err := c.insertMetadata(ctx, tx, nil, key, indexMetaValueVersion, testCollectionMetadata)
		require.NoError(t, err)

		_, err = c.list(ctx, tx, 1, 1, 1)
		require.Equal(t, errors.Internal("key trailer is missing %v", key.IndexParts()), err) //nolint:asasalint
	})
}

func TestIndexSubspaceMigrationV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, tm := initIndexTest(t, ctx)
	defer func() {
		_ = kvStore.DropTable(ctx, c.SubspaceName)
	}()

	tx, cleanupTx := initTx(t, ctx, tm)
	defer cleanupTx()

	// Create index with ID 1 in legacy v0 format
	key := c.getKey(1, 1, 1, "name7")
	err := c.insertPayload(ctx, tx, nil, key, 0, UInt32ToByte(123))
	require.NoError(t, err)

	// New code is able to read legacy version
	meta, err := c.Get(ctx, tx, 1, 1, 1, "name7")
	require.NoError(t, err)
	require.Equal(t, &IndexMetadata{ID: 123}, meta)

	updatedMetadata := &IndexMetadata{
		ID:   123,
		Name: "name333",
	}

	// Updating should overwrite with new format
	require.NoError(t, c.Update(ctx, tx, 1, 1, 1, "name7", updatedMetadata))

	// We are able to read in new format
	meta, err = c.Get(ctx, tx, 1, 1, 1, "name7")
	require.NoError(t, err)
	require.Equal(t, &IndexMetadata{ID: 123, Name: "name333"}, meta)
}
