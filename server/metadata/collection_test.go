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
	"github.com/tigrisdata/tigris/server/transaction"
)

var testCollectionPayload = &CollectionMetadata{
	OldestSchemaVersion: 10,
}

func initCollectionTest(t *testing.T) (*CollectionSubspace, transaction.Tx) {
	c := NewCollectionStore(&NameRegistry{
		CollectionSB: "test_collection",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = kvStore.DropTable(ctx, c.SubspaceName)

	tm := transaction.NewManager(kvStore)
	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)

	return c, tx
}

func TestCollectionSubspace(t *testing.T) {
	t.Run("put_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx := initCollectionTest(t)
		defer func() { assert.NoError(t, tx.Rollback(ctx)) }()

		require.Equal(t, errors.InvalidArgument("invalid id"), u.Insert(ctx, tx, 0, 1, 1, testCollectionPayload))
		require.Equal(t, errors.InvalidArgument("invalid id"), u.Insert(ctx, tx, 1, 0, 1, testCollectionPayload))
		require.Equal(t, errors.InvalidArgument("invalid id"), u.Insert(ctx, tx, 1, 1, 0, testCollectionPayload))
		require.Equal(t, errors.InvalidArgument("invalid nil payload"), u.Insert(ctx, tx, 1, 1, 1, nil))

		_ = kvStore.DropTable(ctx, u.SubspaceName)
	})

	t.Run("put_get_1", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx := initCollectionTest(t)
		defer func() { assert.NoError(t, tx.Rollback(ctx)) }()

		require.NoError(t, u.Insert(ctx, tx, 1, 1, 1, testCollectionPayload))
		collection, err := u.Get(ctx, tx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, testCollectionPayload, collection)

		_ = kvStore.DropTable(ctx, u.SubspaceName)
	})

	t.Run("put_get_2", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx := initCollectionTest(t)
		defer func() { assert.NoError(t, tx.Rollback(ctx)) }()

		appPayload := &CollectionMetadata{
			OldestSchemaVersion: 20,
		}

		require.NoError(t, u.Insert(ctx, tx, 1, 1, 1, appPayload))
		collection, err := u.Get(ctx, tx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, appPayload, collection)

		_ = kvStore.DropTable(ctx, u.SubspaceName)
	})

	t.Run("put_get_update_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx := initCollectionTest(t)
		defer func() { assert.NoError(t, tx.Rollback(ctx)) }()

		require.NoError(t, u.Insert(ctx, tx, 1, 1, 1, testCollectionPayload))
		collection, err := u.Get(ctx, tx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, testCollectionPayload, collection)

		updatedCollectionPayload := &CollectionMetadata{
			OldestSchemaVersion: 30,
		}

		require.NoError(t, u.Update(ctx, tx, 1, 1, 1, updatedCollectionPayload))
		collection, err = u.Get(ctx, tx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, updatedCollectionPayload, collection)

		_ = kvStore.DropTable(ctx, u.SubspaceName)
	})

	t.Run("put_get_delete_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx := initCollectionTest(t)
		defer func() { assert.NoError(t, tx.Rollback(ctx)) }()

		require.NoError(t, u.Insert(ctx, tx, 1, 1, 1, testCollectionPayload))
		collection, err := u.Get(ctx, tx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, testCollectionPayload, collection)

		require.NoError(t, u.Delete(ctx, tx, 1, 1, 1))
		collection, err = u.Get(ctx, tx, 1, 1, 1)
		require.NoError(t, err)
		require.Nil(t, collection)

		_ = kvStore.DropTable(ctx, u.SubspaceName)
	})
}
