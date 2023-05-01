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
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

func testClearDictionary(ctx context.Context, k *Dictionary, kvStore kv.TxStore) {
	_ = kvStore.DropTable(ctx, k.EncodingSubspaceName())
	_ = kvStore.DropTable(ctx, k.ReservedSubspaceName())
	_ = kvStore.DropTable(ctx, k.NamespaceSubspaceName())
}

func TestDictionaryEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nr := newTestNameRegistry(t)
	k := NewMetadataDictionary(nr)

	startId := nr.BaseCounterValue

	testClearDictionary(ctx, k, kvStore)

	tm := transaction.NewManager(kvStore)

	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", NewNamespaceMetadata(1234, "proj1-org-1", "proj1-org-1-display_name")))
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	dbMeta, err := k.CreateDatabase(ctx, tx, "db-1", 1234, 0)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	collMeta, err := k.CreateCollection(ctx, tx, "coll-1", 1234, startId, createSecondaryIdxs())
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	indexMeta, err := k.CreatePrimaryIndex(ctx, tx, "pkey", 1234, startId, startId+1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	d, err := k.GetDatabase(ctx, tx, "db-1", 1234)
	require.NoError(t, err)
	require.Equal(t, d.ID, dbMeta.ID)

	c, err := k.GetCollection(ctx, tx, "coll-1", 1234, dbMeta.ID)
	require.NoError(t, err)
	require.Equal(t, c.ID, collMeta.ID)

	i, err := k.GetPrimaryIndex(ctx, tx, "pkey", 1234, dbMeta.ID, collMeta.ID)
	require.NoError(t, err)
	require.Equal(t, i.ID, indexMeta.ID)
	require.NoError(t, tx.Commit(ctx))

	// try assigning the same namespace id to some other namespace
	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	require.Error(t, k.ReserveNamespace(ctx, tx, "proj2-org-1", NewNamespaceMetadata(1234, "proj2-org-1", "proj2-org-1-display_name")))
	require.NoError(t, tx.Rollback(ctx))
}

func TestDictionaryEncodingDropped(t *testing.T) {
	t.Run("drop_database", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tm := transaction.NewManager(kvStore)

		k := NewMetadataDictionary(newTestNameRegistry(t))
		testClearDictionary(ctx, k, kvStore)

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", NewNamespaceMetadata(1234, "proj1-org-1", "proj1-org-1-display_name")))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		dbMeta, err := k.CreateDatabase(ctx, tx, "db-1", 1234, 0)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err := k.GetDatabase(ctx, tx, "db-1", 1234)
		require.NoError(t, err)
		require.Equal(t, v.ID, dbMeta.ID)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = k.DropDatabase(ctx, tx, "db-1", 1234)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		_, err = k.GetDatabase(ctx, tx, "db-1", 1234)
		require.Equal(t, errors.ErrNotFound, err)

		require.NoError(t, tx.Commit(ctx))
	})

	t.Run("drop_collection", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tm := transaction.NewManager(kvStore)

		k := NewMetadataDictionary(newTestNameRegistry(t))
		testClearDictionary(ctx, k, kvStore)

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", NewNamespaceMetadata(1234, "proj1-org-1", "proj1-org-1-display_name")))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		dbMeta, err := k.CreateDatabase(ctx, tx, "db-1", 1234, 0)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		collMeta, err := k.CreateCollection(ctx, tx, "coll-1", 1234, dbMeta.ID, createSecondaryIdxs())
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err := k.GetCollection(ctx, tx, "coll-1", 1234, dbMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v.ID, collMeta.ID)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = k.DropCollection(ctx, tx, "coll-1", 1234, dbMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		_, err = k.GetCollection(ctx, tx, "coll-1", 1234, dbMeta.ID)
		require.Error(t, errors.ErrNotFound, err)

		require.NoError(t, tx.Commit(ctx))
	})

	t.Run("drop_index", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tm := transaction.NewManager(kvStore)

		k := NewMetadataDictionary(newTestNameRegistry(t))
		testClearDictionary(ctx, k, kvStore)

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", NewNamespaceMetadata(1234, "proj1-org-1", "proj1-org-1-display_name")))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		dbMeta, err := k.CreateDatabase(ctx, tx, "db-1", 1234, 0)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.NotEqual(t, 0, dbMeta.ID)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		collMeta, err := k.CreateCollection(ctx, tx, "coll-1", 1234, dbMeta.ID, createSecondaryIdxs())
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.NotEqual(t, 0, collMeta.ID)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		idxMeta, err := k.CreatePrimaryIndex(ctx, tx, "idx-1", 1234, dbMeta.ID, collMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.NotEqual(t, 0, idxMeta.ID)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err := k.GetPrimaryIndex(ctx, tx, "idx-1", 1234, dbMeta.ID, collMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v.ID, idxMeta.ID)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = k.DropPrimaryIndex(ctx, tx, "idx-1", 1234, dbMeta.ID, collMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		_, err = k.GetPrimaryIndex(ctx, tx, "idx-1", 1234, dbMeta.ID, collMeta.ID)
		require.Equal(t, errors.ErrNotFound, err)
		require.NoError(t, tx.Commit(ctx))
	})

	t.Run("drop_collection_multiple", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tm := transaction.NewManager(kvStore)

		k := NewMetadataDictionary(newTestNameRegistry(t))
		testClearDictionary(ctx, k, kvStore)

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", NewNamespaceMetadata(1234, "proj1-org-1", "proj1-org-1-display_name")))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		dbMeta, err := k.CreateDatabase(ctx, tx, "db-1", 1234, 0)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		collMeta, err := k.CreateCollection(ctx, tx, "coll-1", 1234, dbMeta.ID, createSecondaryIdxs())
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err := k.GetCollection(ctx, tx, "coll-1", 1234, dbMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v.ID, collMeta.ID)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = k.DropCollection(ctx, tx, "coll-1", 1234, dbMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		_, err = k.GetCollection(ctx, tx, "coll-1", 1234, dbMeta.ID)
		require.Equal(t, errors.ErrNotFound, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		newColl, err := k.CreateCollection(ctx, tx, "coll-1", 1234, dbMeta.ID, createSecondaryIdxs())
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err = k.GetCollection(ctx, tx, "coll-1", 1234, dbMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v.ID, newColl.ID)
	})
}

func TestDictionaryEncoding_Error(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	k := NewMetadataDictionary(newTestNameRegistry(t))
	testClearDictionary(ctx, k, kvStore)

	tm := transaction.NewManager(kvStore)

	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)

	_, err = k.CreateDatabase(ctx, tx, "db-1", 0, 0)
	require.Error(t, errors.InvalidArgument("invalid namespace id"), err)

	_, err = k.CreateCollection(ctx, tx, "coll-1", 1234, 0, createSecondaryIdxs())
	require.Error(t, errors.InvalidArgument("invalid database id"), err)

	_, err = k.CreatePrimaryIndex(ctx, tx, "pkey", 1234, 1, 0)
	require.Error(t, errors.InvalidArgument("invalid collection id"), err)

	require.NoError(t, tx.Rollback(context.TODO()))
}

func TestDictionaryEncoding_GetMethods(t *testing.T) {
	tm := transaction.NewManager(kvStore)

	t.Run("get_databases", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		k := NewMetadataDictionary(newTestNameRegistry(t))
		testClearDictionary(ctx, k, kvStore)

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		dbMeta1, err := k.CreateDatabase(ctx, tx, "db-1", 1, 0)
		require.NoError(t, err)
		dbMeta2, err := k.CreateDatabase(ctx, tx, "db-2", 1, 0)
		require.NoError(t, err)

		dbToId, err := k.GetDatabases(ctx, tx, 1)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Len(t, dbToId, 2)
		require.Equal(t, dbToId["db-1"].ID, dbMeta1.ID)
		require.Equal(t, dbToId["db-2"].ID, dbMeta2.ID)
	})

	t.Run("get_collections", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		k := NewMetadataDictionary(newTestNameRegistry(t))
		testClearDictionary(ctx, k, kvStore)

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		dbMeta, err := k.CreateDatabase(ctx, tx, "db-1", 1, 0)
		require.NoError(t, err)

		cid1, err := k.CreateCollection(ctx, tx, "coll-1", 1, dbMeta.ID, createSecondaryIdxs())
		require.NoError(t, err)
		cid2, err := k.CreateCollection(ctx, tx, "coll-2", 1, dbMeta.ID, createSecondaryIdxs())
		require.NoError(t, err)

		collToId, err := k.GetCollections(ctx, tx, 1, dbMeta.ID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Len(t, collToId, 2)
		require.Equal(t, collToId["coll-1"], cid1)
		require.Equal(t, collToId["coll-2"], cid2)
	})

	t.Run("get_indexes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		k := NewMetadataDictionary(newTestNameRegistry(t))
		testClearDictionary(ctx, k, kvStore)

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		dbMeta, err := k.CreateDatabase(ctx, tx, "db-1", 1, 0)
		require.NoError(t, err)

		cid1, err := k.CreateCollection(ctx, tx, "coll-1", 1, dbMeta.ID, createSecondaryIdxs())
		require.NoError(t, err)

		pkID, err := k.CreatePrimaryIndex(ctx, tx, "pkey", 1, dbMeta.ID, cid1.ID)
		require.NoError(t, err)
		idxToId, err := k.GetPrimaryIndexes(ctx, tx, 1, dbMeta.ID, cid1.ID)
		require.NoError(t, err)
		require.Len(t, idxToId, 1)
		require.Equal(t, idxToId["pkey"], pkID)
		require.NoError(t, tx.Commit(ctx))
	})
}

func TestReservedNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r := newReservedSubspace(newTestNameRegistry(t))

	_ = kvStore.DropTable(ctx, r.EncodingSubspaceName())
	_ = kvStore.DropTable(ctx, r.ReservedSubspaceName())
	_ = kvStore.DropTable(ctx, r.NamespaceSubspaceName())

	tm := transaction.NewManager(kvStore)

	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, r.reserveNamespace(ctx, tx, "p1-o1", NewNamespaceMetadata(123, "p1-o1", "p1-o1-display_name")))
	require.NoError(t, tx.Commit(ctx))

	// check in the allocated id is assigned
	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, r.reload(ctx, tx))
	require.Equal(t, "p1-o1", r.idToNamespaceStruct[123].StrId)
	require.NoError(t, tx.Commit(ctx))

	// try assigning the same namespace id to some other namespace
	tx, err = tm.StartTx(context.TODO())
	require.NoError(t, err)
	expError := errors.AlreadyExists("id is already assigned to the namespace 'p1-o1'")
	require.Equal(t, expError, r.reserveNamespace(context.TODO(), tx, "p2-o2", NewNamespaceMetadata(123, "p2-o2", "p2-o2-display_name")))
	require.NoError(t, tx.Rollback(ctx))
}

func TestUpdateNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	r := newReservedSubspace(newTestNameRegistry(t))

	_ = kvStore.DropTable(ctx, r.EncodingSubspaceName())
	_ = kvStore.DropTable(ctx, r.ReservedSubspaceName())
	_ = kvStore.DropTable(ctx, r.NamespaceSubspaceName())

	tm := transaction.NewManager(kvStore)

	meta := NewNamespaceMetadata(123, "p1-o1", "p1-o1-display_name")
	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, r.reserveNamespace(ctx, tx, meta.StrId, meta))
	require.NoError(t, tx.Commit(ctx))

	// check in the allocated id is assigned
	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, r.reload(ctx, tx))
	require.Equal(t, meta, r.idToNamespaceStruct[meta.Id])
	require.NoError(t, tx.Commit(ctx))

	// update the namespace metadata
	tx, err = tm.StartTx(context.TODO())
	require.NoError(t, err)
	update := NewNamespaceMetadata(123, "p1-o1", "p1-o1-display_name")
	update.Accounts.AddMetronome("met_123")
	err = r.updateNamespace(context.TODO(), tx, update)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// validate
	require.NoError(t, err)
	require.Equal(t, update, r.strIdToNamespaceStruct[meta.StrId])
	require.Equal(t, update, r.idToNamespaceStruct[meta.Id])
}

func TestDecode(t *testing.T) {
	k := kv.BuildKey(encKeyVersion, UInt32ToByte(1234), dbKey, "db-1", keyEnd)
	mp, err := NewMetadataDictionary(newTestNameRegistry(t)).decode(context.TODO(), k)
	require.NoError(t, err)
	require.Equal(t, mp[dbKey], "db-1")
}

func createSecondaryIdxs() []*schema.Index {
	return []*schema.Index{
		{
			Name:  "idx1",
			Id:    uint32(1),
			State: schema.UNKNOWN,
		},
		{
			Name:  "idx2",
			Id:    uint32(2),
			State: schema.UNKNOWN,
		},
		{
			Name:  "idx3",
			Id:    uint32(3),
			State: schema.UNKNOWN,
		},
	}
}
