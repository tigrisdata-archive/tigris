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

package encoding

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc/codes"
)

func TestDictionaryEncoding(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	k := NewDictionaryEncoder(&TestMDNameRegistry{
		ReserveSB:  "test_reserved",
		EncodingSB: "test_encoding",
	})
	_ = kv.DropTable(ctx, k.EncodingSubspaceName())
	_ = kv.DropTable(ctx, k.ReservedSubspaceName())

	tm := transaction.NewManager(kv)

	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", 1234))
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1234)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	collId, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1234, 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	indexId, err := k.EncodeIndexName(ctx, tx, "pkey", 1234, 1, 2)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	v, err := k.GetDatabaseId(ctx, tx, "db-1", 1234)
	require.NoError(t, err)
	require.Equal(t, v, dbId)

	v, err = k.GetCollectionId(ctx, tx, "coll-1", 1234, dbId)
	require.NoError(t, err)
	require.Equal(t, v, collId)

	v, err = k.GetIndexId(ctx, tx, "pkey", 1234, dbId, collId)
	require.NoError(t, err)
	require.Equal(t, v, indexId)
	require.NoError(t, tx.Commit(ctx))

	// try assigning the same namespace id to some other namespace
	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	require.Error(t, k.ReserveNamespace(ctx, tx, "proj2-org-1", 1234))
	require.NoError(t, tx.Rollback(ctx))
}

func TestDictionaryEncodingDropped(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("drop_database", func(t *testing.T) {
		k := NewDictionaryEncoder(&TestMDNameRegistry{
			ReserveSB:  "test_reserved",
			EncodingSB: "test_encoding",
		})

		_ = kv.DropTable(ctx, k.EncodingSubspaceName())
		_ = kv.DropTable(ctx, k.ReservedSubspaceName())

		tm := transaction.NewManager(kv)

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", 1234))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1234)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err := k.GetDatabaseId(ctx, tx, "db-1", 1234)
		require.NoError(t, err)
		require.Equal(t, v, dbId)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = k.EncodeDatabaseAsDropped(ctx, tx, "db-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err = k.GetDatabaseId(ctx, tx, "db-1", 1234)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v, InvalidId)
	})
	t.Run("drop_collection", func(t *testing.T) {
		k := NewDictionaryEncoder(&TestMDNameRegistry{
			ReserveSB:  "test_reserved",
			EncodingSB: "test_encoding",
		})

		_ = kv.DropTable(ctx, k.EncodingSubspaceName())
		_ = kv.DropTable(ctx, k.ReservedSubspaceName())

		tm := transaction.NewManager(kv)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", 1234))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1234)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		collId, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err := k.GetCollectionId(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v, collId)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = k.EncodeCollectionAsDropped(ctx, tx, "coll-1", 1234, dbId, collId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err = k.GetCollectionId(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v, InvalidId)
	})
	t.Run("drop_index", func(t *testing.T) {
		k := NewDictionaryEncoder(&TestMDNameRegistry{
			ReserveSB:  "test_reserved",
			EncodingSB: "test_encoding",
		})

		_ = kv.DropTable(ctx, k.EncodingSubspaceName())
		_ = kv.DropTable(ctx, k.ReservedSubspaceName())

		tm := transaction.NewManager(kv)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", 1234))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1234)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		collId, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		idxId, err := k.EncodeIndexName(ctx, tx, "idx-1", 1234, dbId, collId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err := k.GetIndexId(ctx, tx, "idx-1", 1234, dbId, collId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v, idxId)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = k.EncodeIndexAsDropped(ctx, tx, "idx-1", 1234, dbId, collId, idxId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err = k.GetIndexId(ctx, tx, "idx-1", 1234, dbId, collId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v, InvalidId)
	})
	t.Run("drop_collection_multiple", func(t *testing.T) {
		k := NewDictionaryEncoder(&TestMDNameRegistry{
			ReserveSB:  "test_reserved",
			EncodingSB: "test_encoding",
		})

		_ = kv.DropTable(ctx, k.EncodingSubspaceName())
		_ = kv.DropTable(ctx, k.ReservedSubspaceName())

		tm := transaction.NewManager(kv)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", 1234))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1234)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		collId, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err := k.GetCollectionId(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v, collId)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		err = k.EncodeCollectionAsDropped(ctx, tx, "coll-1", 1234, dbId, collId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err = k.GetCollectionId(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v, InvalidId)

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		newCollId, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		require.NoError(t, err)
		v, err = k.GetCollectionId(ctx, tx, "coll-1", 1234, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, v, newCollId)
	})
}

func TestDictionaryEncoding_Error(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	k := NewDictionaryEncoder(&TestMDNameRegistry{
		ReserveSB:  "test_reserved",
		EncodingSB: "test_encoding",
	})

	_ = kv.DropTable(ctx, k.EncodingSubspaceName())
	_ = kv.DropTable(ctx, k.ReservedSubspaceName())

	tm := transaction.NewManager(kv)
	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 0)
	require.Error(t, api.Errorf(codes.InvalidArgument, "invalid namespace id"), err)
	require.Equal(t, InvalidId, dbId)

	collId, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1234, 0)
	require.Error(t, api.Errorf(codes.InvalidArgument, "invalid database id"), err)
	require.Equal(t, InvalidId, collId)

	indexId, err := k.EncodeIndexName(ctx, tx, "pkey", 1234, 1, 0)
	require.Error(t, api.Errorf(codes.InvalidArgument, "invalid collection id"), err)
	require.Equal(t, InvalidId, indexId)
	require.NoError(t, tx.Rollback(context.TODO()))
}

func TestDictionaryEncoding_GetMethods(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tm := transaction.NewManager(kv)
	t.Run("get_databases", func(t *testing.T) {
		k := NewDictionaryEncoder(&TestMDNameRegistry{
			ReserveSB:  "test_reserved",
			EncodingSB: "test_encoding",
		})

		_ = kv.DropTable(ctx, k.EncodingSubspaceName())
		_ = kv.DropTable(ctx, k.ReservedSubspaceName())

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		dbId1, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1)
		require.NoError(t, err)
		dbId2, err := k.EncodeDatabaseName(ctx, tx, "db-2", 1)
		require.NoError(t, err)

		dbToId, err := k.GetDatabases(ctx, tx, 1)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Len(t, dbToId, 2)
		require.Equal(t, dbToId["db-1"], dbId1)
		require.Equal(t, dbToId["db-2"], dbId2)
	})
	t.Run("get_collections", func(t *testing.T) {
		k := NewDictionaryEncoder(&TestMDNameRegistry{
			ReserveSB:  "test_reserved",
			EncodingSB: "test_encoding",
		})

		_ = kv.DropTable(ctx, k.EncodingSubspaceName())
		_ = kv.DropTable(ctx, k.ReservedSubspaceName())

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1)
		require.NoError(t, err)

		cid1, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1, dbId)
		require.NoError(t, err)
		cid2, err := k.EncodeCollectionName(ctx, tx, "coll-2", 1, dbId)
		require.NoError(t, err)

		collToId, err := k.GetCollections(ctx, tx, 1, dbId)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Len(t, collToId, 2)
		require.Equal(t, collToId["coll-1"], cid1)
		require.Equal(t, collToId["coll-2"], cid2)
	})
	t.Run("get_indexes", func(t *testing.T) {
		k := NewDictionaryEncoder(&TestMDNameRegistry{
			ReserveSB:  "test_reserved",
			EncodingSB: "test_encoding",
		})

		_ = kv.DropTable(ctx, k.EncodingSubspaceName())
		_ = kv.DropTable(ctx, k.ReservedSubspaceName())

		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1)
		require.NoError(t, err)

		cid1, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1, dbId)
		require.NoError(t, err)

		pkid, err := k.EncodeIndexName(ctx, tx, "pkey", 1, dbId, cid1)
		require.NoError(t, err)
		idxToId, err := k.GetIndexes(ctx, tx, 1, dbId, cid1)
		require.NoError(t, err)
		require.Len(t, idxToId, 1)
		require.Equal(t, idxToId["pkey"], pkid)
		require.NoError(t, tx.Commit(ctx))
	})
}

func TestReservedNamespace(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r := newReservedSubspace(&TestMDNameRegistry{
		ReserveSB:  "test_reserved",
		EncodingSB: "test_encoding",
	})

	_ = kv.DropTable(ctx, r.EncodingSubspaceName())
	_ = kv.DropTable(ctx, r.ReservedSubspaceName())

	tm := transaction.NewManager(kv)

	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, r.reserveNamespace(ctx, tx, "p1-o1", 123))
	require.NoError(t, tx.Commit(ctx))

	// check in the allocated id is assigned
	tx, err = tm.StartTx(ctx)
	require.NoError(t, err)
	require.NoError(t, r.reload(ctx, tx))
	require.Equal(t, "p1-o1", r.allocated[123])

	// try assigning the same namespace id to some other namespace
	tx, err = tm.StartTx(context.TODO())
	require.NoError(t, err)
	expError := api.Errorf(codes.AlreadyExists, "id is already assigned to the namespace 'p1-o1'")
	require.Equal(t, expError, r.reserveNamespace(context.TODO(), tx, "p2-o2", 123))
}

func TestDecode(t *testing.T) {
	k := kv.BuildKey(encVersion, UInt32ToByte(1234), dbKey, "db-1", keyEnd)
	mp, err := NewDictionaryEncoder(&TestMDNameRegistry{
		ReserveSB:  "test_reserved",
		EncodingSB: "test_encoding",
	}).decode(context.TODO(), k)
	require.NoError(t, err)
	require.Equal(t, mp[dbKey], "db-1")
}

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled"})
	os.Exit(m.Run())
}
