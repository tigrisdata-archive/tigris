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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"google.golang.org/grpc/codes"
)

func TestDictionaryEncoding(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = kv.DropTable(ctx, encodingSubspaceKey)
	_ = kv.DropTable(ctx, reservedSubspaceKey)

	tm := transaction.NewManager(kv)
	k := NewDictionaryEncoder()

	tx, err := tm.StartTxWithoutTracking(ctx)
	require.NoError(t, err)
	require.NoError(t, k.ReserveNamespace(ctx, tx, "proj1-org-1", 1234))
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTxWithoutTracking(ctx)
	require.NoError(t, err)
	dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 1234)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTxWithoutTracking(ctx)
	require.NoError(t, err)
	collId, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1234, 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTxWithoutTracking(ctx)
	require.NoError(t, err)
	indexId, err := k.EncodeIndexName(ctx, tx, "pkey", 1234, 1, 2)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx, err = tm.StartTxWithoutTracking(ctx)
	require.NoError(t, err)
	v, err := k.getDatabaseId(ctx, tx, "db-1", 1234)
	require.NoError(t, err)
	require.Equal(t, v, dbId)

	v, err = k.getCollectionId(ctx, tx, "coll-1", 1234, dbId)
	require.NoError(t, err)
	require.Equal(t, v, collId)

	v, err = k.getIndexId(ctx, tx, "pkey", 1234, dbId, collId)
	require.NoError(t, err)
	require.Equal(t, v, indexId)

	// try assigning the same namespace id to some other namespace
	tx, err = tm.StartTxWithoutTracking(context.TODO())
	require.NoError(t, err)
	require.Error(t, k.ReserveNamespace(context.TODO(), tx, "proj2-org-1", 1234))
}

func TestDictionaryEncoding_Error(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = kv.DropTable(ctx, encodingSubspaceKey)
	_ = kv.DropTable(ctx, reservedSubspaceKey)

	tm := transaction.NewManager(kv)
	k := NewDictionaryEncoder()

	tx, err := tm.StartTxWithoutTracking(ctx)
	require.NoError(t, err)
	dbId, err := k.EncodeDatabaseName(ctx, tx, "db-1", 0)
	require.Error(t, api.Errorf(codes.InvalidArgument, "invalid namespace id"), err)
	require.Equal(t, invalidId, dbId)

	collId, err := k.EncodeCollectionName(ctx, tx, "coll-1", 1234, 0)
	require.Error(t, api.Errorf(codes.InvalidArgument, "invalid database id"), err)
	require.Equal(t, invalidId, collId)

	indexId, err := k.EncodeIndexName(ctx, tx, "pkey", 1234, 1, 0)
	require.Error(t, api.Errorf(codes.InvalidArgument, "invalid collection id"), err)
	require.Equal(t, invalidId, indexId)
	require.NoError(t, tx.Rollback(context.TODO()))
}

func TestReservedNamespace(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = kv.DropTable(ctx, encodingSubspaceKey)
	_ = kv.DropTable(ctx, reservedSubspaceKey)

	tm := transaction.NewManager(kv)
	r := newReservedSubspace()

	tx, err := tm.StartTxWithoutTracking(ctx)
	require.NoError(t, err)
	require.NoError(t, r.reserveNamespace(ctx, tx, "p1-o1", 123))
	require.NoError(t, tx.Commit(ctx))

	// check in the allocated id is assigned
	tx, err = tm.StartTxWithoutTracking(ctx)
	require.NoError(t, err)
	require.NoError(t, r.reload(ctx, tx))
	require.Equal(t, "p1-o1", r.allocated[123])

	// try assigning the same namespace id to some other namespace
	tx, err = tm.StartTxWithoutTracking(context.TODO())
	require.NoError(t, err)
	expError := api.Errorf(codes.AlreadyExists, "id is already assigned to the namespace 'p1-o1'")
	require.Equal(t, expError, r.reserveNamespace(context.TODO(), tx, "p2-o2", 123))
}

func TestDecode(t *testing.T) {
	k := kv.BuildKey(encVersion, UInt32ToByte(1234), dbKey, "db-1", keyEnd)
	mp, err := NewDictionaryEncoder().decode(context.TODO(), k)
	require.NoError(t, err)
	require.Equal(t, mp[dbKey], "db-1")
}
