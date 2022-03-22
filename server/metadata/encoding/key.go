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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/keys"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"google.golang.org/grpc/codes"
)

// There are three subspaces(tables) introduced by this package. The first is reserved to which is used for two things
// mainly, the first is to reserve a value against a namespace which needs to be unique and passed by the caller and the
// other use case is to maintain a counter.
//
// The reserved subspace structure looks like below,
//   [“reserved”, "namespace", "namespace1", "created"] = x
//   where,
//     "reserved", "namespace", and "created" are keywords and "namespace1" is a namespace.
//
// The second subspace is the "encoding" which is used to assign dictionary encoded values for the database, collection
// and any index names. Values assigned are monotonically incremental counter and are local to this cluster and doesn't
// need to be unique across the TigrisDB ecosystem.
//
// The structure for encoding subspace looks like below,
//    ["encoding", 0x01, x, "db", "db-1", "created"] = 0x01
//  where,
//    - encoding is the keyword for this table.
//    - 0x01 is the encoding version
//    - x is the value assigned for the namespace
//    - "db", "created" are keywords and "db-1" is the name of the database.
//    - 0x01 is the value assigned to this database.

// Based on the above, The below is an example of how operations are mapped to these values.
// Request: To Create a New Database
// The value would be derived from the counter value stored in the reserved subspace. If the counter is not yet issued
// then it will start from 0x01.
//    ["encoding", 0x01, x "db", "db-1", "created"] = 0x01
//    ["encoding", 0x01, x, "db", "db-2", "created"] = 0x02
//
// Request: To Create a New Collection
// The value would be derived from the counter value stored in the reserved table. It can't be empty as there must be
// a database already created so a value is already assigned to it.
//    ["encoding", 0x01, x, 0x01, "coll", "coll-1", "created"] = 0x03
//
// Request: To Add an Index
//    ["encoding", 0x01, x, 0x01, 0x03, "index", "pkey", "created"] = 0x04
//    ["encoding", 0x01, x, 0x01, 0x03, "index", "email_index", "created"] = 0x05
//
// Request: To drop an Index
//   ["encoding", 0x01, x, 0x01, 0x03, "index", "pkey", "dropped"] = 0x04
//
// The schemas' subspace will be storing the actual schema of the user for a collection. The schema subspace will
// look like below
//    ["schemas", 0x01, x, 0x01, 0x03, "created", 0x01] => {
//      properties: {"a": int},
//      primary_key: ["a"],
//      indexes: [
//        {name: “primary”, code: 2, columns: [“id”]},
//        {name: “email_index”, code: 3, columns: [“email”], unique:true},
//        {name: “created_at_index”, code: 4, columns: [“created_at”]},
//      ]
//    }
//
const (
	namespaceKey  = "namespace"
	dbKey         = "db"
	collectionKey = "coll"
	counterKey    = "counter"
	indexKey      = "index"
	keyEnd        = "created"
)

// subspaces
const (
	reservedSubspaceKey = "reserved"
	encodingSubspaceKey = "encoding"
)

var (
	// versions
	encVersion = []byte{0x01}

	invalidId         = uint32(0)
	reservedBaseValue = uint32(1)
)

// reservedSubspace struct is used to manage reserved subspace.
type reservedSubspace struct {
	sync.RWMutex

	allocated map[uint32]string
}

func newReservedSubspace() *reservedSubspace {
	return &reservedSubspace{
		allocated: make(map[uint32]string),
	}
}

func (r *reservedSubspace) reload(ctx context.Context, tx transaction.Tx) error {
	key := keys.NewKey(reservedSubspaceKey)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return err
	}

	for it.More() {
		row, err := it.Next()
		if err != nil {
			return err
		}

		if len(row.Key) < 3 {
			return api.Errorf(codes.Internal, "not a valid key %v", row.Key)
		}

		allocatedTo := row.Key[len(row.Key)-2]
		if _, ok := allocatedTo.(string); !ok {
			return api.Errorf(codes.Internal, "unable to deduce the encoded key from fdb key %T", allocatedTo)
		}

		r.Lock()
		r.allocated[ByteToUInt32(row.Value)] = allocatedTo.(string)
		r.Unlock()
	}

	return nil
}

func (r *reservedSubspace) reserveNamespace(ctx context.Context, tx transaction.Tx, namespace string, id uint32) error {
	if len(namespace) == 0 {
		return api.Errorf(codes.InvalidArgument, "namespace is empty")
	}

	if err := r.reload(ctx, tx); err != nil {
		return err
	}

	r.RLock()
	defer r.RUnlock()
	if allocatedTo, ok := r.allocated[id]; ok {
		if allocatedTo == namespace {
			return nil
		} else {
			return api.Errorf(codes.AlreadyExists, "id is already assigned to the namespace '%s'", allocatedTo)
		}
	}

	key := keys.NewKey(reservedSubspaceKey, namespaceKey, namespace, keyEnd)
	// now do insert because we need strict insert functionality here
	if err := tx.Insert(ctx, key, UInt32ToByte(id)); err != nil {
		log.Debug().Interface("key", key).Uint32("value", id).Err(err).Msg("reserving namespace failed")
		return err
	}

	log.Debug().Interface("key", key).Uint32("value", id).Msg("reserving namespace succeed")
	return nil
}

func (r *reservedSubspace) allocateToken(ctx context.Context, tx transaction.Tx, keyName string) (uint32, error) {
	key := keys.NewKey(reservedSubspaceKey, keyName, counterKey, keyEnd)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return 0, err
	}

	newReservedValue := reservedBaseValue
	if it.More() {
		row, err := it.Next()
		if err != nil {
			return 0, err
		}

		newReservedValue = ByteToUInt32(row.Value) + 1
	}

	if err := tx.Replace(ctx, key, UInt32ToByte(newReservedValue)); err != nil {
		return 0, err
	}

	log.Debug().Interface("key", key).Uint32("value", newReservedValue).Msg("reserved new value")

	return newReservedValue, nil
}

type KeyEncoder struct {
	reservedSb *reservedSubspace
}

func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{
		reservedSb: newReservedSubspace(),
	}
}

// ReserveNamespace is the first step in the encoding and the mapping is passed the caller. As this is the first encoded
// integer the caller needs to make sure a unique value is assigned to this namespace.
func (k *KeyEncoder) ReserveNamespace(ctx context.Context, tx transaction.Tx, namespace string, id uint32) error {
	return k.reservedSb.reserveNamespace(ctx, tx, namespace, id)
}

func (k *KeyEncoder) EncodeDatabaseName(ctx context.Context, tx transaction.Tx, dbName string, namespaceId uint32) (uint32, error) {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return invalidId, err
	}
	if len(dbName) == 0 {
		return invalidId, api.Errorf(codes.InvalidArgument, "database name is empty")
	}

	key := keys.NewKey(encodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), dbKey, dbName, keyEnd)
	return k.encode(ctx, tx, key, dbKey)
}

func (k *KeyEncoder) EncodeCollectionName(ctx context.Context, tx transaction.Tx, collection string, namespaceId uint32, dbId uint32) (uint32, error) {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return invalidId, err
	}
	if err := k.validDatabaseId(dbId); err != nil {
		return invalidId, err
	}
	if len(collection) == 0 {
		return invalidId, api.Errorf(codes.InvalidArgument, "collection name is empty")
	}

	key := keys.NewKey(encodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), collectionKey, collection, keyEnd)
	return k.encode(ctx, tx, key, collectionKey)
}

func (k *KeyEncoder) EncodeIndexName(ctx context.Context, tx transaction.Tx, indexName string, namespaceId uint32, dbId uint32, collId uint32) (uint32, error) {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return invalidId, err
	}
	if err := k.validDatabaseId(dbId); err != nil {
		return invalidId, err
	}
	if err := k.validCollectionId(collId); err != nil {
		return invalidId, err
	}
	if len(indexName) == 0 {
		return invalidId, api.Errorf(codes.InvalidArgument, "index name is empty")
	}

	key := keys.NewKey(encodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), indexKey, indexName, keyEnd)
	return k.encode(ctx, tx, key, indexKey)
}

func (k *KeyEncoder) encode(ctx context.Context, tx transaction.Tx, key keys.Key, encName string) (uint32, error) {
	reserveToken, err := k.reservedSb.allocateToken(ctx, tx, encodingSubspaceKey)
	if err != nil {
		return invalidId, err
	}

	// now do insert because we need to fail if token is already assigned
	if err := tx.Insert(ctx, key, UInt32ToByte(reserveToken)); err != nil {
		log.Debug().Interface("key", key).Uint32("value", reserveToken).Err(err).Str("type", encName).Msg("encoding failed")
		return invalidId, err
	}
	log.Debug().Interface("key", key).Uint32("value", reserveToken).Str("type", encName).Msg("encoding succeed")

	return reserveToken, nil
}

func (k *KeyEncoder) validNamespaceId(id uint32) error {
	if id == invalidId {
		return api.Errorf(codes.InvalidArgument, "invalid namespace id")
	}
	return nil
}

func (k *KeyEncoder) validDatabaseId(id uint32) error {
	if id == invalidId {
		return api.Errorf(codes.InvalidArgument, "invalid database id")
	}
	return nil
}

func (k *KeyEncoder) validCollectionId(id uint32) error {
	if id == invalidId {
		return api.Errorf(codes.InvalidArgument, "invalid collection id")
	}
	return nil
}

func (k *KeyEncoder) getDatabaseId(ctx context.Context, tx transaction.Tx, dbName string, namespaceId uint32) (uint32, error) {
	key := keys.NewKey(encodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), dbKey, dbName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *KeyEncoder) getCollectionId(ctx context.Context, tx transaction.Tx, collName string, namespaceId uint32, dbId uint32) (uint32, error) {
	key := keys.NewKey(encodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), collectionKey, collName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *KeyEncoder) getIndexId(ctx context.Context, tx transaction.Tx, indexName string, namespaceId uint32, dbId uint32, collId uint32) (uint32, error) {
	key := keys.NewKey(encodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), indexKey, indexName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *KeyEncoder) getId(ctx context.Context, tx transaction.Tx, key keys.Key) (uint32, error) {
	it, err := tx.Read(ctx, key)
	if err != nil {
		return invalidId, err
	}

	if it.More() {
		row, err := it.Next()
		if err != nil {
			return 0, err
		}

		return ByteToUInt32(row.Value), nil
	}

	return invalidId, api.Errorf(codes.NotFound, "not found %v", key)
}

// decode is currently only use for debugging purpose, once we have a layer on top of this encoding then we leverage this
// method
func (k *KeyEncoder) decode(_ context.Context, fdbKey kv.Key) (map[string]interface{}, error) {
	var decoded = make(map[string]interface{})
	if len(fdbKey) > 0 {
		decoded["version"] = fdbKey[0]
	}
	if len(fdbKey) > 1 {
		decoded[namespaceKey] = ByteToUInt32(fdbKey[1].([]byte))
	}
	if end, ok := fdbKey[len(fdbKey)-1].(string); !ok || end != keyEnd {
		return nil, fmt.Errorf("key is not from encoding subspace")
	}

	switch len(fdbKey) {
	case 5:
		// if it is the format <version,namespace-id,db,dbName,keyEnd>
		decoded[dbKey] = fdbKey[3].(string)
	case 6:
		// if it is the format <version,namespace-id,db-id,coll,coll-name,keyEnd>
		decoded[dbKey] = ByteToUInt32(fdbKey[2].([]byte))
		decoded[collectionKey] = fdbKey[4].(string)
	case 7:
		// if it is the format <version,namespace-id,db-id,coll-id,indexName,index-name,keyEnd>
		decoded[dbKey] = ByteToUInt32(fdbKey[2].([]byte))
		decoded[collectionKey] = ByteToUInt32(fdbKey[3].([]byte))
		decoded[indexKey] = fdbKey[5].(string)
	}

	return decoded, nil
}

func ByteToUInt32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func UInt32ToByte(v uint32) []byte {
	b := make([]byte, 4)

	binary.BigEndian.PutUint32(b, v)
	return b
}
