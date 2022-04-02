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
const (
	namespaceKey  = "namespace"
	dbKey         = "db"
	collectionKey = "coll"
	counterKey    = "counter"
	indexKey      = "index"
	keyEnd        = "created"
)

// subspaces - only reason of not declaring these as consts because tests in different packages can overwrite this.
var (
	ReservedSubspaceKey = "reserved"
	EncodingSubspaceKey = "encoding"
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

	allocated     map[uint32]string
	namespaceToId map[string]uint32
}

func newReservedSubspace() *reservedSubspace {
	return &reservedSubspace{
		allocated:     make(map[uint32]string),
		namespaceToId: make(map[string]uint32),
	}
}

func (r *reservedSubspace) getNamespaces() map[string]uint32 {
	return r.namespaceToId
}

func (r *reservedSubspace) reload(ctx context.Context, tx transaction.Tx) error {
	r.Lock()
	defer r.Unlock()

	key := keys.NewKey(ReservedSubspaceKey)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return err
	}

	var row kv.KeyValue
	for it.Next(&row) {
		if len(row.Key) < 3 {
			return api.Errorf(codes.Internal, "not a valid key %v", row.Key)
		}

		allocatedTo := row.Key[len(row.Key)-2]
		if _, ok := allocatedTo.(string); !ok {
			return api.Errorf(codes.Internal, "unable to deduce the encoded key from fdb key %T", allocatedTo)
		}

		r.allocated[ByteToUInt32(row.Value)] = allocatedTo.(string)
		r.namespaceToId[allocatedTo.(string)] = ByteToUInt32(row.Value)
	}

	return it.Err()
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
	log.Debug().Uint32("namespace-id", id).Str("namespace", r.allocated[id]).Msg("reserved for namespace")
	if allocatedTo, ok := r.allocated[id]; ok {
		if allocatedTo == namespace {
			return nil
		} else {
			return api.Errorf(codes.AlreadyExists, "id is already assigned to the namespace '%s'", allocatedTo)
		}
	}

	key := keys.NewKey(ReservedSubspaceKey, namespaceKey, namespace, keyEnd)
	// now do an insert to fail if namespace already exists.
	if err := tx.Insert(ctx, key, UInt32ToByte(id)); err != nil {
		log.Debug().Interface("key", key).Uint32("value", id).Err(err).Msg("reserving namespace failed")
		return err
	}

	log.Debug().Interface("key", key).Uint32("value", id).Msg("reserving namespace succeed")
	return nil
}

func (r *reservedSubspace) allocateToken(ctx context.Context, tx transaction.Tx, keyName string) (uint32, error) {
	key := keys.NewKey(ReservedSubspaceKey, keyName, counterKey, keyEnd)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return 0, err
	}

	newReservedValue := reservedBaseValue
	var row kv.KeyValue
	if it.Next(&row) {
		newReservedValue = ByteToUInt32(row.Value) + 1
	}

	if err := it.Err(); err != nil {
		return 0, err
	}

	if err := tx.Replace(ctx, key, UInt32ToByte(newReservedValue)); err != nil {
		return 0, err
	}

	log.Debug().Interface("key", key).Uint32("value", newReservedValue).Msg("reserved new value")

	return newReservedValue, nil
}

// DictionaryEncoder is used to replace variable length strings to their corresponding codes to encode it. Compression
// is achieved by replacing long strings with a simple 4byte representation.
type DictionaryEncoder struct {
	reservedSb *reservedSubspace
}

func NewDictionaryEncoder() *DictionaryEncoder {
	return &DictionaryEncoder{
		reservedSb: newReservedSubspace(),
	}
}

// ReserveNamespace is the first step in the encoding and the mapping is passed the caller. As this is the first encoded
// integer the caller needs to make sure a unique value is assigned to this namespace.
func (k *DictionaryEncoder) ReserveNamespace(ctx context.Context, tx transaction.Tx, namespace string, id uint32) error {
	return k.reservedSb.reserveNamespace(ctx, tx, namespace, id)
}

func (k *DictionaryEncoder) GetNamespaces(ctx context.Context, tx transaction.Tx) (map[string]uint32, error) {
	if err := k.reservedSb.reload(ctx, tx); err != nil {
		return nil, err
	}

	return k.reservedSb.getNamespaces(), nil
}

func (k *DictionaryEncoder) EncodeDatabaseName(ctx context.Context, tx transaction.Tx, dbName string, namespaceId uint32) (uint32, error) {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return invalidId, err
	}
	if len(dbName) == 0 {
		return invalidId, api.Errorf(codes.InvalidArgument, "database name is empty")
	}

	key := keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), dbKey, dbName, keyEnd)
	return k.encode(ctx, tx, key, dbKey)
}

func (k *DictionaryEncoder) EncodeCollectionName(ctx context.Context, tx transaction.Tx, collection string, namespaceId uint32, dbId uint32) (uint32, error) {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return invalidId, err
	}
	if err := k.validDatabaseId(dbId); err != nil {
		return invalidId, err
	}
	if len(collection) == 0 {
		return invalidId, api.Errorf(codes.InvalidArgument, "collection name is empty")
	}

	key := keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), collectionKey, collection, keyEnd)
	return k.encode(ctx, tx, key, collectionKey)
}

func (k *DictionaryEncoder) EncodeIndexName(ctx context.Context, tx transaction.Tx, indexName string, namespaceId uint32, dbId uint32, collId uint32) (uint32, error) {
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

	key := keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), indexKey, indexName, keyEnd)
	return k.encode(ctx, tx, key, indexKey)
}

func (k *DictionaryEncoder) encode(ctx context.Context, tx transaction.Tx, key keys.Key, encName string) (uint32, error) {
	reserveToken, err := k.reservedSb.allocateToken(ctx, tx, EncodingSubspaceKey)
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

func (k *DictionaryEncoder) validNamespaceId(id uint32) error {
	if id == invalidId {
		return api.Errorf(codes.InvalidArgument, "invalid namespace id")
	}
	return nil
}

func (k *DictionaryEncoder) validDatabaseId(id uint32) error {
	if id == invalidId {
		return api.Errorf(codes.InvalidArgument, "invalid database id")
	}
	return nil
}

func (k *DictionaryEncoder) validCollectionId(id uint32) error {
	if id == invalidId {
		return api.Errorf(codes.InvalidArgument, "invalid collection id")
	}
	return nil
}

func (k *DictionaryEncoder) GetDatabases(ctx context.Context, tx transaction.Tx, namespaceId uint32) (map[string]uint32, error) {
	databases := make(map[string]uint32)
	it, err := tx.Read(ctx, keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), dbKey))
	if err != nil {
		return nil, err
	}

	var v kv.KeyValue
	for it.Next(&v) {
		if len(v.Key) < 5 {
			return nil, api.Errorf(codes.Internal, "not a valid key %v", v.Key)
		}
		if len(v.Key) == 5 {
			// format <version,namespace-id,db,dbName,keyEnd>
			end, ok := v.Key[4].(string)
			if !ok || end != keyEnd {
				return nil, api.Errorf(codes.Internal, "database encoding is missing %v", v.Key)
			}
			name, ok := v.Key[3].(string)
			if !ok {
				return nil, api.Errorf(codes.Internal, "database name not found %T %v", v.Key[3], v.Key[3])
			}

			databases[name] = ByteToUInt32(v.Value)
		}
	}

	return databases, it.Err()
}

func (k *DictionaryEncoder) GetCollections(ctx context.Context, tx transaction.Tx, namespaceId uint32, databaseId uint32) (map[string]uint32, error) {
	var collections = make(map[string]uint32)
	it, err := tx.Read(ctx, keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(databaseId)))
	if err != nil {
		return nil, err
	}

	var v kv.KeyValue
	for it.Next(&v) {
		if len(v.Key) < 6 {
			return nil, api.Errorf(codes.Internal, "not a valid key %v", v.Key)
		}

		if len(v.Key) == 6 {
			// format <version,namespace-id,db-id,coll,coll-name,keyEnd>
			end, ok := v.Key[5].(string)
			if !ok || end != keyEnd {
				return nil, api.Errorf(codes.Internal, "collection encoding is missing %v", v.Key)
			}

			name, ok := v.Key[4].(string)
			if !ok {
				return nil, api.Errorf(codes.Internal, "collection name not found %T %v", v.Key[4], v.Key[4])
			}

			collections[name] = ByteToUInt32(v.Value)
		}
	}

	return collections, it.Err()
}

func (k *DictionaryEncoder) GetIndexes(ctx context.Context, tx transaction.Tx, namespaceId uint32, databaseId uint32, collId uint32) (map[string]uint32, error) {
	var indexes = make(map[string]uint32)
	it, err := tx.Read(ctx, keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(databaseId), UInt32ToByte(collId)))
	if err != nil {
		return nil, err
	}

	var v kv.KeyValue
	for it.Next(&v) {
		if len(v.Key) < 6 {
			return nil, api.Errorf(codes.Internal, "not a valid key %v", v.Key)
		}
		if len(v.Key) == 7 {
			// if it is the format <version,namespace-id,db-id,coll-id,indexName,index-name,keyEnd>
			end, ok := v.Key[6].(string)
			if !ok || end != keyEnd {
				// if it is not index skip it
				continue
			}

			name, ok := v.Key[5].(string)
			if !ok {
				return nil, api.Errorf(codes.Internal, "index name not found %T %v", v.Key[5], v.Key[5])
			}

			indexes[name] = ByteToUInt32(v.Value)
		}
	}

	return indexes, it.Err()
}

func (k *DictionaryEncoder) GetDatabaseId(ctx context.Context, tx transaction.Tx, dbName string, namespaceId uint32) (uint32, error) {
	key := keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), dbKey, dbName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *DictionaryEncoder) GetCollectionId(ctx context.Context, tx transaction.Tx, collName string, namespaceId uint32, dbId uint32) (uint32, error) {
	key := keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), collectionKey, collName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *DictionaryEncoder) GetIndexId(ctx context.Context, tx transaction.Tx, indexName string, namespaceId uint32, dbId uint32, collId uint32) (uint32, error) {
	key := keys.NewKey(EncodingSubspaceKey, encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), indexKey, indexName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *DictionaryEncoder) getId(ctx context.Context, tx transaction.Tx, key keys.Key) (uint32, error) {
	it, err := tx.Read(ctx, key)
	if err != nil {
		return invalidId, err
	}

	var row kv.KeyValue
	if it.Next(&row) {
		return ByteToUInt32(row.Value), nil
	}

	if err := it.Err(); err != nil {
		return 0, err
	}

	return invalidId, api.Errorf(codes.NotFound, "not found %v", key)
}

// decode is currently only use for debugging purpose, once we have a layer on top of this encoding then we leverage this
// method
func (k *DictionaryEncoder) decode(_ context.Context, fdbKey kv.Key) (map[string]interface{}, error) {
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
