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

	"github.com/tigrisdata/tigris/internal"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
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
// need to be unique across the Tigris ecosystem.
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
	keyDroppedEnd = "dropped"
)

var (
	// versions
	encVersion = []byte{0x01}

	InvalidId         = uint32(0)
	reservedBaseValue = uint32(1)
)

// reservedSubspace struct is used to manage reserved subspace.
type reservedSubspace struct {
	sync.RWMutex
	MDNameRegistry

	allocated     map[uint32]string
	namespaceToId map[string]uint32
}

func newReservedSubspace(mdNameRegistry MDNameRegistry) *reservedSubspace {
	return &reservedSubspace{
		MDNameRegistry: mdNameRegistry,

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

	key := keys.NewKey(r.ReservedSubspaceName(), namespaceKey)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return err
	}

	var row kv.KeyValue
	for it.Next(&row) {
		if len(row.Key) < 3 {
			return api.Errorf(api.Code_INTERNAL, "not a valid key %v", row.Key)
		}

		allocatedTo := row.Key[len(row.Key)-2]
		if _, ok := allocatedTo.(string); !ok {
			return api.Errorf(api.Code_INTERNAL, "unable to deduce the encoded key from fdb key %T", allocatedTo)
		}

		allocatedValue := ByteToUInt32(row.Data.RawData)
		r.allocated[allocatedValue] = allocatedTo.(string)
		r.namespaceToId[allocatedTo.(string)] = allocatedValue
	}

	return it.Err()
}

func (r *reservedSubspace) reserveNamespace(ctx context.Context, tx transaction.Tx, namespace string, id uint32) error {
	if len(namespace) == 0 {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "namespace is empty")
	}

	if err := r.reload(ctx, tx); ulog.E(err) {
		return err
	}

	r.RLock()
	defer r.RUnlock()
	if allocatedTo, ok := r.allocated[id]; ok {
		log.Debug().Uint32("namespace_id", id).Str("namespace_name", r.allocated[id]).Msg("namespace reserved for")
		if allocatedTo == namespace {
			return nil
		} else {
			return api.Errorf(api.Code_ALREADY_EXISTS, "id is already assigned to the namespace '%s'", allocatedTo)
		}
	}

	key := keys.NewKey(r.ReservedSubspaceName(), namespaceKey, namespace, keyEnd)
	// now do an insert to fail if namespace already exists.
	if err := tx.Insert(ctx, key, internal.NewTableData(UInt32ToByte(id))); err != nil {
		log.Debug().Str("key", key.String()).Uint32("value", id).Err(err).Msg("reserving namespace failed")
		return err
	}

	log.Debug().Str("key", key.String()).Uint32("value", id).Msg("reserving namespace succeed")
	return nil
}

func (r *reservedSubspace) allocateToken(ctx context.Context, tx transaction.Tx, keyName string) (uint32, error) {
	key := keys.NewKey(r.ReservedSubspaceName(), keyName, counterKey, keyEnd)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return 0, err
	}

	newReservedValue := reservedBaseValue
	var row kv.KeyValue
	if it.Next(&row) {
		newReservedValue = ByteToUInt32(row.Data.RawData) + 1
	}

	if err := it.Err(); err != nil {
		return 0, err
	}

	if err := tx.Replace(ctx, key, internal.NewTableData(UInt32ToByte(newReservedValue))); err != nil {
		log.Debug().Str("key", key.String()).Uint32("value", newReservedValue).Msg("allocating token failed")
		return 0, err
	}

	log.Debug().Str("key", key.String()).Uint32("value", newReservedValue).Msg("allocating token succeed")

	return newReservedValue, nil
}

// DictionaryEncoder is used to replace variable length strings to their corresponding codes to encode it. Compression
// is achieved by replacing long strings with a simple 4byte representation.
type DictionaryEncoder struct {
	MDNameRegistry

	reservedSb *reservedSubspace
}

func NewDictionaryEncoder(mdNameRegistry MDNameRegistry) *DictionaryEncoder {
	return &DictionaryEncoder{
		MDNameRegistry: mdNameRegistry,
		reservedSb:     newReservedSubspace(mdNameRegistry),
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
		return InvalidId, err
	}
	if len(dbName) == 0 {
		return InvalidId, api.Errorf(api.Code_INVALID_ARGUMENT, "database name is empty")
	}

	key := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), dbKey, dbName, keyEnd)
	return k.encode(ctx, tx, key, dbKey)
}

// EncodeDatabaseAsDropped will remove the "created" entry from the encoding subspace and will add a "dropped" entry with the same
// value.
func (k *DictionaryEncoder) EncodeDatabaseAsDropped(ctx context.Context, tx transaction.Tx, dbName string, namespaceId uint32, existingId uint32) error {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return err
	}
	if len(dbName) == 0 {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "database name is empty")
	}

	// remove existing entry
	toDeleteKey := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), dbKey, dbName, keyEnd)
	newKey := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), dbKey, dbName, keyDroppedEnd)
	return k.encodeAsDropped(ctx, tx, toDeleteKey, newKey, existingId, dbKey)
}

func (k *DictionaryEncoder) EncodeCollectionName(ctx context.Context, tx transaction.Tx, collection string, namespaceId uint32, dbId uint32) (uint32, error) {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return InvalidId, err
	}
	if err := k.validDatabaseId(dbId); err != nil {
		return InvalidId, err
	}
	if len(collection) == 0 {
		return InvalidId, api.Errorf(api.Code_INVALID_ARGUMENT, "collection name is empty")
	}

	key := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), collectionKey, collection, keyEnd)
	return k.encode(ctx, tx, key, collectionKey)
}

func (k *DictionaryEncoder) EncodeCollectionAsDropped(ctx context.Context, tx transaction.Tx, collection string, namespaceId uint32, dbId uint32, existingId uint32) error {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return err
	}
	if err := k.validDatabaseId(dbId); err != nil {
		return err
	}
	if len(collection) == 0 {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "collection name is empty")
	}

	// remove existing entry
	toDeleteKey := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), collectionKey, collection, keyEnd)
	newKey := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), collectionKey, collection, keyDroppedEnd)
	return k.encodeAsDropped(ctx, tx, toDeleteKey, newKey, existingId, collectionKey)
}

func (k *DictionaryEncoder) EncodeIndexName(ctx context.Context, tx transaction.Tx, indexName string, namespaceId uint32, dbId uint32, collId uint32) (uint32, error) {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return InvalidId, err
	}
	if err := k.validDatabaseId(dbId); err != nil {
		return InvalidId, err
	}
	if err := k.validCollectionId(collId); err != nil {
		return InvalidId, err
	}
	if len(indexName) == 0 {
		return InvalidId, api.Errorf(api.Code_INVALID_ARGUMENT, "index name is empty")
	}

	key := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), indexKey, indexName, keyEnd)
	return k.encode(ctx, tx, key, indexKey)
}

func (k *DictionaryEncoder) EncodeIndexAsDropped(ctx context.Context, tx transaction.Tx, indexName string, namespaceId uint32, dbId uint32, collId uint32, existingId uint32) error {
	if err := k.validNamespaceId(namespaceId); err != nil {
		return err
	}
	if err := k.validDatabaseId(dbId); err != nil {
		return err
	}
	if err := k.validCollectionId(collId); err != nil {
		return err
	}
	if len(indexName) == 0 {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "index name is empty")
	}

	toDeleteKey := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), indexKey, indexName, keyEnd)
	newKey := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), indexKey, indexName, keyDroppedEnd)
	return k.encodeAsDropped(ctx, tx, toDeleteKey, newKey, existingId, indexKey)
}

func (k *DictionaryEncoder) encodeAsDropped(ctx context.Context, tx transaction.Tx, toDeleteKey keys.Key, newKey keys.Key, newValue uint32, encName string) error {
	if err := tx.Delete(ctx, toDeleteKey); err != nil {
		log.Debug().Str("key", toDeleteKey.String()).Err(err).Str("type", encName).Msg("existing entry deletion failed")
		return err
	}
	log.Debug().Str("key", toDeleteKey.String()).Str("type", encName).Msg("existing entry deletion succeed")

	// now do insert because we need to fail if token is already assigned
	if err := tx.Replace(ctx, newKey, internal.NewTableData(UInt32ToByte(newValue))); err != nil {
		log.Debug().Str("key", newKey.String()).Uint32("value", newValue).Err(err).Str("type", encName).Msg("encoding failed")
		return err
	}
	log.Debug().Str("key", newKey.String()).Uint32("value", newValue).Str("type", encName).Msg("encoding succeed")

	return nil
}

func (k *DictionaryEncoder) encode(ctx context.Context, tx transaction.Tx, key keys.Key, encName string) (uint32, error) {
	reserveToken, err := k.reservedSb.allocateToken(ctx, tx, string(k.EncodingSubspaceName()))
	if err != nil {
		return InvalidId, err
	}

	// now do insert because we need to fail if token is already assigned
	if err := tx.Insert(ctx, key, internal.NewTableData(UInt32ToByte(reserveToken))); err != nil {
		log.Debug().Str("type", encName).Str("key", key.String()).Uint32("value", reserveToken).Err(err).Msg("encoding failed for")
		return InvalidId, err
	}
	log.Debug().Str("type", encName).Str("key", key.String()).Uint32("value", reserveToken).Msg("encoding succeed for")

	return reserveToken, nil
}

func (k *DictionaryEncoder) validNamespaceId(id uint32) error {
	if id == InvalidId {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "invalid namespace id")
	}
	return nil
}

func (k *DictionaryEncoder) validDatabaseId(id uint32) error {
	if id == InvalidId {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "invalid database id")
	}
	return nil
}

func (k *DictionaryEncoder) validCollectionId(id uint32) error {
	if id == InvalidId {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "invalid collection id")
	}
	return nil
}

func (k *DictionaryEncoder) GetDatabases(ctx context.Context, tx transaction.Tx, namespaceId uint32) (map[string]uint32, error) {
	databases := make(map[string]uint32)
	it, err := tx.Read(ctx, keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), dbKey))
	if err != nil {
		return nil, err
	}

	droppedDatabase := make(map[string]uint32)
	var v kv.KeyValue
	for it.Next(&v) {
		if len(v.Key) < 5 {
			return nil, api.Errorf(api.Code_INTERNAL, "not a valid key %v", v.Key)
		}
		if len(v.Key) == 5 {
			// format <version,namespace-id,db,dbName,keyEnd>
			end, ok := v.Key[4].(string)
			if !ok || (end != keyEnd && end != keyDroppedEnd) {
				return nil, api.Errorf(api.Code_INTERNAL, "database encoding is missing %v", v.Key)
			}

			name, ok := v.Key[3].(string)
			if !ok {
				return nil, api.Errorf(api.Code_INTERNAL, "database name not found %T %v", v.Key[3], v.Key[3])
			}

			if end == keyDroppedEnd {
				droppedDatabase[name] = ByteToUInt32(v.Data.RawData)
				continue
			}

			databases[name] = ByteToUInt32(v.Data.RawData)
		}
	}

	// retrogression check; if created and dropped both exists then the created id should be greater than dropped id
	log.Debug().Interface("existing_dropped", droppedDatabase).Msg("dropped databases")
	log.Debug().Interface("existing_created", databases).Msg("created databases")
	for droppedDB, droppedValue := range droppedDatabase {
		if createdValue, ok := databases[droppedDB]; ok && droppedValue >= createdValue {
			return nil, api.Errorf(api.Code_INTERNAL, "retrogression found in database assigned value database [%s] droppedValue [%d] createdValue [%d]", droppedDB, droppedValue, createdValue)
		}
	}

	return databases, it.Err()
}

func (k *DictionaryEncoder) GetCollections(ctx context.Context, tx transaction.Tx, namespaceId uint32, databaseId uint32) (map[string]uint32, error) {
	var collections = make(map[string]uint32)
	it, err := tx.Read(ctx, keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(databaseId)))
	if err != nil {
		return nil, err
	}

	droppedCollection := make(map[string]uint32)
	var v kv.KeyValue
	for it.Next(&v) {
		if len(v.Key) < 6 {
			return nil, api.Errorf(api.Code_INTERNAL, "not a valid key %v", v.Key)
		}

		if len(v.Key) == 6 {
			// format <version,namespace-id,db-id,coll,coll-name,keyEnd>
			end, ok := v.Key[5].(string)
			if !ok || (end != keyEnd && end != keyDroppedEnd) {
				return nil, api.Errorf(api.Code_INTERNAL, "collection encoding is missing %v", v.Key)
			}

			name, ok := v.Key[4].(string)
			if !ok {
				return nil, api.Errorf(api.Code_INTERNAL, "collection name not found %T %v", v.Key[4], v.Key[4])
			}

			if end == keyDroppedEnd {
				droppedCollection[name] = ByteToUInt32(v.Data.RawData)
				continue
			}

			collections[name] = ByteToUInt32(v.Data.RawData)
		}
	}

	// retrogression check; if created and dropped both exists then the created id should be greater than dropped id
	log.Debug().Uint32("db", databaseId).Interface("existing_dropped", droppedCollection).Msg("dropped collections")
	log.Debug().Uint32("db", databaseId).Interface("existing_created", collections).Msg("created collections")
	for droppedC, droppedValue := range droppedCollection {
		if createdValue, ok := collections[droppedC]; ok && droppedValue >= createdValue {
			return nil, api.Errorf(api.Code_INTERNAL, "retrogression found in collection assigned value collection [%s] droppedValue [%d] createdValue [%d]", droppedC, droppedValue, createdValue)
		}
	}

	return collections, it.Err()
}

func (k *DictionaryEncoder) GetIndexes(ctx context.Context, tx transaction.Tx, namespaceId uint32, databaseId uint32, collId uint32) (map[string]uint32, error) {
	var indexes = make(map[string]uint32)
	it, err := tx.Read(ctx, keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(databaseId), UInt32ToByte(collId)))
	if err != nil {
		return nil, err
	}

	droppedIndexes := make(map[string]uint32)
	var v kv.KeyValue
	for it.Next(&v) {
		if len(v.Key) < 6 {
			return nil, api.Errorf(api.Code_INTERNAL, "not a valid key %v", v.Key)
		}
		if len(v.Key) == 7 {
			// if it is the format <version,namespace-id,db-id,coll-id,indexName,index-name,keyEnd>
			end, ok := v.Key[6].(string)
			if !ok || (end != keyEnd && end != keyDroppedEnd) {
				// if it is not index skip it
				continue
			}

			name, ok := v.Key[5].(string)
			if !ok {
				return nil, api.Errorf(api.Code_INTERNAL, "index name not found %T %v", v.Key[5], v.Key[5])
			}

			if end == keyDroppedEnd {
				droppedIndexes[name] = ByteToUInt32(v.Data.RawData)
				continue
			}

			indexes[name] = ByteToUInt32(v.Data.RawData)
		}
	}

	log.Debug().Uint32("db", databaseId).Uint32("coll", collId).Interface("existing_dropped", droppedIndexes).Msg("dropped indexes")
	log.Debug().Uint32("db", databaseId).Uint32("coll", collId).Interface("existing_created", indexes).Msg("created indexes")
	// retrogression check
	for droppedC, droppedValue := range droppedIndexes {
		if createdValue, ok := indexes[droppedC]; ok && droppedValue >= createdValue {
			return nil, api.Errorf(api.Code_INTERNAL, "retrogression found in indexes assigned value index [%s] droppedValue [%d] createdValue [%d]", droppedC, droppedValue, createdValue)
		}
	}

	return indexes, it.Err()
}

func (k *DictionaryEncoder) GetDatabaseId(ctx context.Context, tx transaction.Tx, dbName string, namespaceId uint32) (uint32, error) {
	key := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), dbKey, dbName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *DictionaryEncoder) GetCollectionId(ctx context.Context, tx transaction.Tx, collName string, namespaceId uint32, dbId uint32) (uint32, error) {
	key := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), collectionKey, collName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *DictionaryEncoder) GetIndexId(ctx context.Context, tx transaction.Tx, indexName string, namespaceId uint32, dbId uint32, collId uint32) (uint32, error) {
	key := keys.NewKey(k.EncodingSubspaceName(), encVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), indexKey, indexName, keyEnd)
	return k.getId(ctx, tx, key)
}

func (k *DictionaryEncoder) getId(ctx context.Context, tx transaction.Tx, key keys.Key) (uint32, error) {
	it, err := tx.Read(ctx, key)
	if err != nil {
		return InvalidId, err
	}

	var row kv.KeyValue
	if it.Next(&row) {
		return ByteToUInt32(row.Data.RawData), nil
	}

	if err := it.Err(); err != nil {
		return 0, err
	}

	// no need to return an error if not found, upper layer will convert this as an error.
	return InvalidId, nil
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
