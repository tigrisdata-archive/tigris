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
	"encoding/binary"
	"fmt"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
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
//
//	["encoding", 0x01, x "db", "db-1", "created"] = 0x01
//	["encoding", 0x01, x, "db", "db-2", "created"] = 0x02
//
// Request: To Create a New Collection
// The value would be derived from the counter value stored in the reserved table. It can't be empty as there must be
// a database already created so a value is already assigned to it.
//
//	["encoding", 0x01, x, 0x01, "coll", "coll-1", "created"] = 0x03
//
// Request: To Add an Index
//
//	["encoding", 0x01, x, 0x01, 0x03, "index", "pkey", "created"] = 0x04
//	["encoding", 0x01, x, 0x01, 0x03, "index", "email_index", "created"] = 0x05
//
// Request: To drop an Index
//
//	["encoding", 0x01, x, 0x01, 0x03, "index", "pkey", "dropped"] = 0x04
const (
	namespaceKey = "namespace"
	dbKey        = "db"

	collectionKey = "coll"
	counterKey    = "counter"
	indexKey      = "index"
	keyEnd        = "created"
	keyDroppedEnd = "dropped"
)

const (
	namespaceIntEncoding  = 0
	namespaceJsonEncoding = 1

	encKeyVersion byte = 1
)

var reservedBaseValue = uint32(1)

// reservedSubspace struct is used to manage reserved subspace.
type reservedSubspace struct {
	sync.RWMutex
	NameRegistry

	BaseCounterValue uint32

	idToNamespaceStruct    map[uint32]NamespaceMetadata
	strIdToNamespaceStruct map[string]NamespaceMetadata
}

func newReservedSubspace(mdNameRegistry *NameRegistry) *reservedSubspace {
	return &reservedSubspace{
		NameRegistry:     *mdNameRegistry,
		BaseCounterValue: mdNameRegistry.BaseCounterValue,

		idToNamespaceStruct:    make(map[uint32]NamespaceMetadata),
		strIdToNamespaceStruct: make(map[string]NamespaceMetadata),
	}
}

func (r *reservedSubspace) getNamespaces() map[string]NamespaceMetadata {
	result := make(map[string]NamespaceMetadata)

	for name, metadata := range r.strIdToNamespaceStruct {
		result[name] = metadata
	}

	return result
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
			return errors.Internal("not a valid key %v", row.Key)
		}

		allocatedTo := row.Key[len(row.Key)-2]
		nsName, ok := allocatedTo.(string)
		if !ok {
			return errors.Internal("unable to deduce the encoded key from fdb key %T", allocatedTo)
		}

		var nsMeta NamespaceMetadata

		// TODO: Remove Encoding field from the TableData and use Ver
		// TODO: after all metadata migrated.
		// TODO: If the cluster is created after the time of writing this
		// TODO: comment then it's safe to remove below Encoding check.
		if row.Data.Encoding == namespaceJsonEncoding || row.Data.Ver != 0 {
			if err = jsoniter.Unmarshal(row.Data.RawData, &nsMeta); err != nil {
				return errors.Internal("unable to read the namespace for the namespaceKey %s", allocatedTo)
			}
		} else if row.Data.Encoding == namespaceIntEncoding {
			namespaceId := ByteToUInt32(row.Data.RawData)
			log.Warn().Uint32("id", namespaceId).Str("name", nsName).Msg("legacy namespace metadata format")
			// for legacy use display name same as the namespace name
			nsMeta = NewNamespaceMetadata(namespaceId, nsName, nsName)
		}

		r.idToNamespaceStruct[nsMeta.Id] = nsMeta
		r.strIdToNamespaceStruct[nsMeta.StrId] = nsMeta
	}

	return it.Err()
}

func (r *reservedSubspace) reserveNamespace(ctx context.Context, tx transaction.Tx, namespaceId string,
	namespaceMetadata NamespaceMetadata,
) error {
	if len(namespaceId) == 0 {
		return errors.InvalidArgument("namespaceId is empty")
	}
	if namespaceMetadata.Id < 1 {
		return errors.InvalidArgument("id should be greater than 0, received %d", namespaceMetadata.Id)
	}

	if err := r.reload(ctx, tx); ulog.E(err) {
		return err
	}

	r.RLock()
	defer r.RUnlock()

	if _, ok := r.idToNamespaceStruct[namespaceMetadata.Id]; ok {
		for strId := range r.strIdToNamespaceStruct {
			if r.strIdToNamespaceStruct[strId].Id == namespaceMetadata.Id {
				log.Debug().Uint32("id", namespaceMetadata.Id).Str("strId", strId).
					Msg("namespace reserved for")
				return errors.AlreadyExists("id is already assigned to the namespace '%s'", strId)
			}
		}
	}

	key := keys.NewKey(r.ReservedSubspaceName(), namespaceKey, namespaceId, keyEnd)
	// now do an insert to fail if namespace already exists.
	namespaceMetadataBytes, err := jsoniter.Marshal(namespaceMetadata)
	if err != nil {
		return err
	}

	err = tx.Insert(ctx, key, internal.NewTableDataWithVersion(namespaceMetadataBytes, nsMetaValueVersion))

	log.Debug().Err(err).Str("key", key.String()).Uint32("value", namespaceMetadata.Id).
		Msg("reserving namespace")

	return err
}

func (r *reservedSubspace) allocateToken(ctx context.Context, tx transaction.Tx, keyName string) (uint32, error) {
	key := keys.NewKey(r.ReservedSubspaceName(), keyName, counterKey, keyEnd)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return 0, err
	}

	newValue := r.BaseCounterValue

	var row kv.KeyValue
	if it.Next(&row) {
		newValue = ByteToUInt32(row.Data.RawData) + 1
	}

	if err = it.Err(); err != nil {
		return 0, err
	}

	if err = tx.Replace(ctx, key, internal.NewTableData(UInt32ToByte(newValue)), false); err != nil {
		log.Debug().Str("key", key.String()).Uint32("value", newValue).
			Msg("allocating token failed")
		return 0, err
	}

	log.Debug().Str("key", key.String()).Uint32("value", newValue).Msg("allocating token succeed")

	return newValue, nil
}

// Dictionary is used to replace variable length strings to their corresponding codes to allocateAndSave it.
// Compression is achieved by replacing long strings with a simple 4byte representation.
type Dictionary struct {
	NameRegistry

	reservedSb *reservedSubspace

	nsStore      *NamespaceSubspace
	clusterStore *ClusterSubspace
	collStore    *CollectionSubspace
	dbStore      *DatabaseSubspace
	idxManager   *IndexManager
	// idxStore     *IndexSubspace

	schemaStore       *SchemaSubspace
	searchSchemaStore *SearchSchemaSubspace
}

func NewMetadataDictionary(mdNameRegistry *NameRegistry) *Dictionary {
	return &Dictionary{
		NameRegistry: *mdNameRegistry,

		reservedSb:        newReservedSubspace(mdNameRegistry),
		nsStore:           NewNamespaceStore(mdNameRegistry),
		clusterStore:      NewClusterStore(mdNameRegistry),
		collStore:         newCollectionStore(mdNameRegistry),
		idxManager:        newIndexManager(mdNameRegistry),
		dbStore:           newDatabaseStore(mdNameRegistry),
		schemaStore:       NewSchemaStore(mdNameRegistry),
		searchSchemaStore: NewSearchSchemaStore(mdNameRegistry),
	}
}

func (k *Dictionary) Cluster() *ClusterSubspace {
	return k.clusterStore
}

func (k *Dictionary) Collection() *CollectionSubspace {
	return k.collStore
}

func (k *Dictionary) Database() *DatabaseSubspace {
	return k.dbStore
}

func (k *Dictionary) Index() *IndexManager {
	return k.idxManager
}

func (k *Dictionary) Schema() *SchemaSubspace {
	return k.schemaStore
}

func (k *Dictionary) SearchSchema() *SearchSchemaSubspace {
	return k.searchSchemaStore
}

func (k *Dictionary) Namespace() *NamespaceSubspace {
	return k.nsStore
}

// ReserveNamespace is the first step in the encoding and the mapping is passed the caller. As this is the first encoded
// integer the caller needs to make sure a unique value is assigned to this namespace.
func (k *Dictionary) ReserveNamespace(ctx context.Context, tx transaction.Tx, namespaceId string,
	namespaceMetadata NamespaceMetadata,
) error {
	return k.reservedSb.reserveNamespace(ctx, tx, namespaceId, namespaceMetadata)
}

func (k *Dictionary) GetNamespaces(ctx context.Context, tx transaction.Tx,
) (map[string]NamespaceMetadata, error) {
	if err := k.reservedSb.reload(ctx, tx); err != nil {
		return nil, err
	}

	return k.reservedSb.getNamespaces(), nil
}

func (k *Dictionary) CreateDatabase(ctx context.Context, tx transaction.Tx, name string, namespaceId uint32,
) (*DatabaseMetadata, error) {
	id, err := k.allocate(ctx, tx)
	if err != nil {
		return nil, err
	}

	meta := &DatabaseMetadata{ID: id}

	if err = k.Database().insert(ctx, tx, namespaceId, name, meta); err != nil {
		return nil, err
	}

	return meta, nil
}

// DropDatabase will remove the "created" entry from the encoding subspace and will add a "dropped" entry with the same
// value.
func (k *Dictionary) DropDatabase(ctx context.Context, tx transaction.Tx, dbName string, namespaceId uint32,
) error {
	return k.Database().softDelete(ctx, tx, namespaceId, dbName)
}

func (k *Dictionary) CreateCollection(ctx context.Context, tx transaction.Tx, name string,
	namespaceId uint32, dbId uint32,
) (*CollectionMetadata, error) {
	id, err := k.allocate(ctx, tx)
	if err != nil {
		return nil, err
	}

	meta := &CollectionMetadata{ID: id}

	err = k.Collection().insert(ctx, tx, namespaceId, dbId, name, meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func (k *Dictionary) DropCollection(ctx context.Context, tx transaction.Tx, collection string,
	namespaceId uint32, dbId uint32,
) error {
	return k.Collection().softDelete(ctx, tx, namespaceId, dbId, collection)
}

// Active indexes are created on a new collection and no background work is required to build an index.
func (k *Dictionary) CreateActiveIndex(ctx context.Context, tx transaction.Tx, idx *schema.Index, namespaceId uint32,
	dbId uint32, collId uint32,
) (*IndexMetadata, error) {
	id, err := k.allocate(ctx, tx)
	if err != nil {
		return nil, err
	}

	return k.idxManager.InsertActiveIndex(ctx, tx, namespaceId, dbId, collId, idx, id)
}

func (k *Dictionary) CreateIndex(ctx context.Context, tx transaction.Tx, idx *schema.Index, namespaceId uint32,
	dbId uint32, collId uint32,
) (*IndexMetadata, error) {
	id, err := k.allocate(ctx, tx)
	if err != nil {
		return nil, err
	}

	return k.idxManager.InsertIndex(ctx, tx, namespaceId, dbId, collId, idx, id)
}

func (k *Dictionary) DropIndex(ctx context.Context, tx transaction.Tx, idxMeta *IndexMetadata, namespaceId uint32,
	dbId uint32, collId uint32,
) error {
	return k.idxManager.SoftDelete(ctx, tx, namespaceId, dbId, collId, idxMeta)
}

func (k *Dictionary) allocate(ctx context.Context, tx transaction.Tx) (uint32, error) {
	return k.reservedSb.allocateToken(ctx, tx, string(k.EncodingSubspaceName()))
}

func (k *Dictionary) GetDatabases(ctx context.Context, tx transaction.Tx, namespaceId uint32,
) (map[string]*DatabaseMetadata, error) {
	return k.Database().list(ctx, tx, namespaceId)
}

func (k *Dictionary) GetCollections(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	databaseId uint32,
) (map[string]*CollectionMetadata, error) {
	return k.Collection().list(ctx, tx, namespaceId, databaseId)
}

func (k *Dictionary) GetIndexes(ctx context.Context, tx transaction.Tx, namespaceId uint32, databaseId uint32,
	collId uint32,
) (map[string]*IndexMetadata, error) {
	return k.Index().GetIndexes(ctx, tx, namespaceId, databaseId, collId)
}

func (k *Dictionary) GetDatabase(ctx context.Context, tx transaction.Tx, dbName string, namespaceId uint32,
) (*DatabaseMetadata, error) {
	return k.Database().Get(ctx, tx, namespaceId, dbName)
}

func (k *Dictionary) GetCollection(ctx context.Context, tx transaction.Tx, collName string,
	namespaceId uint32, dbId uint32,
) (*CollectionMetadata, error) {
	return k.Collection().Get(ctx, tx, namespaceId, dbId, collName)
}

func (k *Dictionary) GetIndex(ctx context.Context, tx transaction.Tx, indexName string, namespaceId uint32,
	dbId uint32, collId uint32,
) (*IndexMetadata, error) {
	return k.Index().Get(ctx, tx, namespaceId, dbId, collId, indexName)
}

// decode is currently only use for debugging purpose, once we have a layer on top of this encoding then
// we leverage this method.
func (k *Dictionary) decode(_ context.Context, fdbKey kv.Key) (map[string]interface{}, error) {
	decoded := make(map[string]interface{})
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
