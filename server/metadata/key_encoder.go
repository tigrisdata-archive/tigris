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
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
)

type SearchEncoder interface {
	// EncodeSearchTableName will encode search index created by the user and return an encoded string that will be use
	// as an index name in the underlying search store.
	EncodeSearchTableName(tenantId uint32, projId uint32, indexName string) string
	// DecodeSearchTableName will decode the information from encoded search index Name. This method returns tenant id,
	// project id, index name.
	DecodeSearchTableName(name string) (uint32, uint32, string, bool)
	// EncodeFDBSearchTableName is the search index table in FDB
	EncodeFDBSearchTableName(searchTable string) []byte
	// EncodeFDBSearchKey is the search row-key in FDB
	EncodeFDBSearchKey(searchTable string, id string) (keys.Key, error)
}

type CacheEncoder interface {
	EncodeCacheTableName(tenantId uint32, projId uint32, name string) (string, error)
	DecodeCacheTableName(stream string) (uint32, uint32, string, bool)
	DecodeInternalCacheKeyNameToExternal(internalKey string) string
}

// Encoder is used to encode/decode values of the Key.
type Encoder interface {
	SearchEncoder

	// EncodeTableName returns encoded bytes which are formed by combining namespace, database, and collection.
	EncodeTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) ([]byte, error)
	// EncodeSecondaryIndexTableName returns encoded bytes for the table name of a collections secondary index.
	EncodeSecondaryIndexTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) ([]byte, error)
	EncodePartitionTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) ([]byte, error)
	// EncodeIndexName returns encoded bytes for the index name
	EncodeIndexName(idx *schema.Index) []byte
	// EncodeKey returns encoded bytes of the key which will be used to store the values in fdb. The Key return by this
	// method has two parts,
	//   - tableName: This is set with an encoding of namespace, database and collection id.
	//   - IndexParts: This has the index identifier and value(s) associated with a single or composite index. This is appended
	//	   to the table name to form the Key. The first element of this list is the dictionary encoding of index type key
	//	   information i.e. whether the index is pkey, etc. The remaining elements are values for this index.
	EncodeKey(encodedTable []byte, idx *schema.Index, idxParts []any) (keys.Key, error)

	// DecodeTableName is used to decode the key stored in FDB and extract namespace name, database name and collection ids.
	DecodeTableName(tableName []byte) (uint32, uint32, uint32, bool)
	DecodeIndexName(indexName []byte) uint32
}

// NewCacheEncoder creates CacheEncoder to encode cache tenant, project and keys.
func NewCacheEncoder() CacheEncoder {
	return &DictKeyEncoder{}
}

// NewEncoder creates Dictionary metaStore to encode keys.
func NewEncoder() Encoder {
	return &DictKeyEncoder{}
}

type DictKeyEncoder struct{}

// EncodeTableName creates storage friendly table name from namespace, database and collection ids
// Database and collection objects can be omitted to get table name prefix.
// If the collection is omitted then result name includes all the collections in the database
// If both database and collections are omitted then result name includes all databases in the namespace.
func (d *DictKeyEncoder) EncodeTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) ([]byte, error) {
	return d.encodedTableName(ns, db, coll, internal.UserTableKeyPrefix), nil
}

func (d *DictKeyEncoder) EncodeSecondaryIndexTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) ([]byte, error) {
	return d.encodedTableName(ns, db, coll, internal.SecondaryTableKeyPrefix), nil
}

func (d *DictKeyEncoder) EncodePartitionTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) ([]byte, error) {
	return d.encodedTableName(ns, db, coll, internal.PartitionKeyPrefix), nil
}

func (d *DictKeyEncoder) EncodeIndexName(idx *schema.Index) []byte {
	return d.encodedIdxName(idx)
}

func (d *DictKeyEncoder) EncodeKey(encodedTable []byte, idx *schema.Index, idxParts []any) (keys.Key, error) {
	if idx == nil {
		return nil, errors.InvalidArgument("index is missing")
	}

	encodedIdxName := d.encodedIdxName(idx)

	var remainingKeyParts []any
	remainingKeyParts = append(remainingKeyParts, encodedIdxName)
	remainingKeyParts = append(remainingKeyParts, idxParts...)

	return keys.NewKey(encodedTable, remainingKeyParts...), nil
}

func (*DictKeyEncoder) encodedTableName(ns Namespace, db *Database, coll *schema.DefaultCollection, prefix []byte) []byte {
	var appendTo []byte
	appendTo = append(appendTo, prefix...)
	appendTo = append(appendTo, UInt32ToByte(ns.Id())...)
	if db != nil {
		appendTo = append(appendTo, UInt32ToByte(db.id)...)
	}
	if coll != nil {
		appendTo = append(appendTo, UInt32ToByte(coll.Id)...)
	}
	return appendTo
}

func (*DictKeyEncoder) encodedIdxName(idx *schema.Index) []byte {
	return UInt32ToByte(idx.Id)
}

func (d *DictKeyEncoder) DecodeTableName(tableName []byte) (uint32, uint32, uint32, bool) {
	if len(tableName) < 16 || !d.validPrefix(tableName) {
		return 0, 0, 0, false
	}

	nsId := ByteToUInt32(tableName[4:8])
	dbId := ByteToUInt32(tableName[8:12])
	collId := ByteToUInt32(tableName[12:16])

	return nsId, dbId, collId, true
}

func (*DictKeyEncoder) validPrefix(tableName []byte) bool {
	return bytes.Equal(tableName[0:4], internal.UserTableKeyPrefix) || bytes.Equal(tableName[0:4], internal.PartitionKeyPrefix)
}

func (*DictKeyEncoder) DecodeIndexName(indexName []byte) uint32 {
	return ByteToUInt32(indexName)
}

func (*DictKeyEncoder) EncodeCacheTableName(tenantId uint32, projId uint32, name string) (string, error) {
	return fmt.Sprintf("%s:%d:%d:%s", internal.CacheKeyPrefix, tenantId, projId, name), nil
}

func (*DictKeyEncoder) DecodeInternalCacheKeyNameToExternal(internalKey string) string {
	i := 0
	// first four parts are internal (cache prefix, tenant, project, cache name)
	for m := 1; m <= 4; m++ {
		x := strings.Index(internalKey[i:], ":")
		if x < 0 {
			break
		}
		i += x + 1
	}
	return internalKey[i:]
}

func (*DictKeyEncoder) DecodeCacheTableName(name string) (uint32, uint32, string, bool) {
	if !strings.HasPrefix(name, internal.CacheKeyPrefix) {
		return 0, 0, "", false
	}
	allParts := strings.Split(name, ":")
	nsId, _ := strconv.ParseInt(allParts[1], 10, 64)
	pid, _ := strconv.ParseInt(allParts[2], 10, 64)

	return uint32(nsId), uint32(pid), allParts[3], true
}

// EncodeSearchTableName will encode search index created by the user and return an encoded string that will be use
// as an index name in the underlying search store.
func (*DictKeyEncoder) EncodeSearchTableName(tenantId uint32, projId uint32, indexName string) string {
	return fmt.Sprintf("%d:%d:%s", tenantId, projId, indexName)
}

// DecodeSearchTableName will decode the information from encoded search index Name. This method returns tenant id,
// project id, and index name.
func (*DictKeyEncoder) DecodeSearchTableName(name string) (uint32, uint32, string, bool) {
	allParts := strings.Split(name, ":")
	if len(allParts) != 3 {
		return 0, 0, "", false
	}

	nsId, _ := strconv.ParseInt(allParts[0], 10, 64)
	pid, _ := strconv.ParseInt(allParts[1], 10, 64)

	return uint32(nsId), uint32(pid), allParts[2], true
}

func (*DictKeyEncoder) EncodeFDBSearchTableName(searchTable string) []byte {
	var appendTo []byte
	appendTo = append(appendTo, internal.SearchTableKeyPrefix...)
	appendTo = append(appendTo, []byte(searchTable)...)

	return appendTo
}

func (d *DictKeyEncoder) EncodeFDBSearchKey(searchTable string, id string) (keys.Key, error) {
	if len(id) == 0 {
		return nil, fmt.Errorf("expected 'id' to be non-nil")
	}

	return keys.NewKey(d.EncodeFDBSearchTableName(searchTable), id), nil
}
