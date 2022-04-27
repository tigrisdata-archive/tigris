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

package metadata

import (
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata/encoding"
	"google.golang.org/grpc/codes"
)

// Encoder is used to encode/decode values of the Key.
type Encoder interface {
	// EncodeTableName returns encoded bytes which are formed by combining namespace, database, and collection.
	EncodeTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) []byte
	// EncodeIndexName returns encoded bytes for the index name
	EncodeIndexName(idx *schema.Index) []byte
	// EncodeKey returns encoded bytes of the key which will be used to store the values in fdb. The Key return by this
	// method has two parts,
	//   - tableName: This is set with an encoding of namespace, database and collection id.
	//   - IndexParts: This has the index identifier and value(s) associated with a single or composite index. This is appended
	//	   to the table name to form the Key. The first element of this list is the dictionary encoding of index type key
	//	   information i.e. whether the index is pkey, etc. The remaining elements are values for this index.
	EncodeKey(ns Namespace, db *Database, coll *schema.DefaultCollection, idx *schema.Index, idxParts []interface{}) (keys.Key, error)

	DecodeTableName(tableName []byte) (uint32, uint32, uint32)
	DecodeIndexName(indexName []byte) uint32
}

// NewEncoder creates Dictionary encoder to encode keys.
func NewEncoder() Encoder {
	return &DictKeyEncoder{}
}

type DictKeyEncoder struct{}

func (d *DictKeyEncoder) EncodeTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) []byte {
	return d.encodedTableName(ns, db, coll)
}

func (d *DictKeyEncoder) EncodeIndexName(idx *schema.Index) []byte {
	return d.encodedIdxName(idx)
}

func (d *DictKeyEncoder) EncodeKey(ns Namespace, db *Database, coll *schema.DefaultCollection, idx *schema.Index, idxParts []interface{}) (keys.Key, error) {
	if db == nil {
		return nil, api.Errorf(codes.InvalidArgument, "database is missing")
	}
	if coll == nil {
		return nil, api.Errorf(codes.InvalidArgument, "collection is missing")
	}
	if idx == nil {
		return nil, api.Errorf(codes.InvalidArgument, "index is missing")
	}

	encodedTable := d.encodedTableName(ns, db, coll)
	encodedIdxName := d.encodedIdxName(idx)

	var remainingKeyParts []interface{}
	remainingKeyParts = append(remainingKeyParts, encodedIdxName)
	remainingKeyParts = append(remainingKeyParts, idxParts...)

	return keys.NewKey(encodedTable, remainingKeyParts...), nil
}

func (d *DictKeyEncoder) encodedTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) []byte {
	var appendTo []byte
	appendTo = append(appendTo, encoding.UInt32ToByte(ns.Id())...)
	appendTo = append(appendTo, encoding.UInt32ToByte(db.id)...)
	appendTo = append(appendTo, encoding.UInt32ToByte(coll.Id)...)
	return appendTo
}

func (d *DictKeyEncoder) encodedIdxName(idx *schema.Index) []byte {
	return encoding.UInt32ToByte(idx.Id)
}

func (d *DictKeyEncoder) DecodeTableName(tableName []byte) (uint32, uint32, uint32) {
	nsId := encoding.ByteToUInt32(tableName[0:4])
	dbId := encoding.ByteToUInt32(tableName[4:8])
	collId := encoding.ByteToUInt32(tableName[8:12])

	return nsId, dbId, collId
}

func (d *DictKeyEncoder) DecodeIndexName(indexName []byte) uint32 {
	return encoding.ByteToUInt32(indexName)
}
