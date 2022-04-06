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
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/keys"
	"github.com/tigrisdata/tigrisdb/schema"
	"github.com/tigrisdata/tigrisdb/server/metadata/encoding"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"github.com/tigrisdata/tigrisdb/value"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

// Encoder is used to encode/decode values of the Key.
type Encoder interface {
	// EncodeTableName returns encoded bytes which are formed by combining namespace, database, collection and index ids.
	EncodeTableName(ns Namespace, db *Database, coll *schema.DefaultCollection, idx *schema.Index) []byte
	// EncodeKey returns encoded bytes of the key which will be used to store the values in fdb. The Key return by this
	// method is set with encoded table name and index values.
	EncodeKey(ns Namespace, db *Database, coll *schema.DefaultCollection, idx *schema.Index, doc map[string]*structpb.Value) (keys.Key, error)
}

// NewEncoder creates Dictionary encoder to encode keys.
func NewEncoder() Encoder {
	return &DictKeyEncoder{}
}

type DictKeyEncoder struct{}

func (d *DictKeyEncoder) EncodeTableName(ns Namespace, db *Database, coll *schema.DefaultCollection, idx *schema.Index) []byte {
	return d.buildTableParts(ns, db, coll, idx)
}

func (d *DictKeyEncoder) EncodeKey(ns Namespace, db *Database, coll *schema.DefaultCollection, idx *schema.Index, doc map[string]*structpb.Value) (keys.Key, error) {
	if db == nil {
		return nil, api.Errorf(codes.InvalidArgument, "database is missing")
	}
	if coll == nil {
		return nil, api.Errorf(codes.InvalidArgument, "collection is missing")
	}
	if idx == nil {
		return nil, api.Errorf(codes.InvalidArgument, "index is missing")
	}

	indexKeyParts, err := d.buildIndexKeyParts(idx.Fields, doc)
	if err != nil {
		return nil, err
	}

	table := d.buildTableParts(ns, db, coll, idx)
	return keys.NewKey(table, indexKeyParts...), nil
}

func (d *DictKeyEncoder) buildTableParts(ns Namespace, db *Database, coll *schema.DefaultCollection, idx *schema.Index) []byte {
	var tableKey []byte
	tableKey = append(tableKey, encoding.UInt32ToByte(ns.Id())...)
	tableKey = append(tableKey, encoding.UInt32ToByte(db.id)...)
	tableKey = append(tableKey, encoding.UInt32ToByte(coll.Id)...)
	tableKey = append(tableKey, encoding.UInt32ToByte(idx.Id)...)

	return tableKey
}

func (d *DictKeyEncoder) buildIndexKeyParts(userDefinedKeys []*schema.Field, doc map[string]*structpb.Value) ([]interface{}, error) {
	var indexKeyParts []interface{}
	for _, v := range userDefinedKeys {
		k, ok := doc[v.Name()]
		if !ok {
			return nil, ulog.CE("missing index key column(s) %v", v)
		}

		val, err := value.NewValueUsingSchema(v, k)
		if err != nil {
			return nil, err
		}
		indexKeyParts = append(indexKeyParts, val.AsInterface())
	}
	if len(indexKeyParts) == 0 {
		return nil, ulog.CE("missing index key column(s)")
	}
	return indexKeyParts, nil
}
