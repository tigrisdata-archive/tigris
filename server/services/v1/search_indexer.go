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

package v1

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/lib/date"
	tjson "github.com/tigrisdata/tigris/lib/json"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util/log"
)

const (
	searchCreate string = "create"
	searchUpsert string = "upsert"
	searchUpdate string = "update"
)

type SearchIndexer struct {
	searchStore search.Store
	tenantMgr   *metadata.TenantManager
}

func NewSearchIndexer(searchStore search.Store, tenantMgr *metadata.TenantManager) *SearchIndexer {
	return &SearchIndexer{
		searchStore: searchStore,
		tenantMgr:   tenantMgr,
	}
}

func (i *SearchIndexer) OnPostCommit(ctx context.Context, tenant *metadata.Tenant, eventListener kv.EventListener) error {
	for _, event := range eventListener.GetEvents() {
		var err error

		_, db, coll, ok := i.tenantMgr.DecodeTableName(event.Table)
		if !ok {
			continue
		}

		collection := tenant.GetCollection(db, coll)
		if collection == nil {
			continue
		}

		searchKey, err := CreateSearchKey(event.Table, event.Key)
		if err != nil {
			return err
		}

		if event.Op == kv.DeleteEvent {
			if err = i.searchStore.DeleteDocuments(ctx, collection.SearchCollectionName(), searchKey); err != nil {
				if err != search.ErrNotFound {
					return err
				}
				return nil
			}
		} else {
			var action string
			switch event.Op {
			case kv.InsertEvent:
				action = searchCreate
			case kv.ReplaceEvent:
				action = searchUpsert
			case kv.UpdateEvent:
				action = searchUpdate
			}

			tableData, err := internal.Decode(event.Data)
			if err != nil {
				return err
			}

			searchData, err := PackSearchFields(tableData, collection, searchKey)
			if err != nil {
				return err
			}

			reader := bytes.NewReader(searchData)
			if err = i.searchStore.IndexDocuments(ctx, collection.SearchCollectionName(), reader, search.IndexDocumentsOptions{
				Action:    action,
				BatchSize: 1,
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (i *SearchIndexer) OnPreCommit(context.Context, *metadata.Tenant, transaction.Tx, kv.EventListener) error {
	return nil
}

func (i *SearchIndexer) OnRollback(context.Context, *metadata.Tenant, kv.EventListener) {}

func CreateSearchKey(table []byte, fdbKey []byte) (string, error) {
	sb := subspace.FromBytes(table)
	tp, err := sb.Unpack(fdb.Key(fdbKey))
	if err != nil {
		return "", err
	}

	if bytes.Equal(table[0:4], internal.UserTableKeyPrefix) {
		// TODO: add a pkey check here
		// the zeroth entry represents index key name
		tp = tp[1:]
	} else {
		// the zeroth entry represents index key name, the first entry represent partition key
		tp = tp[2:]
	}

	if len(tp) == 1 {
		// simply marshal it if it is single primary key
		var value string
		switch t := tp[0].(type) {
		case int:
			// we need to convert numeric to string
			value = fmt.Sprintf("%d", t)
		case int32:
			value = fmt.Sprintf("%d", t)
		case int64:
			value = fmt.Sprintf("%d", t)
		case string:
			value = t
		case []byte:
			value = base64.StdEncoding.EncodeToString(t)
		}
		return value, nil
	} else {
		// for composite there is no easy way, pack it and then base64 encode it
		return base64.StdEncoding.EncodeToString(tp.Pack()), nil
	}
}

// ToPackKeys returns parent names that we need to pack before sending to indexing store. This
// packing doesn't mean that we are not flattening this object.
func ToPackKeys(collection *schema.DefaultCollection) map[string]struct{} {
	toPackKeys := make(map[string]struct{})
	for _, f := range collection.QueryableFields {
		if f.FlattenedAsArray {
			keys := strings.Split(f.Name(), ".")
			toPackKeys[keys[0]] = struct{}{}
		}
	}

	return toPackKeys
}

func removeUnsupportedIndexes(collection *schema.DefaultCollection, decData map[string]any) {
	for _, qf := range collection.QueryableFields {
		// if a field parent already is an array then it becomes Array of array which we do not support in indexes
		// so remove all these fields.
		//
		// Note: Parent of these fields are already packed so this field is present as packed structure.
		if qf.FlattenedAsArray && qf.DataType == schema.ArrayType {
			delete(decData, qf.FieldName)
		}
	}
}

func PackSearchFields(data *internal.TableData, collection *schema.DefaultCollection, id string) ([]byte, error) {
	// better to decode it and then update the JSON
	decData, err := tjson.Decode(data.RawData)
	if err != nil {
		return nil, err
	}

	if value, ok := decData[schema.SearchId]; ok {
		// if user schema collection has id field already set then change it
		decData[schema.ReservedFields[schema.IdToSearchKey]] = value
	}

	// pack any date time or array fields here
	for _, f := range collection.QueryableFields {
		key, value := f.Name(), decData[f.Name()]
		if value == nil {
			continue
		}
		if f.ShouldPack() {
			switch f.DataType {
			case schema.DateTimeType:
				if dateStr, ok := value.(string); ok {
					t, err := date.ToUnixNano(schema.DateTimeFormat, dateStr)
					if err != nil {
						return nil, errors.InvalidArgument("Validation failed, %s is not a valid date-time", dateStr)
					}
					decData[key] = t
					// pack original date as string to a shadowed key
					decData[schema.ToSearchDateKey(key)] = dateStr
				}
			default:
				if decData[key], err = jsoniter.MarshalToString(value); err != nil {
					return nil, err
				}
			}
		}
	}

	toPackKeys := ToPackKeys(collection)

	// first we pack these keys and store it in a map so that we can copy it to original doc after flattening
	packedObjData := make(map[string]string)
	for toPackKey := range toPackKeys {
		// The complex objects are marshalled as string, so that during un-flattening we can parse them as-is
		if v, ok := decData[toPackKey]; ok {
			if packedObjData[toPackKey], err = jsoniter.MarshalToString(v); err != nil {
				return nil, err
			}
		}
	}

	decData = FlattenObjectsAndArray(decData, func(name string) bool {
		qf, _ := collection.GetQueryableField(name)
		return qf.FlattenedAsArray && qf.DataType != schema.ArrayType
	})

	removeUnsupportedIndexes(collection, decData)

	// now we need to add packed objects that have arrays as flattened
	for k, obj := range packedObjData {
		decData[k] = obj
	}

	decData[schema.SearchId] = id
	decData[schema.ReservedFields[schema.CreatedAt]] = data.CreatedAt.UnixNano()
	if data.UpdatedAt != nil {
		decData[schema.ReservedFields[schema.UpdatedAt]] = data.UpdatedAt.UnixNano()
	}

	encoded, err := tjson.Encode(decData)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

func UnpackSearchFields(doc map[string]interface{}, collection *schema.DefaultCollection) (string, *internal.TableData, map[string]interface{}, error) {
	packedKeys := ToPackKeys(collection)
	for _, f := range collection.QueryableFields {
		if f.ShouldPack() {
			if v, ok := doc[f.Name()]; ok {
				switch f.DataType {
				case schema.DateTimeType:
					// unpack original date from shadowed key
					shadowedKey := schema.ToSearchDateKey(f.Name())
					doc[f.Name()] = doc[shadowedKey]
					delete(doc, shadowedKey)
				default:
					var value interface{}
					if err := jsoniter.UnmarshalFromString(v.(string), &value); err != nil {
						return "", nil, nil, err
					}
					doc[f.Name()] = value
				}
			}
		}
	}

	for k := range doc {
		// remove any object that is packed as well as flattened
		if keys := strings.Split(k, "."); len(keys) > 1 {
			if _, ok := packedKeys[keys[0]]; ok {
				delete(doc, k)
			}
		}
	}

	// unFlatten the remaining keys now
	doc = UnFlattenObjects(doc)

	for packedKey := range packedKeys {
		if _, ok := doc[packedKey]; ok {
			var value interface{}
			if err := jsoniter.UnmarshalFromString(doc[packedKey].(string), &value); err != nil {
				return "", nil, nil, err
			}
			doc[packedKey] = value
		}
	}

	searchKey := doc[schema.SearchId].(string)
	if value, ok := doc[schema.ReservedFields[schema.IdToSearchKey]]; ok {
		// if user has an id field then check it and set it back
		doc[schema.SearchId] = value
		delete(doc, schema.ReservedFields[schema.IdToSearchKey])
	} else {
		// otherwise, remove the search id from the result
		delete(doc, schema.SearchId)
	}

	// set tableData with metadata
	tableData := &internal.TableData{}
	if value, ok := doc[schema.ReservedFields[schema.CreatedAt]]; ok {
		nano, err := value.(json.Number).Int64()
		if !log.E(err) {
			tableData.CreatedAt = internal.CreateNewTimestamp(nano)
			delete(doc, schema.ReservedFields[schema.CreatedAt])
		}
	}
	if value, ok := (doc)[schema.ReservedFields[schema.UpdatedAt]]; ok {
		nano, err := value.(json.Number).Int64()
		if !log.E(err) {
			tableData.UpdatedAt = internal.CreateNewTimestamp(nano)
			delete(doc, schema.ReservedFields[schema.UpdatedAt])
		}
	}

	return searchKey, tableData, doc, nil
}

// FlattenObjectsAndArray flattens objects and array of objects. Primitive arrays or an array of arrays remain untouched. The format of flattening is as follows:
//
//	Object,
//	  - {"a": {"b": "c}} will be flattened as {"a.b": "c}
//	Array of Object,
//	  - {"arr_obj": [{"a": 1, "b": "first"}, {"a": 2, "b": "second"}]" will be flattened as {"arr_obj.a":[1, 2], "arr_obj.b":["first","second"]}
//	Primitive arrays inside an object or array of objects follow the same rule as above,
//	  - {"arr_obj": [{"a": [1, 2, 3]}, {"a": [4, 5]}]} will be flattened as {"arr_obj.a": [[1, 2, 3], [4, 5]]}
func FlattenObjectsAndArray(data map[string]any, mergeWithArray func(name string) bool) map[string]any {
	resp := make(map[string]any)
	flattenObjectsAndArray("", data, resp, false, mergeWithArray)
	return resp
}

func flattenObjectsAndArray(key string, obj map[string]any, resp map[string]any, isArray bool, mergeWithArray func(name string) bool) {
	if key != "" {
		key += schema.ObjFlattenDelimiter
	}

	assign := func(result map[string]any, k string, v any, isArray bool) {
		if existing, ok := result[k]; ok {
			if existingArr, ok := existing.([]any); ok {
				if vArr, ok := v.([]any); ok {
					if mergeWithArray(k) {
						// if it is an array, and it needs to be flattened as array, otherwise store as-is
						result[k] = append(existingArr, vArr...)
					} else {
						result[k] = []any{existingArr, v}
					}
				} else {
					result[k] = append(existingArr, v)
				}
			} else {
				result[k] = []any{existing, v}
			}
		} else {
			if isArray {
				if _, ok := v.([]any); ok {
					result[k] = v
				} else {
					result[k] = []any{v}
				}
			} else {
				result[k] = v
			}
		}
	}

	for k, v := range obj {
		switch vTy := v.(type) {
		case map[string]any:
			flattenObjectsAndArray(key+k, vTy, resp, isArray, mergeWithArray)
		case []any:
			if _, ok := vTy[0].(map[string]any); ok {
				for _, va := range vTy {
					flattenObjectsAndArray(key+k, va.(map[string]any), resp, true, mergeWithArray)
				}
			} else {
				assign(resp, key+k, v, true)
			}
		default:
			assign(resp, key+k, v, isArray)
		}
	}
}

// UnFlattenObjects is only responsible for unflattening objects which means that if an array of object is flattened to an
// array by the FlattenObject then that will not be merged back to an Obj. This is handled by the caller as this
// method doesn't expect an array of objects, or a top level object with a nested array of object.
func UnFlattenObjects(flat map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range flat {
		keys := strings.Split(k, schema.ObjFlattenDelimiter)
		m := result
		for i := 1; i < len(keys); i++ {
			if _, ok := m[keys[i-1]]; !ok {
				m[keys[i-1]] = make(map[string]any)
			}
			m = m[keys[i-1]].(map[string]any)
		}
		m[keys[len(keys)-1]] = v
	}

	return result
}
