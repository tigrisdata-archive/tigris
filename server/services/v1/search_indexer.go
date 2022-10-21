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

var (
// ErrSearchIndexingFailed = fmt.Errorf("failed to index documents")
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

	decData = FlattenObjects(decData)

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
	for _, f := range collection.QueryableFields {
		if f.ShouldPack() {
			if v, ok := doc[f.Name()]; ok {
				switch f.DataType {
				case schema.ArrayType:
					if _, ok := v.(string); ok {
						var value interface{}
						if err := jsoniter.UnmarshalFromString(v.(string), &value); err != nil {
							return "", nil, nil, err
						}
						doc[f.Name()] = value
					}
				case schema.DateTimeType:
					// unpack original date from shadowed key
					shadowedKey := schema.ToSearchDateKey(f.Name())
					doc[f.Name()] = doc[shadowedKey]
					delete(doc, shadowedKey)
				default:
					if _, ok := v.(string); ok {
						var value interface{}
						if err := jsoniter.UnmarshalFromString(v.(string), &value); err != nil {
							return "", nil, nil, err
						}
						doc[f.Name()] = value
					}
				}
			}
		}
	}

	// unFlatten the map now
	doc = UnFlattenObjects(doc)

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

func FlattenObjects(data map[string]any) map[string]any {
	resp := make(map[string]any)
	flattenObjects("", data, resp)
	return resp
}

func flattenObjects(key string, obj map[string]any, resp map[string]any) {
	if key != "" {
		key += schema.ObjFlattenDelimiter
	}

	for k, v := range obj {
		switch vMap := v.(type) {
		case map[string]any:
			flattenObjects(key+k, vMap, resp)
		default:
			resp[key+k] = v
		}
	}
}

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
