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
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
)

var (
	ErrSearchIndexingFailed = fmt.Errorf("failed to index documents")
)

const (
	searchID = "id"
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

	// TODO: add a pkey check here
	/*
		if tp[0] != schema.PrimaryKeyIndexName {
			// this won't work as tp[0] is dictionary encoded value of PrimaryKeyIndexName
		}
	*/

	// the zeroth entry represents index key name
	tp = tp[1:]

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
			value = string(t)
		}
		return value, nil
	} else {
		// for composite there is no easy way, pack it and then base64 encode it
		return base64.StdEncoding.EncodeToString(tp.Pack()), nil
	}
}

func PackSearchFields(data *internal.TableData, collection *schema.DefaultCollection, id string) ([]byte, error) {
	var err error
	// better to decode it and then update the JSON
	var decData map[string]any
	if err = jsoniter.Unmarshal(data.RawData, &decData); err != nil {
		return nil, err
	}

	if value, ok := decData[searchID]; ok {
		// if user schema collection has id field already set then change it
		decData[schema.ReservedFields[schema.IdToSearchKey]] = value
	}

	decData = FlattenObjects(decData)

	// now if there is any array we need to pack it
	for key, value := range decData {
		if _, ok := value.([]any); ok {
			// pack any array field
			if decData[key], err = jsoniter.MarshalToString(value); err != nil {
				return nil, err
			}
		}
	}

	decData[searchID] = id
	decData[schema.ReservedFields[schema.CreatedAt]] = data.CreatedAt.UnixNano()
	if data.UpdatedAt != nil {
		decData[schema.ReservedFields[schema.UpdatedAt]] = data.UpdatedAt.UnixNano()
	}

	return jsoniter.Marshal(decData)
}

func UnpackSearchFields(doc map[string]interface{}, collection *schema.DefaultCollection) (string, *internal.TableData, map[string]interface{}, error) {
	for _, f := range collection.QueryableFields {
		if f.ShouldPack() {
			if v, ok := doc[f.Name()]; ok {
				var value interface{}
				if err := jsoniter.UnmarshalFromString(v.(string), &value); err != nil {
					return "", nil, nil, err
				}
				doc[f.Name()] = value
			}
		}
	}

	// unFlatten the map now
	doc = UnFlattenObjects(doc)

	var searchKey = doc[searchID].(string)
	if value, ok := doc[schema.ReservedFields[schema.IdToSearchKey]]; ok {
		// if user has an id field then check it and set it back
		doc["id"] = value
		delete(doc, schema.ReservedFields[schema.IdToSearchKey])
	} else {
		// otherwise, remove the search id from the result
		delete(doc, searchID)
	}

	// set tableData with metadata
	var tableData = &internal.TableData{}
	if value, ok := doc[schema.ReservedFields[schema.CreatedAt]]; ok {
		tableData.CreatedAt = internal.CreateNewTimestamp(int64(value.(float64)))
		delete(doc, schema.ReservedFields[schema.CreatedAt])
	}
	if value, ok := (doc)[schema.ReservedFields[schema.UpdatedAt]]; ok {
		tableData.UpdatedAt = internal.CreateNewTimestamp(int64(value.(float64)))
		delete(doc, schema.ReservedFields[schema.UpdatedAt])
	}

	return searchKey, tableData, doc, nil
}

func UnpackAndSetMD(doc map[string]interface{}, tableData *internal.TableData) {
	if v, ok := doc[schema.ReservedFields[schema.CreatedAt]]; ok {
		tableData.CreatedAt = internal.CreateNewTimestamp(int64(v.(float64)))
	}
	if v, ok := doc[schema.ReservedFields[schema.UpdatedAt]]; ok {
		tableData.UpdatedAt = internal.CreateNewTimestamp(int64(v.(float64)))
	}
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
