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

package database

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	jsoniter "github.com/json-iterator/go"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	"github.com/tigrisdata/tigris/util/log"
)

type TentativeSearchKeysToRemove struct{}

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

func (i *SearchIndexer) OnPostCommit(ctx context.Context, _ *metadata.Tenant, eventListener kv.EventListener) error {
	if request.DisableSearch(ctx) {
		return nil
	}

	for _, event := range eventListener.GetEvents() {
		var err error

		db, collName, ok := i.tenantMgr.DecodeTableName(event.Table)
		if !ok {
			continue
		}

		collection := db.GetCollection(collName)
		if collection == nil || event.Key == nil {
			// event.Key == nil if event comes from drop table
			continue
		}

		searchKey, err := CreateSearchKey(event.Key)
		if err != nil {
			return err
		}

		searchIndex := collection.GetImplicitSearchIndex()
		if searchIndex == nil {
			return fmt.Errorf("implicit search index not found")
		}

		if event.Op == kv.DeleteEvent {
			if err = i.searchStore.DeleteDocument(ctx, searchIndex.StoreIndexName(), searchKey); err != nil {
				if !search.IsErrNotFound(err) {
					return err
				}

				return nil
			}
		} else {
			var action search.IndexAction
			switch event.Op {
			case kv.InsertEvent:
				action = search.Create
			case kv.ReplaceEvent:
				action = search.Replace
			case kv.UpdateEvent:
				action = search.Update
			}

			searchData, err := PackSearchFields(ctx, event.Data, collection, searchKey)
			if err != nil {
				return err
			}

			reader := bytes.NewReader(searchData)

			var resp []search.IndexResp

			if resp, err = i.searchStore.IndexDocuments(ctx, searchIndex.StoreIndexName(), reader, search.IndexDocumentsOptions{
				Action:    action,
				BatchSize: 1,
			}); err != nil {
				return err
			}

			if len(resp) == 1 && !resp[0].Success {
				return search.NewSearchError(resp[0].Code, search.ErrCodeUnhandled, resp[0].Error)
			}
		}
	}

	return nil
}

func (*SearchIndexer) OnPreCommit(context.Context, *metadata.Tenant, transaction.Tx, kv.EventListener) error {
	return nil
}

func (*SearchIndexer) OnRollback(context.Context, *metadata.Tenant, kv.EventListener) {}

func CreateSearchKey(key kv.Key) (string, error) {
	// the zeroth element is index key name i.e. pkey
	key = key[1:]
	if len(key) == 0 {
		return "", fmt.Errorf("not able to create search key for indexing")
	}

	if len(key) == 1 {
		// simply marshal it if it is single primary key
		var value string
		switch t := key[0].(type) {
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
	}

	var tp tuple.Tuple
	for _, k := range key {
		tp = append(tp, k)
	}
	// for composite there is no easy way, pack it and then base64 encode it
	return base64.StdEncoding.EncodeToString(tp.Pack()), nil
}

func PackSearchFields(ctx context.Context, data *internal.TableData, collection *schema.DefaultCollection, id string) ([]byte, error) {
	// better to decode it and then update the JSON
	decData, err := util.JSONToMap(data.RawData)
	if err != nil {
		return nil, err
	}

	if value, ok := decData[schema.SearchId]; ok {
		// if user schema collection has id field already set then change it
		decData[schema.ReservedFields[schema.IdToSearchKey]] = value
	}

	doNotFlatten := container.NewHashSet()
	for _, f := range collection.QueryableFields {
		if f.DoNotFlatten {
			doNotFlatten.Insert(f.Name())
		}
	}

	decData = util.FlatMap(decData, doNotFlatten)

	keysToRemove := ctx.Value(TentativeSearchKeysToRemove{})
	if keysToRemove != nil {
		if conv, ok := keysToRemove.([]string); ok {
			for _, k := range conv {
				// remove object keys that are not part of the update request but needs to be cleaned up from search.
				if _, found := decData[k]; !found {
					// only remove if it is not set in the current payload
					decData[k] = nil
				}
			}
		}
	}

	var nullKeys []string
	// pack any date time or array fields here
	for _, f := range collection.QueryableFields {
		key, value := f.Name(), decData[f.Name()]
		if value == nil {
			if f.DataType == schema.ArrayType || f.DataType == schema.ObjectType {
				nullKeys = append(nullKeys, f.Name())
				delete(decData, f.Name())
			}

			continue
		} else if len(f.AllowedNestedQFields) > 0 {
			removeNullsFromArrayObj(decData, f.Name())
		}

		if f.SearchType == "string[]" {
			// if string array has null set then replace it with our null marker
			if valueArr, ok := value.([]any); ok {
				for i, item := range valueArr {
					if item == nil {
						valueArr[i] = schema.ReservedFields[schema.SearchArrNullItem]
					}
				}
			}
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

	if len(nullKeys) > 0 {
		decData[schema.ReservedFields[schema.SearchNullKeys]] = nullKeys
	}

	decData[schema.SearchId] = id
	decData[schema.ReservedFields[schema.CreatedAt]] = data.CreatedAt.UnixNano()
	if data.UpdatedAt != nil {
		decData[schema.ReservedFields[schema.UpdatedAt]] = data.UpdatedAt.UnixNano()
	}

	encoded, err := util.MapToJSON(decData)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

func removeNullObjectLow(obj any) {
	switch orig := obj.(type) {
	case []any:
		for _, a := range orig {
			removeNullObjectLow(a)
		}
	case map[string]any:
		for key, value := range orig {
			if value == nil {
				delete(orig, key)
			} else {
				removeNullObjectLow(value)
			}
		}
	}
}

func removeNullsFromArrayObj(doc map[string]any, parent string) {
	if arr, ok := doc[parent].([]any); ok {
		for _, each := range arr {
			removeNullObjectLow(each)
		}
	}
}

func UnpackSearchFields(doc map[string]any, collection *schema.DefaultCollection) (string, *internal.TableData, map[string]any, error) {
	userCreatedAt := false
	userUpdatedAt := false
	for _, f := range collection.QueryableFields {
		if f.FieldName == "created_at" {
			userCreatedAt = true
		}
		if f.FieldName == "updated_at" {
			userUpdatedAt = true
		}
	}
	// set tableData with metadata
	tableData := &internal.TableData{}
	// data prior to _tigris_ prefix
	tableData.CreatedAt = getInternalTS(doc, "created_at")
	if !userCreatedAt {
		delete(doc, "created_at")
	}
	if createdAt := getInternalTS(doc, schema.ReservedFields[schema.CreatedAt]); createdAt != nil {
		// prioritize the value from the _tigris_ prefix
		tableData.CreatedAt = createdAt
		delete(doc, schema.ReservedFields[schema.CreatedAt])
	}

	// data prior to _tigris_ prefix
	tableData.UpdatedAt = getInternalTS(doc, "updated_at")
	if !userUpdatedAt {
		delete(doc, "updated_at")
	}
	if updatedAt := getInternalTS(doc, schema.ReservedFields[schema.UpdatedAt]); updatedAt != nil {
		// prioritize the value from the _tigris_ prefix
		tableData.UpdatedAt = updatedAt
		delete(doc, schema.ReservedFields[schema.UpdatedAt])
	}

	// process user fields now
	var arrayOfObjects []string
	for _, f := range collection.QueryableFields {
		if f.SearchType == "string[]" {
			// if string array has our internal null marker
			if valueArr, ok := doc[f.FieldName].([]any); ok {
				for i, item := range valueArr {
					if item == schema.ReservedFields[schema.SearchArrNullItem] {
						valueArr[i] = nil
					}
				}
			}
		}
		if f.ShouldPack() {
			if v, ok := doc[f.Name()]; ok {
				switch f.DataType {
				case schema.ArrayType:
					if _, ok := v.(string); ok {
						var value any
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
						var value any
						if err := jsoniter.UnmarshalFromString(v.(string), &value); err != nil {
							return "", nil, nil, err
						}
						doc[f.Name()] = value
					}
				}
			}
		}
		if f.DataType == schema.ArrayType && f.SubType == schema.ObjectType {
			arrayOfObjects = append(arrayOfObjects, f.Name())
		}
	}
	for k, v := range doc {
		for _, ao := range arrayOfObjects {
			if strings.HasPrefix(k, ao+util.ObjFlattenDelimiter) {
				if _, ok := v.([]any); !ok {
					zlog.Info().Msgf("found non slice entry in flattened array of object element '%v'", v)
				}
				delete(doc, k)
			}
		}
	}

	if v, found := doc[schema.ReservedFields[schema.SearchNullKeys]]; found {
		if vArr, ok := v.([]string); ok {
			for _, k := range vArr {
				doc[k] = nil
			}
		}
		delete(doc, schema.ReservedFields[schema.SearchNullKeys])
	}

	// unFlatten the map now
	doc = util.UnFlatMap(doc, config.DefaultConfig.Search.IgnoreExtraFields)

	searchKey := doc[schema.SearchId].(string)
	if value, ok := doc[schema.ReservedFields[schema.IdToSearchKey]]; ok {
		// if user has an id field then check it and set it back
		doc[schema.SearchId] = value
		delete(doc, schema.ReservedFields[schema.IdToSearchKey])
	} else {
		// otherwise, remove the search id from the result
		delete(doc, schema.SearchId)
	}

	return searchKey, tableData, doc, nil
}

func getInternalTS(doc map[string]any, keyName string) *internal.Timestamp {
	if value, ok := doc[keyName]; ok {
		conv, ok := value.(json.Number)
		if ok {
			nano, err := conv.Int64()
			if !log.E(err) {
				return internal.CreateNewTimestamp(nano)
			}
		}
	}

	return nil
}
