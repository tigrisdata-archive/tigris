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

package search

import (
	"encoding/json"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/services/v1/common"
	"github.com/tigrisdata/tigris/util"
	"github.com/tigrisdata/tigris/util/log"
)

// writeTransformer is used to transform the incoming user document to the document that we need to index as we need
// inject internal fields and flatten objects.
type writeTransformer interface {
	getOrGenerateId(rawDoc []byte, doc map[string]any) (string, error)
	toSearch(id string, doc map[string]any) (map[string]any, error)
}

// readTransformer is to inverse the indexing document to the user document.
type readTransformer interface {
	fromSearch(doc map[string]any) (map[string]any, *internal.Timestamp, *internal.Timestamp, error)
}

// transformer is used to transform the document and then inverse when we read it from search. The writeTransformer
// is used when writing to search backend. The readTransformer is used when reading data from search backend.
type transformer struct {
	ts        *internal.Timestamp
	index     *schema.SearchIndex
	isUpdate  bool
	converter *common.StringToInt64Converter
}

func newReadTransformer(index *schema.SearchIndex) readTransformer {
	return &transformer{
		index: index,
	}
}

func newWriteTransformer(index *schema.SearchIndex, ts *internal.Timestamp, isUpdate bool) writeTransformer {
	return &transformer{
		ts:        ts,
		index:     index,
		isUpdate:  isUpdate,
		converter: common.NewStringToInt64Converter(index.GetField),
	}
}

func (transformer *transformer) toSearch(id string, doc map[string]any) (map[string]any, error) {
	if err := transformer.transformStart(id, doc); err != nil {
		return nil, err
	}

	return transformer.transformEnd(doc)
}

// transformStart is used to validate/generate the id if missing. After this method, the incoming payload is guaranteed to have
// "id" as top level key in the map.
func (transformer *transformer) transformStart(id string, doc map[string]any) error {
	if _, err := transformer.converter.Convert(doc, transformer.index.GetInt64FieldsPath()); err != nil {
		// User may have an explicit "id" field in the schema which is integer, but it is not a document "id" field
		// so we need to first do any conversion before we change it to "_tigris_id".
		return err
	}

	if transformer.index.SearchIDField != nil && transformer.index.SearchIDField.FieldName != schema.SearchId {
		// if user has "id" field as value then change it to _tigris_id field so that it doesn't clash with search "id"
		// This is only done in case explicitly "@id" tag is set in the schema.
		if value, found := doc[schema.SearchId]; found {
			doc[schema.ReservedFields[schema.IdToSearchKey]] = value
			delete(doc, schema.SearchId)
		}
	}

	doc[schema.SearchId] = id
	return nil
}

// transformEnd is responsible for flattening the map, update the document with timestamp fields, perform any other
// mutation that we need before indexing the document.
func (transformer *transformer) transformEnd(doc map[string]any) (map[string]any, error) {
	doNotFlatten := container.NewHashSet()
	for _, f := range transformer.index.QueryableFields {
		if f.DoNotFlatten {
			doNotFlatten.Insert(f.FieldName)
		}
	}

	doc = util.FlatMap(doc, doNotFlatten)

	var nullKeys []string
	// pack any date time or array fields here
	for _, f := range transformer.index.QueryableFields {
		key, value := f.Name(), doc[f.Name()]
		if value == nil {
			if (f.DataType == schema.ArrayType || f.DataType == schema.ObjectType) && f.SearchIndexed {
				nullKeys = append(nullKeys, f.Name())
				delete(doc, f.Name())
			}

			continue
		}

		if f.SearchType == "string[]" {
			// if string array has null set then replace it with our null marker
			if valueArr, ok := value.([]interface{}); ok {
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
					doc[key] = t
					// pack original date as string to a shadowed key
					doc[schema.ToSearchDateKey(key)] = dateStr
				}
			default:
				var err error
				if doc[key], err = jsoniter.MarshalToString(value); err != nil {
					return nil, err
				}
			}
		}
	}
	if len(nullKeys) > 0 {
		doc[schema.ReservedFields[schema.SearchNullKeys]] = nullKeys
	}

	field := schema.CreatedAt
	if transformer.isUpdate {
		field = schema.UpdatedAt
	}
	doc[schema.ReservedFields[field]] = transformer.ts.UnixNano()

	return doc, nil
}

func (transformer *transformer) fromSearch(doc map[string]any) (map[string]any, *internal.Timestamp, *internal.Timestamp, error) {
	reversed, ts1, ts2, err := transformer.inverseStart(doc)
	if err != nil {
		return nil, nil, nil, err
	}

	transformer.inverseEnd(reversed)

	return reversed, ts1, ts2, nil
}

func (transformer *transformer) inverseStart(doc map[string]any) (map[string]any, *internal.Timestamp, *internal.Timestamp, error) {
	// set tableData with metadata
	for _, f := range transformer.index.QueryableFields {
		if f.SearchType == "string[]" {
			// if string array has our internal null marker
			if valueArr, ok := doc[f.FieldName].([]interface{}); ok {
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
						var value interface{}
						if err := jsoniter.UnmarshalFromString(v.(string), &value); err != nil {
							return nil, nil, nil, err
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
							return nil, nil, nil, err
						}
						doc[f.Name()] = value
					}
				}
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
	if transformer.index.SearchIDField != nil && transformer.index.SearchIDField.FieldName != schema.SearchId {
		// if user has some other key tagged as id, and it is not 'id'
		value, found := doc[schema.ReservedFields[schema.IdToSearchKey]]
		if found {
			doc[schema.SearchId] = value
		}
	}

	createdAt := getInternalTS(doc, schema.ReservedFields[schema.CreatedAt])
	updatedAt := getInternalTS(doc, schema.ReservedFields[schema.UpdatedAt])
	delete(doc, schema.ReservedFields[schema.CreatedAt])
	delete(doc, schema.ReservedFields[schema.UpdatedAt])

	// unFlatten the map now
	doc = util.UnFlatMap(doc)
	return doc, createdAt, updatedAt, nil
}

// inverseEnd is the last step to remove any final transformed data. This is usually called to cleanup "id" at
// top-level if we have added it.
func (transformer *transformer) inverseEnd(doc map[string]any) {
	if transformer.index.SearchIDField != nil {
		// if user has annotated "id" field in the schema then we may need to remove it from the document as we always
		// add top level "id" irrespective of whether user has annotated some other field as @id.
		removeTopLevelId := false
		if transformer.index.SearchIDField.FieldName != schema.SearchId {
			// if search index has different "@id" annotation
			removeTopLevelId = true
		} else if len(transformer.index.SearchIDField.KeyPath()) > 1 {
			// if search index has id field annotated by the user but not top level.
			removeTopLevelId = true
		}

		if removeTopLevelId {
			// first remove it
			delete(doc, schema.SearchId)
		}
		if val, found := doc[schema.ReservedFields[schema.IdToSearchKey]]; found {
			doc[schema.SearchId] = val
			delete(doc, schema.ReservedFields[schema.IdToSearchKey])
		}
	}
}

func (transformer *transformer) getOrGenerateId(rawDoc []byte, doc map[string]any) (string, error) {
	if transformer.index.SearchIDField != nil {
		// if user has annotated a field with search id, then extract it here and if not present return an error
		value, dtp, _, _ := jsonparser.Get(rawDoc, transformer.index.SearchIDField.KeyPath()...)
		if dtp == jsonparser.NotExist {
			return "", errors.InvalidArgument("index has explicitly marked '%s' field as 'id' but document is missing that field", transformer.index.SearchIDField.FieldName)
		}
		if dtp != jsonparser.String {
			return "", errors.InvalidArgument("wrong type of 'id' field")
		}
		if len(value) == 0 {
			return "", errors.InvalidArgument("empty 'id' is not allowed")
		}

		return string(value), nil
	}

	id, ok := doc[schema.SearchId]
	if !ok {
		return uuid.New().String(), nil
	}

	strId, ok := id.(string)
	if !ok {
		return "", errors.InvalidArgument("wrong type of 'id' field")
	}
	if len(strId) == 0 {
		return "", errors.InvalidArgument("empty 'id' is not allowed")
	}

	return strId, nil
}

func getInternalTS(doc map[string]any, keyName string) *internal.Timestamp {
	if value, ok := doc[keyName]; ok {
		switch conv := value.(type) {
		case json.Number:
			nano, err := conv.Int64()
			if !log.E(err) {
				return internal.CreateNewTimestamp(nano)
			}
		case float64:
			return internal.CreateNewTimestamp(int64(conv))
		}
	}

	return nil
}
