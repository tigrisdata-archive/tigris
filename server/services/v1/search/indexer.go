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

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/util"
	"github.com/tigrisdata/tigris/util/log"
)

func MutateSearchDocument(index *schema.SearchIndex, ts *internal.Timestamp, decData map[string]any, isUpdate bool) (map[string]any, error) {
	doNotFlatten := container.NewHashSet()
	for _, f := range index.QueryableFields {
		if f.DoNotFlatten {
			doNotFlatten.Insert(f.FieldName)
		}
	}

	decData = util.FlatMap(decData, doNotFlatten)

	var nullKeys []string
	// pack any date time or array fields here
	for _, f := range index.QueryableFields {
		key, value := f.Name(), decData[f.Name()]
		if value == nil {
			if (f.DataType == schema.ArrayType || f.DataType == schema.ObjectType) && f.SearchIndexed {
				nullKeys = append(nullKeys, f.Name())
				delete(decData, f.Name())
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
					decData[key] = t
					// pack original date as string to a shadowed key
					decData[schema.ToSearchDateKey(key)] = dateStr
				}
			default:
				var err error
				if decData[key], err = jsoniter.MarshalToString(value); err != nil {
					return nil, err
				}
			}
		}
	}
	if len(nullKeys) > 0 {
		decData[schema.ReservedFields[schema.SearchNullKeys]] = nullKeys
	}

	field := schema.CreatedAt
	if isUpdate {
		field = schema.UpdatedAt
	}
	decData[schema.ReservedFields[field]] = ts.UnixNano()

	return decData, nil
}

func UnpackSearchFields(index *schema.SearchIndex, doc map[string]any) (map[string]any, *internal.Timestamp, *internal.Timestamp, error) {
	// set tableData with metadata
	for _, f := range index.QueryableFields {
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

	createdAt := getInternalTS(doc, schema.ReservedFields[schema.CreatedAt])
	updatedAt := getInternalTS(doc, schema.ReservedFields[schema.UpdatedAt])
	delete(doc, schema.ReservedFields[schema.CreatedAt])
	delete(doc, schema.ReservedFields[schema.UpdatedAt])

	// unFlatten the map now
	doc = util.UnFlatMap(doc)
	return doc, createdAt, updatedAt, nil
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
