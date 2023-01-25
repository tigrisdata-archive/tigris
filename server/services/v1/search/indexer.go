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
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/schema"
)

func MutateSearchDocument(index *schema.SearchIndex, decData map[string]any) (map[string]any, error) {
	decData = FlattenObjects(decData)

	// pack any date time or array fields here
	for _, f := range index.QueryableFields {
		key, value := f.Name(), decData[f.Name()]
		if value == nil {
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

	return decData, nil
}

func UnpackSearchFields(index *schema.SearchIndex, doc map[string]any) (map[string]any, error) {
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
							return nil, err
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
							return nil, err
						}
						doc[f.Name()] = value
					}
				}
			}
		}
	}

	// unFlatten the map now
	doc = UnFlattenObjects(doc)
	return doc, nil
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
