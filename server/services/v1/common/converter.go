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

package common

import (
	"strconv"
	"strings"

	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
)

type fieldAccessor func(name string) *schema.Field

type StringToInt64Converter struct {
	converted     bool
	fieldAccessor fieldAccessor
}

func NewStringToInt64Converter(fieldAccessor fieldAccessor) *StringToInt64Converter {
	return &StringToInt64Converter{
		fieldAccessor: fieldAccessor,
	}
}

func (converter *StringToInt64Converter) Convert(doc map[string]any, paths map[string]struct{}) (bool, error) {
	for key := range paths {
		keys := strings.Split(key, ".")
		value, ok := doc[keys[0]]
		if !ok {
			continue
		}

		field := converter.fieldAccessor(keys[0])
		if field == nil {
			continue
		}

		if err := converter.traverse(doc, value, keys[1:], field); err != nil {
			return false, err
		}
	}

	return converter.converted, nil
}

func (converter *StringToInt64Converter) traverse(parentMap map[string]any, value any, keys []string, parentField *schema.Field) error {
	var err error
	if parentField.Type() == schema.Int64Type {
		if conv, ok := value.(string); ok {
			if parentMap[parentField.FieldName], err = strconv.ParseInt(conv, 10, 64); err != nil {
				return errors.InvalidArgument("json schema validation failed for field '%s' reason "+
					"'expected integer, but got string'", parentField.FieldName)
			}

			converter.converted = true
			return nil
		}
		return nil
	}

	switch converted := value.(type) {
	case map[string]any:
		value, ok := converted[keys[0]]
		if !ok {
			return nil
		}

		return converter.traverse(converted, value, keys[1:], parentField.GetNestedField(keys[0]))
	case []any:
		// array should have a single nested field either as object or primitive type
		field := parentField.Fields[0]
		if field.DataType == schema.ObjectType {
			// is object or simple type
			for _, va := range converted {
				if err := converter.traverse(va.(map[string]any), va, keys, field); err != nil {
					return err
				}
			}
		} else if field.DataType == schema.Int64Type {
			for idx := range converted {
				if conv, ok := converted[idx].(string); ok {
					if converted[idx], err = strconv.ParseInt(conv, 10, 64); err != nil {
						return errors.InvalidArgument("json schema validation failed for field '%s' "+
							"reason 'expected integer, but got string'", field.FieldName)
					}
					converter.converted = true
				}
			}
		}
	}

	return nil
}
