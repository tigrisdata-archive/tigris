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

package schema

import (
	"bytes"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
)

type FactoryBuilder struct {
	forSearch     bool
	onUserRequest bool
}

func NewFactoryBuilder(onUserRequest bool) *FactoryBuilder {
	return &FactoryBuilder{
		onUserRequest: onUserRequest,
	}
}

func (fb *FactoryBuilder) setBuilderForSearch() {
	fb.forSearch = true
}

func (fb *FactoryBuilder) deserializeArray(items *FieldBuilder, current *[]*Field) error {
	for ; items.Items != nil; items = items.Items {
		it, err := items.Build(false)
		if err != nil {
			return err
		}

		*current = []*Field{it}
		current = &it.Fields
	}

	// object array type
	if len(items.Properties) > 0 {
		if items.Type != jsonSpecObject {
			return errors.InvalidArgument("Properties only allowed for object type")
		}

		ptrFalse := false
		nestedFields, err := fb.deserializeProperties(items.Properties, nil, &ptrFalse)
		if err != nil {
			return err
		}

		*current = []*Field{{DataType: ObjectType, Fields: nestedFields}}
	} else {
		// primitive array type
		it, err := items.Build(false)
		if err != nil {
			return err
		}

		*current = []*Field{it}
	}

	return nil
}

func (fb *FactoryBuilder) deserializeProperties(properties jsoniter.RawMessage, primaryKeysSet *container.HashSet, setSearchDefaults *bool) ([]*Field, error) {
	var fields []*Field

	err := jsonparser.ObjectEach(properties, func(key []byte, v []byte, dataType jsonparser.ValueType, offset int) error {
		var err error
		var builder FieldBuilder

		if fb.onUserRequest {
			// only validate during incoming request i.e. no need to validate during reloading of schemas.
			if err = ValidateSupportedProperties(v); err != nil {
				// builder validates against the supported schema attributes on properties
				return err
			}
		}

		// set field name and try to unmarshal the value into field builder
		builder.FieldName = string(key)

		dec := jsoniter.NewDecoder(bytes.NewReader(v))
		dec.UseNumber()
		if err = dec.Decode(&builder); err != nil {
			return errors.Internal(err.Error())
		}

		if builder.Type == jsonSpecArray {
			if builder.Items != nil {
				if err = fb.deserializeArray(builder.Items, &builder.Fields); err != nil {
					return err
				}
			} else if builder.MaxItems == nil || *builder.MaxItems != 0 {
				if fb.onUserRequest {
					// only return an error for online requests
					return errors.InvalidArgument("missing items for array field")
				}
			}
		}

		// for objects, properties are part of the field definitions in that case deserialize those
		// nested fields
		if len(builder.Properties) > 0 {
			if builder.Type != jsonSpecObject {
				return errors.InvalidArgument("Properties only allowed for object type")
			}

			if builder.Fields, err = fb.deserializeProperties(builder.Properties, nil, setSearchDefaults); err != nil {
				return err
			}
		}

		if primaryKeysSet != nil && primaryKeysSet.Contains(builder.FieldName) {
			boolTrue := true
			builder.Primary = &boolTrue
		}

		if fb.onUserRequest {
			if err = ValidateFieldBuilder(builder); err != nil {
				return err
			}
		}

		setSearchDefaultsC := false
		if setSearchDefaults != nil {
			setSearchDefaultsC = *setSearchDefaults
		}

		if fb.forSearch && setSearchDefaults == nil {
			setSearchDefaultsC = true
			ty := ToFieldType(builder.Type, builder.Encoding, builder.Format)
			if ty == ObjectType && len(builder.Fields) > 0 {
				setSearchDefaultsC = false
			}
		}

		f, err := builder.Build(setSearchDefaultsC)
		if err != nil {
			return err
		}

		fields = append(fields, f)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return fields, nil
}
