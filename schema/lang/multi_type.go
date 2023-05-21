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
	"fmt"

	jsoniter "github.com/json-iterator/go"
)

var (
	ErrInvalidType               = fmt.Errorf("invalid type. expected string or array of strings")
	ErrOnlyOneNonNullTypeAllowed = fmt.Errorf("only one non-null type allowed for the field")
)

type FieldMultiType struct {
	Type       []string
	isNullable bool
}

func (f *FieldMultiType) append(val string) {
	if val == "null" {
		f.isNullable = true
	} else {
		f.Type = append(f.Type, val)
	}
}

func NewMultiType(val string) FieldMultiType {
	var f FieldMultiType

	f.append(val)

	return f
}

func NewStringType() FieldMultiType {
	var f FieldMultiType

	f.append(typeString)

	return f
}

func NewIntegerType() FieldMultiType {
	var f FieldMultiType

	f.append(typeInteger)

	return f
}

func NewFloatType() FieldMultiType {
	var f FieldMultiType

	f.append(typeNumber)

	return f
}

func NewBooleanType() FieldMultiType {
	var f FieldMultiType

	f.append(typeBoolean)

	return f
}

func NewObjectType() FieldMultiType {
	var f FieldMultiType

	f.append(typeObject)

	return f
}

func NewArrayType() FieldMultiType {
	var f FieldMultiType

	f.append(typeArray)

	return f
}

func NewNullableMultiType(val string) FieldMultiType {
	var f FieldMultiType

	f.append(val)

	f.isNullable = true

	return f
}

func (f *FieldMultiType) UnmarshalJSON(b []byte) error {
	var tp any

	if err := jsoniter.Unmarshal(b, &tp); err != nil {
		return err
	}

	switch val := tp.(type) {
	case string:
		f.append(val)
	case []any:
		for _, v := range val {
			s, ok := v.(string)
			if !ok {
				return ErrInvalidType
			}

			f.append(s)
		}
	}

	if len(f.Type) > 1 {
		return ErrOnlyOneNonNullTypeAllowed
	}

	return nil
}

func (f *FieldMultiType) MarshalJSON() ([]byte, error) {
	if !f.isNullable && len(f.Type) == 1 {
		return jsoniter.Marshal(f.Type[0])
	}

	if !f.isNullable {
		return jsoniter.Marshal(f.Type)
	}

	return jsoniter.Marshal(append(f.Type, "null"))
}

func (f *FieldMultiType) First() string {
	if len(f.Type) > 0 {
		return f.Type[0]
	}

	return ""
}

func (f *FieldMultiType) Set(tp string) {
	f.Type = []string{tp}
}

func (f *FieldMultiType) SetNullable() {
	f.isNullable = true
}
