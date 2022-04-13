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

package schema

import (
	"strings"

	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/lib/set"
	"google.golang.org/grpc/codes"
)

type Fields interface{}

type FieldType int

const (
	UnknownType FieldType = iota + 1
	NullType
	BoolType
	IntType
	DoubleType
	BytesType
	StringType
)

const (
	jsonSpecNull   = "null"
	jsonSpecBool   = "boolean"
	jsonSpecInt    = "integer"
	jsonSpecDouble = "number"
	jsonSpecString = "string"

	jsonSpecEncodingB64 = "base64"
)

func ToFieldType(jsonType string, encoding string) FieldType {
	jsonType = strings.ToLower(jsonType)
	switch jsonType {
	case jsonSpecNull:
		return NullType
	case jsonSpecBool:
		return BoolType
	case jsonSpecInt:
		return IntType
	case jsonSpecDouble:
		return DoubleType
	case jsonSpecString:
		if encoding == jsonSpecEncodingB64 {
			return BytesType
		}
		if len(encoding) > 0 {
			return UnknownType
		}

		return StringType
	default:
		return UnknownType
	}
}

func ToJSONSpecString(t FieldType) string {
	switch t {
	case NullType:
		return jsonSpecNull
	case IntType:
		return jsonSpecInt
	case DoubleType:
		return jsonSpecDouble
	case StringType:
		return jsonSpecString
	default:
		return "unknown"
	}
}

func IsValidIndexType(t FieldType) bool {
	switch t {
	case IntType, StringType, BytesType:
		return true
	default:
		return false
	}
}

var SupportedFieldProperties = set.New(
	"type",
	"format",
	"maxLength",
	"description",
	"contentEncoding",
)

type FieldBuilder struct {
	FieldName   string
	Description string `json:"description,omitempty"`
	Type        string `json:"type,omitempty"`
	Format      string `json:"format,omitempty"`
	Encoding    string `json:"contentEncoding,omitempty"`
	MaxLength   *int32 `json:"maxLength,omitempty"`
	Primary     *bool
}

func (f *FieldBuilder) Validate(v []byte) error {
	var fieldProperties map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(v, &fieldProperties); err != nil {
		return err
	}

	for key := range fieldProperties {
		if !SupportedFieldProperties.Contains(key) {
			return api.Errorf(codes.InvalidArgument, "unsupported property found '%s'", key)
		}
	}

	return nil
}

func (f *FieldBuilder) Build() (*Field, error) {
	var field = &Field{}
	field.FieldName = f.FieldName
	field.MaxLength = f.MaxLength
	fieldType := ToFieldType(f.Type, f.Encoding)
	if fieldType == UnknownType {
		if len(f.Encoding) > 0 {
			return nil, api.Errorf(codes.InvalidArgument, "unsupported encoding '%s'", f.Encoding)
		}
		return nil, api.Errorf(codes.InvalidArgument, "unsupported type detected '%s'", f.Type)
	}
	field.DataType = fieldType
	if f.Primary != nil && *f.Primary {
		// validate the primary key types
		if !IsValidIndexType(field.DataType) {
			return nil, api.Errorf(codes.InvalidArgument, "unsupported primary key type detected '%s'", f.Type)
		}
	}
	field.PrimaryKeyField = f.Primary
	return field, nil
}

type Field struct {
	FieldName       string
	DataType        FieldType
	MaxLength       *int32
	UniqueKeyField  *bool
	PrimaryKeyField *bool
}

func (f *Field) Name() string {
	return f.FieldName
}

func (f *Field) Type() FieldType {
	return f.DataType
}

func (f *Field) IsPrimaryKey() bool {
	if f.PrimaryKeyField == nil {
		return false
	}

	return *f.PrimaryKeyField
}
