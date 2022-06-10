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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/lib/set"
)

type FieldType int

const (
	searchDoubleType = "float"
	searchArrayType  = "[]"
)

const (
	UnknownType FieldType = iota
	NullType
	BoolType
	Int32Type
	Int64Type
	DoubleType
	StringType
	// ByteType is a base64 encoded characters, this means if this type is used as key then we need to decode it
	// and then use it as key.
	ByteType
	UUIDType
	// DateTimeType is a valid date representation as defined by RFC 3339, see https://datatracker.ietf.org/doc/html/rfc3339#section-5.6
	DateTimeType
	ArrayType
	ObjectType
)

var FieldNames = [...]string{
	UnknownType:  "unknown",
	NullType:     "null",
	BoolType:     "bool",
	Int32Type:    "int32",
	Int64Type:    "int64",
	DoubleType:   "double",
	StringType:   "string",
	ByteType:     "byte",
	UUIDType:     "uuid",
	DateTimeType: "datetime",
	ArrayType:    "array",
	ObjectType:   "object",
}

const (
	jsonSpecNull   = "null"
	jsonSpecBool   = "boolean"
	jsonSpecInt    = "integer"
	jsonSpecDouble = "number"
	jsonSpecString = "string"
	jsonSpecArray  = "array"
	jsonSpecObject = "object"

	jsonSpecEncodingB64    = "base64"
	jsonSpecFormatUUID     = "uuid"
	jsonSpecFormatDateTime = "date-time"
	jsonSpecFormatByte     = "byte"
	jsonSpecFormatInt32    = "int32"
	jsonSpecFormatInt64    = "int64"
)

func ToFieldType(jsonType string, encoding string, format string) FieldType {
	jsonType = strings.ToLower(jsonType)
	switch jsonType {
	case jsonSpecNull:
		return NullType
	case jsonSpecBool:
		return BoolType
	case jsonSpecInt:
		if len(format) == 0 {
			return Int64Type
		}

		switch format {
		case jsonSpecFormatInt32:
			return Int32Type
		case jsonSpecFormatInt64:
			return Int64Type
		}
		return UnknownType
	case jsonSpecDouble:
		return DoubleType
	case jsonSpecString:
		// if encoding is set
		switch encoding {
		case jsonSpecEncodingB64:
			// base64 encoded characters
			return ByteType
		default:
			if len(encoding) > 0 {
				return UnknownType
			}
		}

		// if format is specified
		switch format {
		case jsonSpecFormatUUID:
			return UUIDType
		case jsonSpecFormatDateTime:
			return DateTimeType
		case jsonSpecFormatByte:
			return ByteType
		default:
			if len(format) > 0 {
				return UnknownType
			}
		}

		return StringType
	case jsonSpecArray:
		return ArrayType
	case jsonSpecObject:
		return ObjectType
	default:
		return UnknownType
	}
}

func IsValidIndexType(t FieldType) bool {
	switch t {
	case Int32Type, Int64Type, StringType, ByteType, DateTimeType, UUIDType:
		return true
	default:
		return false
	}
}

func IndexableField(field *Field) bool {
	switch field.Type() {
	case BoolType, Int32Type, Int64Type, UUIDType, StringType, DateTimeType, DoubleType:
		return true
	default:
		return false
	}
}

func FacetableField(field *Field) bool {
	switch field.Type() {
	case Int32Type, Int64Type, StringType, DoubleType:
		return true
	default:
		return false
	}
}

func PackSearchField(field *Field) bool {
	switch field.Type() {
	case ObjectType:
		return true
	case ArrayType:
		return len(field.Fields) > 1 || field.Fields[0].Type() == ArrayType || field.Fields[0].Type() == ObjectType
	}

	return false
}

func ToSearchFieldType(field *Field) string {
	switch field.Type() {
	case BoolType:
		return FieldNames[field.Type()]
	case Int32Type, Int64Type:
		return FieldNames[field.Type()]
	case StringType, ByteType, UUIDType, DateTimeType:
		return FieldNames[StringType]
	case DoubleType:
		return searchDoubleType
	case ObjectType:
		return FieldNames[StringType]
	case ArrayType:
		if len(field.Fields) == 1 && field.Fields[0].Type() != ArrayType && field.Fields[0].Type() != ObjectType {
			arrayType := ToSearchFieldType(field.Fields[0])
			return arrayType + searchArrayType
		}

		return FieldNames[StringType]
	}

	return ""
}

var SupportedFieldProperties = set.New(
	"type",
	"format",
	"items",
	"maxLength",
	"description",
	"contentEncoding",
	"properties",
	"autoGenerate",
)

type FieldBuilder struct {
	FieldName   string
	Description string              `json:"description,omitempty"`
	Type        string              `json:"type,omitempty"`
	Format      string              `json:"format,omitempty"`
	Encoding    string              `json:"contentEncoding,omitempty"`
	MaxLength   *int32              `json:"maxLength,omitempty"`
	Auto        *bool               `json:"autoGenerate,omitempty"`
	Items       *FieldBuilder       `json:"items,omitempty"`
	Properties  jsoniter.RawMessage `json:"properties,omitempty"`
	Primary     *bool
	Fields      []*Field
}

func (f *FieldBuilder) Validate(v []byte) error {
	var fieldProperties map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(v, &fieldProperties); err != nil {
		return err
	}

	for key := range fieldProperties {
		if !SupportedFieldProperties.Contains(key) {
			return api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported property found '%s'", key)
		}
	}

	return nil
}

func (f *FieldBuilder) Build() (*Field, error) {
	fieldType := ToFieldType(f.Type, f.Encoding, f.Format)
	if fieldType == UnknownType {
		if len(f.Encoding) > 0 {
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported encoding '%s'", f.Encoding)
		}
		if len(f.Format) > 0 {
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported format '%s'", f.Format)
		}

		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported type detected '%s'", f.Type)
	}
	if f.Primary != nil && *f.Primary {
		// validate the primary key types
		if !IsValidIndexType(fieldType) {
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported primary key type detected '%s'", f.Type)
		}
	}
	if f.Primary == nil && f.Auto != nil && *f.Auto {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "only primary fields can be set as auto-generated '%s'", f.FieldName)
	}

	var field = &Field{}
	field.FieldName = f.FieldName
	field.MaxLength = f.MaxLength
	field.DataType = fieldType
	field.PrimaryKeyField = f.Primary
	field.Fields = f.Fields
	field.AutoGenerated = f.Auto
	return field, nil
}

type Field struct {
	FieldName       string
	DataType        FieldType
	MaxLength       *int32
	UniqueKeyField  *bool
	PrimaryKeyField *bool
	AutoGenerated   *bool
	Fields          []*Field
}

func (f *Field) Name() string {
	return f.FieldName
}

func (f *Field) Type() FieldType {
	return f.DataType
}

func (f *Field) IsPrimaryKey() bool {
	return f.PrimaryKeyField != nil && *f.PrimaryKeyField
}

func (f *Field) IsAutoGenerated() bool {
	return f.AutoGenerated != nil && *f.AutoGenerated
}
