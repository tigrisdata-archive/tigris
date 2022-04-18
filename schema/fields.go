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

type FieldType int

const (
	UnknownType FieldType = iota + 1
	NullType
	BoolType
	IntType
	DoubleType
	StringType
	// ByteType is a base64 encoded characters, base64 encoding is done by the user.
	ByteType
	// BinaryType is a any sequence of octets send by the user.
	BinaryType
	UUIDType
	// DateTimeType is a valid date representation as defined by RFC 3339, see https://datatracker.ietf.org/doc/html/rfc3339#section-5.6
	DateTimeType
	ArrayType
	ObjectType
)

const (
	jsonSpecNull   = "null"
	jsonSpecBool   = "boolean"
	jsonSpecInt    = "integer"
	jsonSpecDouble = "number"
	jsonSpecString = "string"
	jsonSpecArray  = "array"
	jsonSpecObject = "object"

	jsonSpecEncodingB64    = "base64"
	jsonSpecEncodingBinary = "binary"
	jsonSpecFormatUUID     = "uuid"
	jsonSpecFormatDateTime = "date-time"
)

func ToFieldType(jsonType string, encoding string, format string) FieldType {
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
		// if encoding is set
		switch encoding {
		case jsonSpecEncodingB64:
			// base64 encoded characters
			return ByteType
		case jsonSpecEncodingBinary:
			return BinaryType
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

func ToFieldTypeString(t FieldType) string {
	switch t {
	case NullType:
		return jsonSpecNull
	case IntType:
		return jsonSpecInt
	case DoubleType:
		return jsonSpecDouble
	case StringType:
		return jsonSpecString
	case ArrayType:
		return jsonSpecArray
	case ObjectType:
		return jsonSpecObject
	case ByteType:
		return "byte"
	case BinaryType:
		return "binary"
	case UUIDType:
		return jsonSpecFormatUUID
	case DateTimeType:
		return jsonSpecFormatDateTime
	default:
		return "unknown"
	}
}

func IsValidIndexType(t FieldType) bool {
	switch t {
	case IntType, StringType, ByteType, DateTimeType, UUIDType, BinaryType:
		return true
	default:
		return false
	}
}

var SupportedFieldProperties = set.New(
	"type",
	"format",
	"items",
	"maxLength",
	"description",
	"contentEncoding",
	"properties",
)

type FieldBuilder struct {
	FieldName   string
	Description string              `json:"description,omitempty"`
	Type        string              `json:"type,omitempty"`
	Format      string              `json:"format,omitempty"`
	Encoding    string              `json:"contentEncoding,omitempty"`
	MaxLength   *int32              `json:"maxLength,omitempty"`
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
			return api.Errorf(codes.InvalidArgument, "unsupported property found '%s'", key)
		}
	}

	return nil
}

func (f *FieldBuilder) Build() (*Field, error) {
	fieldType := ToFieldType(f.Type, f.Encoding, f.Format)
	if fieldType == UnknownType {
		if len(f.Encoding) > 0 {
			return nil, api.Errorf(codes.InvalidArgument, "unsupported encoding '%s'", f.Encoding)
		}
		if len(f.Format) > 0 {
			return nil, api.Errorf(codes.InvalidArgument, "unsupported format '%s'", f.Format)
		}

		return nil, api.Errorf(codes.InvalidArgument, "unsupported type detected '%s'", f.Type)
	}
	if f.Primary != nil && *f.Primary {
		// validate the primary key types
		if !IsValidIndexType(fieldType) {
			return nil, api.Errorf(codes.InvalidArgument, "unsupported primary key type detected '%s'", f.Type)
		}
	}

	var field = &Field{}
	field.FieldName = f.FieldName
	field.MaxLength = f.MaxLength
	field.DataType = fieldType
	field.PrimaryKeyField = f.Primary
	field.Fields = f.Fields
	return field, nil
}

type Field struct {
	FieldName       string
	DataType        FieldType
	MaxLength       *int32
	UniqueKeyField  *bool
	PrimaryKeyField *bool
	Fields          []*Field
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
