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

	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"google.golang.org/grpc/codes"
)

type Fields interface{}

type FieldType int

const (
	UnknownType FieldType = iota + 1
	NullType
	BoolType
	IntType
	BigIntType
	DoubleType
	BytesType
	StringType
)

const (
	nullDef   = "null"
	boolDef   = "bool"
	intDef    = "int"
	bigIntDef = "bigint"
	doubleDef = "double"
	bytesDef  = "bytes"
	stringDef = "string"
)

func ToFieldType(t string) FieldType {
	t = strings.ToLower(t)
	switch t {
	case nullDef:
		return NullType
	case boolDef:
		return BoolType
	case intDef:
		return IntType
	case bigIntDef:
		return BigIntType
	case doubleDef:
		return DoubleType
	case bytesDef:
		return BytesType
	case stringDef:
		return StringType
	default:
		return UnknownType
	}
}

func ToStringType(t FieldType) string {
	switch t {
	case NullType:
		return nullDef
	case IntType:
		return intDef
	case DoubleType:
		return doubleDef
	case BytesType:
		return bytesDef
	case StringType:
		return stringDef
	default:
		return "unknown"
	}
}

func IsValidIndexType(t FieldType) bool {
	switch t {
	case IntType, BigIntType, StringType, BytesType:
		return true
	default:
		return false
	}
}

type FieldBuilder struct {
	Description string `json:"description,omitempty"`
	FieldName   string
	Type        string `json:"type,omitempty"`
	MaxLength   *int32 `json:"max_length,omitempty"`
	Primary     *bool
	Unique      *bool `json:"unique,omitempty"`
}

func (f *FieldBuilder) Build() (*Field, error) {
	var field = &Field{}
	field.FieldName = f.FieldName
	field.MaxLength = f.MaxLength
	field.UniqueKeyField = f.Unique
	fieldType := ToFieldType(f.Type)
	if fieldType == UnknownType {
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
