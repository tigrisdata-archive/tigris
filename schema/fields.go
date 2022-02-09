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
)

type Fields interface{}

type FieldType int

const (
	UnknownType FieldType = iota + 1
	NullType
	IntType
	BytesType
	StringType
	UUIDType
)

const (
	nullDef   = "null"
	intDef    = "int"
	bytesDef  = "bytes"
	stringDef = "string"
	uuidDef   = "uuid"
)

func ToFieldType(t string) FieldType {
	t = strings.ToLower(t)
	switch t {
	case nullDef:
		return NullType
	case intDef:
		return IntType
	case bytesDef:
		return BytesType
	case stringDef:
		return StringType
	case uuidDef:
		return UUIDType
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
	case BytesType:
		return bytesDef
	case StringType:
		return stringDef
	case UUIDType:
		return uuidDef
	default:
		return "unknown"
	}
}

type Field interface {
	Name() string
	Type() FieldType
	TypeName() string
	IsPrimaryKey() bool
}

type FieldImpl struct {
	FieldName       string
	DataType        FieldType
	PrimaryKeyField *bool
}

func NewField(name string, ty string, isPrimaryKey bool) *FieldImpl {
	f := FieldImpl{
		FieldName: name,
		DataType:  ToFieldType(ty),
	}
	if isPrimaryKey {
		f.PrimaryKeyField = &isPrimaryKey
	}

	return &f
}

func (f *FieldImpl) Name() string {
	return f.FieldName
}

func (f *FieldImpl) Type() FieldType {
	return f.DataType
}

func (f *FieldImpl) IsPrimaryKey() bool {
	if f.PrimaryKeyField == nil {
		return false
	}

	return *f.PrimaryKeyField
}
