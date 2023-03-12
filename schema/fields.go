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
	"regexp"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/server/config"
)

type FieldType int

const (
	searchDoubleType = "float"
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

var (
	MsgFieldNameInvalidPattern = "Invalid collection field name, field name can only contain [a-zA-Z0-9_$] and it can only start with [a-zA-Z_$] for fieldName = '%s'"
	ValidFieldNamePattern      = regexp.MustCompile(`^[a-zA-Z_$][a-zA-Z0-9_$]*$`)
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

func IsValidKeyType(t FieldType) bool {
	switch t {
	case Int32Type, Int64Type, StringType, ByteType, DateTimeType, UUIDType:
		return true
	default:
		return false
	}
}

func IsPrimitiveType(fieldType FieldType) bool {
	switch fieldType {
	case BoolType, Int32Type, Int64Type, UUIDType, StringType, DateTimeType, DoubleType:
		return true
	}
	return false
}

func SupportedIndexableType(fieldType FieldType) bool {
	switch fieldType {
	case BoolType, Int32Type, Int64Type, UUIDType, StringType, DateTimeType, DoubleType:
		return true
	default:
		return false
	}
}

func SupportedSearchIndexableType(fieldType FieldType, subType FieldType) bool {
	switch fieldType {
	case BoolType, Int32Type, Int64Type, UUIDType, StringType, DateTimeType, DoubleType, ArrayType:
		return true
	case ObjectType:
		return subType == UnknownType
	default:
		return false
	}
}

func SupportedFacetableType(fieldType FieldType, subType FieldType) bool {
	switch fieldType {
	case Int32Type, Int64Type, StringType, DoubleType:
		return true
	case ArrayType:
		return subType == Int32Type || subType == Int64Type || subType == StringType || subType == DoubleType
	default:
		return false
	}
}

// DefaultFacetableType are the types for which faceting is automatically enabled for database collections.
func DefaultFacetableType(fieldType FieldType) bool {
	switch fieldType {
	case Int32Type, Int64Type, DoubleType:
		return true
	default:
		return false
	}
}

// DefaultSortableType are the types for which sorting is automatically enabled.
func DefaultSortableType(fieldType FieldType) bool {
	switch fieldType {
	case Int32Type, Int64Type, DoubleType, DateTimeType, BoolType:
		return true
	default:
		return false
	}
}

func toSearchFieldType(fieldType FieldType, subType FieldType) string {
	switch fieldType {
	case BoolType:
		return FieldNames[fieldType]
	case Int32Type, Int64Type:
		return FieldNames[fieldType]
	case StringType, ByteType, UUIDType:
		return FieldNames[StringType]
	case DateTimeType:
		return FieldNames[Int64Type]
	case DoubleType:
		return searchDoubleType
	case ObjectType:
		return FieldNames[ObjectType]
	case ArrayType:
		switch subType {
		case BoolType:
			return FieldNames[subType] + "[]"
		case Int32Type, Int64Type:
			return FieldNames[subType] + "[]"
		case StringType, ByteType, UUIDType:
			return FieldNames[StringType] + "[]"
		case DateTimeType:
			return FieldNames[Int64Type] + "[]"
		case DoubleType:
			return searchDoubleType + "[]"
		case ObjectType:
			return FieldNames[ObjectType] + "[]"
		default:
			// pack it
			return FieldNames[StringType]
		}
	}

	return ""
}

var SupportedFieldProperties = container.NewHashSet(
	"type",
	"format",
	"items",
	"maxLength",
	"description",
	"contentEncoding",
	"properties",
	"autoGenerate",
	"sorted",
	"sort",
	"index",
	"facet",
	"searchIndex",
	"default",
	"createdAt",
	"updatedAt",
	"title",
	"required",
	"index",
	"facet",
	"searchIndex",
)

// Indexes is to wrap different index that a collection can have.
type Indexes struct {
	PrimaryKey     *Index
	SecondaryIndex *Index
}

func (i *Indexes) GetIndexes() []*Index {
	var indexes []*Index
	indexes = append(indexes, i.PrimaryKey)
	return indexes
}

// Index can be composite, so it has a list of fields, each index has name and encoded id. The encoded is used for key
// building.
type Index struct {
	// Fields that are part of this index. An index can have a single or composite fields.
	Fields []*Field
	// Name is used by dictionary encoder for this index.
	Name string
	// Id is assigned to this index by the dictionary encoder.
	Id uint32
}

func (i *Index) IsCompatible(i1 *Index) error {
	if i.Name != i1.Name {
		return errors.InvalidArgument("index name mismatch")
	}

	if i.Id != i1.Id {
		return errors.Internal("internal id mismatch")
	}
	if len(i.Fields) != len(i1.Fields) {
		return errors.InvalidArgument("number of index fields changed")
	}

	for j := 0; j < len(i.Fields); j++ {
		if i.Fields[j].FieldName != i1.Fields[j].FieldName {
			return errors.InvalidArgument("index fields modified expected %q, found %q", i.Fields[j].FieldName, i1.Fields[j].FieldName)
		}

		if err := i.Fields[j].IsCompatible(i1.Fields[j]); err != nil {
			return err
		}
	}

	return nil
}

type FieldBuilder struct {
	FieldName   string
	Description string              `json:"description,omitempty"`
	Type        string              `json:"type,omitempty"`
	Format      string              `json:"format,omitempty"`
	Encoding    string              `json:"contentEncoding,omitempty"`
	Default     interface{}         `json:"default,omitempty"`
	CreatedAt   *bool               `json:"createdAt,omitempty"`
	UpdatedAt   *bool               `json:"updatedAt,omitempty"`
	MaxLength   *int32              `json:"maxLength,omitempty"`
	Auto        *bool               `json:"autoGenerate,omitempty"`
	Sorted      *bool               `json:"sort,omitempty"`
	Index       *bool               `json:"index,omitempty"`
	Facet       *bool               `json:"facet,omitempty"`
	SearchIndex *bool               `json:"searchIndex,omitempty"`
	Items       *FieldBuilder       `json:"items,omitempty"`
	Properties  jsoniter.RawMessage `json:"properties,omitempty"`
	Primary     *bool
	Fields      []*Field
}

func (f *FieldBuilder) Build(setSearchDefaults bool) (*Field, error) {
	fieldType := ToFieldType(f.Type, f.Encoding, f.Format)
	if setSearchDefaults {
		// for search indexes, any field in schema is search indexable if it is not set explicitly.
		// Similarly, we also tag it with sort if it is numeric.
		if f.supportableFieldForSearchAttributes(fieldType) {
			ptrTrue := true
			if f.SearchIndex == nil {
				f.SearchIndex = &ptrTrue
			}
			if f.Sorted == nil && DefaultSortableType(fieldType) {
				// enable it by default for numeric fields
				f.Sorted = &ptrTrue
			}
		}
	}

	field := &Field{
		FieldName:       f.FieldName,
		MaxLength:       f.MaxLength,
		DataType:        fieldType,
		Fields:          f.Fields,
		Sorted:          f.Sorted,
		Indexed:         f.Index,
		Faceted:         f.Facet,
		SearchIndexed:   f.SearchIndex,
		PrimaryKeyField: f.Primary,
		AutoGenerated:   f.Auto,
	}

	if f.CreatedAt != nil || f.UpdatedAt != nil || f.Default != nil {
		var err error
		if field.Defaulter, err = newDefaulter(f.CreatedAt, f.UpdatedAt, field.FieldName, field.DataType, f.Default); err != nil {
			// just log, as this should not happen and during incoming request the field builder should have already caught this.
			log.Err(err).Msgf("defaulter creation failed for field '%s'", field.FieldName)
		}
	}

	return field, nil
}

func (f *FieldBuilder) supportableFieldForSearchAttributes(fieldType FieldType) bool {
	return len(f.FieldName) > 0 && (fieldType != ObjectType || (fieldType == ObjectType && len(f.Fields) == 0))
}

type Field struct {
	FieldName       string
	Defaulter       *FieldDefaulter
	DataType        FieldType
	MaxLength       *int32
	FillCreatedAt   *bool
	FillUpdatedAt   *bool
	UniqueKeyField  *bool
	PrimaryKeyField *bool
	AutoGenerated   *bool
	Sorted          *bool
	Indexed         *bool
	Faceted         *bool
	SearchIndexed   *bool
	// Nested fields are the fields where we know the schema of nested attributes like if properties are
	Fields []*Field
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

func (f *Field) IsSorted() bool {
	return f.Sorted != nil && *f.Sorted
}

func (f *Field) IsIndexed() bool {
	return f.Indexed != nil && *f.Indexed
}

func (f *Field) IsSearchIndexed() bool {
	return f.SearchIndexed != nil && *f.SearchIndexed
}

func (f *Field) IsFaceted() bool {
	return f.Faceted != nil && *f.Faceted
}

func (f *Field) IsCompatible(f1 *Field) error {
	if f.DataType != f1.DataType && !config.DefaultConfig.Schema.AllowIncompatible {
		return errors.InvalidArgument("data type mismatch for field %q", f.FieldName)
	}

	if f.IsPrimaryKey() != f1.IsPrimaryKey() {
		return errors.InvalidArgument("primary key changes are not allowed %q", f.FieldName)
	}

	if f.MaxLength != nil && f1.MaxLength != nil {
		if *f.MaxLength > *f1.MaxLength && !config.DefaultConfig.Schema.AllowIncompatible {
			return errors.InvalidArgument("reducing length of an existing field is not allowed %q", f.FieldName)
		}
	}

	return nil
}

func (f *Field) GetNestedField(name string) *Field {
	for _, r := range f.Fields {
		if r.FieldName == name {
			return r
		}
	}

	return nil
}

func (f *Field) GetDefaulter() *FieldDefaulter {
	return f.Defaulter
}

func (f *Field) IsIndexable() bool {
	if f.Indexed != nil && *f.Indexed && SupportedIndexableType(f.DataType) {
		return true
	}

	return false
}
