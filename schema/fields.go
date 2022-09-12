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
	"regexp"
	"strings"

	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/util"
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
	MsgFieldNameAsLanguageKeyword = "Invalid collection field name, It contains language keyword for fieldName = '%s'"
	MsgFieldNameInvalidPattern    = "Invalid collection field name, field name can only contain [a-zA-Z0-9_$] and it can only start with [a-zA-Z_$] for fieldName = '%s'"
	ValidFieldNamePattern         = regexp.MustCompile(`^[a-zA-Z_$][a-zA-Z0-9_$]*$`)
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

func IsValidIndexType(t FieldType) bool {
	switch t {
	case Int32Type, Int64Type, StringType, ByteType, DateTimeType, UUIDType:
		return true
	default:
		return false
	}
}

func IndexableField(fieldType FieldType) bool {
	switch fieldType {
	case BoolType, Int32Type, Int64Type, UUIDType, StringType, DateTimeType, DoubleType:
		return true
	default:
		return false
	}
}

func FacetableField(fieldType FieldType) bool {
	switch fieldType {
	case Int32Type, Int64Type, StringType, DoubleType:
		return true
	default:
		return false
	}
}

func SortableField(fieldType FieldType) bool {
	switch fieldType {
	case Int32Type, Int64Type, DoubleType, DateTimeType, BoolType:
		return true
	default:
		return false
	}
}

func toSearchFieldType(fieldType FieldType) string {
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
	case ArrayType:
		return FieldNames[StringType]
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
)

// Indexes is to wrap different index that a collection can have.
type Indexes struct {
	PrimaryKey *Index
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
		return api.Errorf(api.Code_INVALID_ARGUMENT, "index name mismatch")
	}

	if i.Id != i1.Id {
		return api.Errorf(api.Code_INTERNAL, "internal id mismatch")
	}
	if len(i.Fields) != len(i1.Fields) {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "number of index fields changed")
	}

	for j := 0; j < len(i.Fields); j++ {
		if i.Fields[j].FieldName != i1.Fields[j].FieldName {
			return api.Errorf(api.Code_INVALID_ARGUMENT, "index fields modified expected %q, found %q", i.Fields[j].FieldName, i1.Fields[j].FieldName)
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

func (f *FieldBuilder) Build(isArrayElement bool) (*Field, error) {
	if IsReservedField(f.FieldName) {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "following reserved fields are not allowed %q", ReservedFields)
	}

	// check for language keywords
	if util.LanguageKeywords.Contains(strings.ToLower(f.FieldName)) {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, MsgFieldNameAsLanguageKeyword, f.FieldName)
	}

	// for array elements, items will have field name empty so skip the test for that.
	// make sure they start with [a-z], [A-Z], $, _ and can only contain [a-z], [A-Z], $, _, [0-9]
	if !isArrayElement && !ValidFieldNamePattern.MatchString(f.FieldName) {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, MsgFieldNameInvalidPattern, f.FieldName)
	}

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

func (f *Field) IsCompatible(f1 *Field) error {
	if f.DataType != f1.DataType {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "data type mismatch for field %q", f.FieldName)
	}

	if f.IsPrimaryKey() != f1.IsPrimaryKey() {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "primary key changes are not allowed %q", f.FieldName)
	}

	return nil
}

type QueryableField struct {
	FieldName     string
	InMemoryAlias string
	Faceted       bool
	Indexed       bool
	Sortable      bool
	DataType      FieldType
	SearchType    string
}

func NewQueryableField(name string, tigrisType FieldType) *QueryableField {
	q := &QueryableField{
		FieldName:  name,
		Indexed:    IndexableField(tigrisType),
		Faceted:    FacetableField(tigrisType),
		Sortable:   SortableField(tigrisType),
		SearchType: toSearchFieldType(tigrisType),
		DataType:   tigrisType,
	}

	if IsSearchID(name) {
		q.InMemoryAlias = ReservedFields[IdToSearchKey]
	} else {
		q.InMemoryAlias = name
	}
	return q
}

// InMemoryName returns key name that is used to index this field in the indexing store. For example, an "id" key is indexed with
// "_tigris_id" name.
func (q *QueryableField) InMemoryName() string {
	return q.InMemoryAlias
}

// Name returns the name of this field as defined in the schema.
func (q *QueryableField) Name() string {
	return q.FieldName
}

// ShouldPack returns true if we need to pack this field before sending to indexing store.
func (q *QueryableField) ShouldPack() bool {
	return !q.IsReserved() && (q.DataType == ArrayType || q.DataType == DateTimeType)
}

// IsReserved returns true if the queryable field is internal field.
func (q *QueryableField) IsReserved() bool {
	return IsReservedField(q.Name())
}

func BuildQueryableFields(fields []*Field) []*QueryableField {
	var queryableFields []*QueryableField

	for _, f := range fields {
		if f.DataType == ObjectType {
			queryableFields = append(queryableFields, buildQueryableForObject(f.FieldName, f.Fields)...)
		} else {
			queryableFields = append(queryableFields, buildQueryableField("", f))
		}
	}

	// Allowing metadata fields to be queryable. User provided reserved fields are rejected by FieldBuilder.
	queryableFields = append(queryableFields, NewQueryableField(ReservedFields[CreatedAt], DateTimeType))
	queryableFields = append(queryableFields, NewQueryableField(ReservedFields[UpdatedAt], DateTimeType))

	return queryableFields
}

func buildQueryableForObject(parent string, fields []*Field) []*QueryableField {
	var queryable []*QueryableField
	for _, nested := range fields {
		if nested.DataType != ObjectType {
			queryable = append(queryable, buildQueryableField(parent, nested))
		} else {
			queryable = append(queryable, buildQueryableForObject(parent+ObjFlattenDelimiter+nested.FieldName, nested.Fields)...)
		}
	}

	return queryable
}

func buildQueryableField(parent string, f *Field) *QueryableField {
	name := f.FieldName
	if len(parent) > 0 {
		name = parent + ObjFlattenDelimiter + f.FieldName
	}

	return NewQueryableField(name, f.Type())
}
