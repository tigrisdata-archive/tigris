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
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/util"
	tsApi "github.com/typesense/typesense-go/typesense/api"
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

func IndexableField(fieldType FieldType, subType FieldType) bool {
	switch fieldType {
	case BoolType, Int32Type, Int64Type, UUIDType, StringType, DateTimeType, DoubleType:
		return true
	case ArrayType:
		return IsPrimitiveType(subType)
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
	"default",
	"createdAt",
	"updatedAt",
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
	Sorted      *bool               `json:"sorted,omitempty"`
	Items       *FieldBuilder       `json:"items,omitempty"`
	Properties  jsoniter.RawMessage `json:"properties,omitempty"`
	Primary     *bool
	Partition   *bool
	Fields      []*Field
}

func (f *FieldBuilder) Validate(v []byte) error {
	var fieldProperties map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(v, &fieldProperties); err != nil {
		return err
	}

	for key := range fieldProperties {
		if !SupportedFieldProperties.Contains(key) {
			return errors.InvalidArgument("unsupported property found '%s'", key)
		}
	}

	return nil
}

func (f *FieldBuilder) Build(isArrayElement bool) (*Field, error) {
	if IsReservedField(f.FieldName) {
		return nil, errors.InvalidArgument("following reserved fields are not allowed %q", ReservedFields)
	}

	// check for language keywords
	if util.LanguageKeywords.Contains(strings.ToLower(f.FieldName)) {
		return nil, errors.InvalidArgument(MsgFieldNameAsLanguageKeyword, f.FieldName)
	}

	// for array elements, items will have field name empty so skip the test for that.
	// make sure they start with [a-z], [A-Z], $, _ and can only contain [a-z], [A-Z], $, _, [0-9]
	if !isArrayElement && !ValidFieldNamePattern.MatchString(f.FieldName) {
		return nil, errors.InvalidArgument(MsgFieldNameInvalidPattern, f.FieldName)
	}

	fieldType := ToFieldType(f.Type, f.Encoding, f.Format)
	if fieldType == UnknownType {
		if len(f.Encoding) > 0 {
			return nil, errors.InvalidArgument("unsupported encoding '%s'", f.Encoding)
		}
		if len(f.Format) > 0 {
			return nil, errors.InvalidArgument("unsupported format '%s'", f.Format)
		}

		return nil, errors.InvalidArgument("unsupported type detected '%s'", f.Type)
	}
	if f.Primary != nil && *f.Primary {
		// validate the primary key types
		if !IsValidKeyType(fieldType) {
			return nil, errors.InvalidArgument("unsupported primary key type detected '%s'", f.Type)
		}
	}
	if f.Primary == nil && f.Auto != nil && *f.Auto {
		return nil, errors.InvalidArgument("only primary fields can be set as auto-generated '%s'", f.FieldName)
	}

	field := &Field{}
	field.FieldName = f.FieldName
	field.MaxLength = f.MaxLength
	field.DataType = fieldType
	field.PrimaryKeyField = f.Primary
	field.PartitionKeyField = f.Partition
	field.Fields = f.Fields
	field.AutoGenerated = f.Auto
	field.Sorted = f.Sorted
	if f.CreatedAt != nil || f.UpdatedAt != nil || f.Default != nil {
		var err error
		if field.Defaulter, err = newDefaulter(f.CreatedAt, f.UpdatedAt, field.FieldName, field.DataType, f.Default); err != nil {
			return nil, err
		}
	}

	return field, nil
}

type Field struct {
	FieldName         string
	Defaulter         *FieldDefaulter
	DataType          FieldType
	MaxLength         *int32
	FillCreatedAt     *bool
	FillUpdatedAt     *bool
	UniqueKeyField    *bool
	PrimaryKeyField   *bool
	PartitionKeyField *bool
	AutoGenerated     *bool
	Sorted            *bool

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

func (f *Field) IsPartitionKey() bool {
	return f.PartitionKeyField != nil && *f.PartitionKeyField
}

func (f *Field) IsAutoGenerated() bool {
	return f.AutoGenerated != nil && *f.AutoGenerated
}

func (f *Field) IsSorted() bool {
	return f.Sorted != nil && *f.Sorted
}

func (f *Field) IsCompatible(f1 *Field) error {
	if f.DataType != f1.DataType {
		return errors.InvalidArgument("data type mismatch for field %q", f.FieldName)
	}

	if f.IsPrimaryKey() != f1.IsPrimaryKey() {
		return errors.InvalidArgument("primary key changes are not allowed %q", f.FieldName)
	}

	if f.MaxLength != nil && f1.MaxLength != nil {
		if *f.MaxLength > *f1.MaxLength {
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

type QueryableField struct {
	FieldName     string
	InMemoryAlias string
	Faceted       bool
	Indexed       bool
	Sortable      bool
	DataType      FieldType
	SubType       FieldType
	SearchType    string
	packThis      bool
}

func NewQueryableField(name string, tigrisType FieldType, subType FieldType, sorted *bool, fieldsInSearch []tsApi.Field) *QueryableField {
	var (
		searchType string
		indexed    *bool
		faceted    *bool
		sortable   = sorted
	)

	packThis := false
	if tigrisType == ArrayType {
		for _, fieldInSearch := range fieldsInSearch {
			if fieldInSearch.Name == name {
				searchType = fieldInSearch.Type
				if searchType == FieldNames[StringType] {
					packThis = true
				}

				indexed = fieldInSearch.Index
				faceted = fieldInSearch.Facet
				sortable = fieldInSearch.Sort
			}
		}
	}

	if len(searchType) == 0 {
		searchType = toSearchFieldType(tigrisType, subType)
	}
	if indexed == nil {
		shouldIndex := IndexableField(tigrisType, subType)
		indexed = &shouldIndex
	}
	if faceted == nil {
		shouldFacet := FacetableField(tigrisType)
		faceted = &shouldFacet
	}
	if sortable == nil {
		shouldSort := SortableField(tigrisType)
		sortable = &shouldSort
	}

	q := &QueryableField{
		FieldName:  name,
		Indexed:    *indexed,
		Faceted:    *faceted,
		Sortable:   *sortable,
		SearchType: searchType,
		DataType:   tigrisType,
		SubType:    subType,
		packThis:   packThis,
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
	if q.packThis {
		return true
	}

	if q.DataType == ArrayType && (q.SubType == ArrayType || q.SubType == ObjectType || q.SubType == UnknownType) {
		return true
	}
	return !q.IsReserved() && q.DataType == DateTimeType
}

// IsReserved returns true if the queryable field is internal field.
func (q *QueryableField) IsReserved() bool {
	return IsReservedField(q.Name())
}

func BuildQueryableFields(fields []*Field, fieldsInSearch []tsApi.Field) []*QueryableField {
	var queryableFields []*QueryableField

	for _, f := range fields {
		if f.DataType == ObjectType {
			queryableFields = append(queryableFields, buildQueryableForObject(f.FieldName, f.Fields, fieldsInSearch)...)
		} else {
			queryableFields = append(queryableFields, buildQueryableField("", f, fieldsInSearch))
		}
	}

	// Allowing metadata fields to be queryable. User provided reserved fields are rejected by FieldBuilder.
	queryableFields = append(queryableFields, NewQueryableField(ReservedFields[CreatedAt], DateTimeType, UnknownType, nil, fieldsInSearch))
	queryableFields = append(queryableFields, NewQueryableField(ReservedFields[UpdatedAt], DateTimeType, UnknownType, nil, fieldsInSearch))

	return queryableFields
}

func buildQueryableForObject(parent string, fields []*Field, fieldsInSearch []tsApi.Field) []*QueryableField {
	var queryable []*QueryableField
	for _, nested := range fields {
		if nested.DataType != ObjectType {
			queryable = append(queryable, buildQueryableField(parent, nested, fieldsInSearch))
		} else {
			queryable = append(queryable, buildQueryableForObject(parent+ObjFlattenDelimiter+nested.FieldName, nested.Fields, fieldsInSearch)...)
		}
	}

	return queryable
}

func buildQueryableField(parent string, f *Field, fieldsInSearch []tsApi.Field) *QueryableField {
	name := f.FieldName
	if len(parent) > 0 {
		name = parent + ObjFlattenDelimiter + f.FieldName
	}

	subType := UnknownType
	if f.DataType == ArrayType && len(f.Fields) > 0 {
		subType = f.Fields[0].DataType
	}

	return NewQueryableField(name, f.Type(), subType, f.Sorted, fieldsInSearch)
}

func BuildPartitionFields(fields []*Field) []*Field {
	var partitionFields []*Field

	for _, field := range fields {
		if field.IsPartitionKey() {
			partitionFields = append(partitionFields, field)
		}
	}

	return partitionFields
}
