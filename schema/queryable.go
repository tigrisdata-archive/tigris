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
	"strings"

	"github.com/tigrisdata/tigris/internal"
)

// QueryableField is internal structure used after flattening the fields i.e. the representation of the queryable field
// is of the following form "field" OR "parent.field". This allows us to perform look faster by just checking in this
// structure. Object that doesn't have any nested fields will be same as any other top level field.
type QueryableField struct {
	FieldName     string
	Indexed       bool // Secondary Index
	InMemoryAlias string
	Faceted       bool
	SearchIndexed bool
	Sortable      bool
	DataType      FieldType
	SubType       FieldType
	SearchType    string
	packThis      bool
	DoNotFlatten  bool
	Dimensions    int32
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

// Type returns the data type of this field.
func (q *QueryableField) Type() FieldType {
	return q.DataType
}

// ShouldPack returns true if we need to pack this field before sending to indexing store.
func (q *QueryableField) ShouldPack() bool {
	if q.packThis {
		return true
	}

	if q.DataType == ArrayType && (q.SubType == ArrayType || q.SubType == UnknownType) {
		return true
	}
	return !q.IsReserved() && q.DataType == DateTimeType
}

// IsReserved returns true if the queryable field is internal field.
func (q *QueryableField) IsReserved() bool {
	return IsReservedField(q.Name())
}

func (q *QueryableField) KeyPath() []string {
	return strings.Split(q.FieldName, ".")
}

type QueryableFieldsBuilder struct{}

func NewQueryableFieldsBuilder() *QueryableFieldsBuilder {
	return &QueryableFieldsBuilder{}
}

func (builder *QueryableFieldsBuilder) NewQueryableField(name string, f *Field, fieldsInSearch []internal.SearchField) *QueryableField {
	var (
		searchType    string
		faceted       = f.Faceted
		sortable      = f.Sorted
		searchIndexed = f.SearchIndexed
	)

	subType := UnknownType
	if f.DataType == ArrayType && len(f.Fields) > 0 {
		subType = f.Fields[0].DataType
	}

	packThis := false
	if f.DataType == ArrayType || f.DataType == ObjectType {
		for _, fieldInSearch := range fieldsInSearch {
			if fieldInSearch.Name == name {
				searchType = fieldInSearch.Type
				if searchType == FieldNames[StringType] {
					packThis = true

					// honor whatever we have in search for this case, these are very old collection when we were
					// packing arrays. Probably not been in use.
					searchIndexed = fieldInSearch.Index
					faceted = fieldInSearch.Facet
					sortable = fieldInSearch.Sort
				}
			}
		}
	}

	if len(searchType) == 0 {
		searchType = toSearchFieldType(f.DataType, subType)
	}

	q := &QueryableField{
		FieldName:  name,
		SearchType: searchType,
		DataType:   f.DataType,
		SubType:    subType,
		packThis:   packThis,
		Indexed:    f.IsIndexable(),
	}

	if f.Dimensions != nil {
		q.Dimensions = *f.Dimensions
	}
	if searchIndexed != nil && *searchIndexed {
		q.SearchIndexed = true
	}
	if sortable != nil && *sortable {
		q.Sortable = true
	}
	if faceted != nil && *faceted {
		q.Faceted = true
	}

	if IsSearchID(name) {
		q.InMemoryAlias = ReservedFields[IdToSearchKey]
	} else {
		q.InMemoryAlias = name
	}
	return q
}

func (builder *QueryableFieldsBuilder) BuildQueryableFields(fields []*Field, fieldsInSearch []internal.SearchField) []*QueryableField {
	var queryableFields []*QueryableField

	for _, f := range fields {
		if f.DataType == ObjectType {
			if len(f.Fields) == 0 {
				ff := builder.buildQueryableField("", f, fieldsInSearch)
				ff.DoNotFlatten = true
				queryableFields = append(queryableFields, ff)
				continue
			}
			queryableFields = append(queryableFields, builder.buildQueryableForObject(f.FieldName, f.Fields, fieldsInSearch)...)
		} else {
			queryableFields = append(queryableFields, builder.buildQueryableField("", f, fieldsInSearch))
		}
	}

	ptrTrue := true
	// Allowing metadata fields to be queryable. User provided reserved fields are rejected by FieldBuilder.
	queryableFields = append(queryableFields, builder.NewQueryableField(ReservedFields[CreatedAt], &Field{
		FieldName:     ReservedFields[CreatedAt],
		DataType:      DateTimeType,
		Sorted:        &ptrTrue,
		SearchIndexed: &ptrTrue,
		Indexed:       &ptrTrue,
	}, fieldsInSearch))

	queryableFields = append(queryableFields, builder.NewQueryableField(ReservedFields[UpdatedAt], &Field{
		FieldName:     ReservedFields[UpdatedAt],
		DataType:      DateTimeType,
		Sorted:        &ptrTrue,
		SearchIndexed: &ptrTrue,
		Indexed:       &ptrTrue,
	}, fieldsInSearch))

	return queryableFields
}

func (builder *QueryableFieldsBuilder) buildQueryableForObject(parent string, fields []*Field, fieldsInSearch []internal.SearchField) []*QueryableField {
	var queryable []*QueryableField
	for _, nested := range fields {
		if nested.DataType == ObjectType {
			if len(nested.Fields) == 0 {
				queryable = append(queryable, builder.buildQueryableField(parent, nested, fieldsInSearch))
				continue
			}
			queryable = append(queryable, builder.buildQueryableForObject(parent+ObjFlattenDelimiter+nested.FieldName, nested.Fields, fieldsInSearch)...)
		} else {
			queryable = append(queryable, builder.buildQueryableField(parent, nested, fieldsInSearch))
		}
	}

	return queryable
}

func (builder *QueryableFieldsBuilder) buildQueryableField(parent string, f *Field, fieldsInSearch []internal.SearchField) *QueryableField {
	name := f.FieldName
	if len(parent) > 0 {
		name = parent + ObjFlattenDelimiter + f.FieldName
	}

	return builder.NewQueryableField(name, f, fieldsInSearch)
}
