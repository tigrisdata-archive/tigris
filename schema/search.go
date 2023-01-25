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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

type SearchSourceType string

const (
	// SearchSourceTigris is when the source type is Tigris for the search index.
	SearchSourceTigris SearchSourceType = "tigris"
	// SearchSourceUser is when the source type is user for the search index.
	SearchSourceUser SearchSourceType = "user"
)

type SearchSource struct {
	// Type is the source type i.e. either it is Tigris or the index will be maintained by the user.
	Type SearchSourceType `json:"type,omitempty"`
	// CollectionName is the source name i.e. collection name in case of Tigris otherwise it is optional.
	CollectionName string `json:"collection_name,omitempty"`
	// DatabaseBranch is in case the collection is part of a database branch. Only applicable if Type is Tigris.
	DatabaseBranch string `json:"database_branch,omitempty"`
}

type SearchJSONSchema struct {
	Name        string              `json:"title,omitempty"`
	Description string              `json:"description,omitempty"`
	Properties  jsoniter.RawMessage `json:"properties,omitempty"`
	Source      *SearchSource       `json:"source,omitempty"`
}

// SearchFactory is used as an intermediate step so that collection can be initialized with properly encoded values.
type SearchFactory struct {
	// Name is the index name.
	Name string
	// Fields are derived from the user schema.
	Fields []*Field
	// Schema is the raw JSON schema received
	Schema jsoniter.RawMessage
	Sub    string
	Source SearchSource
}

func BuildSearch(index string, reqSchema jsoniter.RawMessage) (*SearchFactory, error) {
	searchSchema := make([]byte, len(reqSchema))
	copy(searchSchema, reqSchema)

	schema := &SearchJSONSchema{}
	if err := jsoniter.Unmarshal(searchSchema, schema); err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, err.Error()).WithDetails(&api.ErrorDetails{
			Code:    api.Code_INTERNAL.String(),
			Message: fmt.Sprintf("schema: '%s', unmarshalling failed", string(searchSchema)),
		})
	}
	if len(schema.Properties) == 0 {
		return nil, errors.InvalidArgument("missing properties field in schema")
	}
	fields, err := deserializeProperties(schema.Properties, nil)
	if err != nil {
		return nil, err
	}

	var source SearchSource
	if schema.Source == nil {
		source = SearchSource{
			Type: SearchSourceUser,
		}
		schema.Source = &source

		if searchSchema, err = jsoniter.Marshal(schema); err != nil {
			return nil, err
		}
	} else {
		source = *schema.Source
	}

	factory := &SearchFactory{
		Name:   index,
		Fields: fields,
		Schema: searchSchema,
		Source: source,
	}

	return factory, nil
}

// SearchIndex is to manage search index created by the user.
type SearchIndex struct {
	// Name is the name of the index.
	Name string
	// index version
	Version int
	// Fields are derived from the user schema.
	Fields []*Field
	// JSON schema.
	Schema jsoniter.RawMessage
	// StoreSchema is the search schema of the underlying search engine.
	StoreSchema *tsApi.CollectionSchema
	// QueryableFields are similar to Fields but these are flattened forms of fields. For instance, a simple field
	// will be one to one mapped to queryable field but complex fields like object type field there may be more than
	// one queryableFields. As queryableFields represent a flattened state these can be used as-is to index in memory.
	QueryableFields []*QueryableField
	// Source of this index
	Source SearchSource
}

func NewSearchIndex(ver int, searchStoreName string, factory *SearchFactory, fieldsInSearch []tsApi.Field) *SearchIndex {
	queryableFields := BuildQueryableFields(factory.Fields, fieldsInSearch)
	queryableFields = append(queryableFields, NewQueryableField("id", StringType, UnknownType, nil, nil))

	return &SearchIndex{
		Version:         ver,
		Name:            factory.Name,
		Fields:          factory.Fields,
		Schema:          factory.Schema,
		QueryableFields: queryableFields,
		StoreSchema:     buildSearchSchema(searchStoreName, queryableFields),
		Source:          factory.Source,
	}
}

func (s *SearchIndex) StoreIndexName() string {
	return s.StoreSchema.Name
}

func buildSearchSchema(name string, queryableFields []*QueryableField) *tsApi.CollectionSchema {
	ptrTrue, ptrFalse := true, false
	tsFields := make([]tsApi.Field, 0, len(queryableFields))
	for _, s := range queryableFields {
		tsFields = append(tsFields, tsApi.Field{
			Name:     s.Name(),
			Type:     s.SearchType,
			Facet:    &s.Faceted,
			Index:    &s.Indexed,
			Sort:     &s.Sortable,
			Optional: &ptrTrue,
		})
		if s.InMemoryName() != s.Name() {
			// we are storing this field differently in in-memory store
			tsFields = append(tsFields, tsApi.Field{
				Name:     s.InMemoryName(),
				Type:     s.SearchType,
				Facet:    &s.Faceted,
				Index:    &s.Indexed,
				Sort:     &s.Sortable,
				Optional: &ptrTrue,
			})
		}
		// Save original date as string to disk
		if !s.IsReserved() && s.DataType == DateTimeType {
			tsFields = append(tsFields, tsApi.Field{
				Name:     ToSearchDateKey(s.Name()),
				Type:     toSearchFieldType(StringType, UnknownType),
				Facet:    &ptrFalse,
				Index:    &ptrFalse,
				Sort:     &ptrFalse,
				Optional: &ptrTrue,
			})
		}
	}

	return &tsApi.CollectionSchema{
		Name:   name,
		Fields: tsFields,
	}
}

func GetSearchDeltaFields(existingFields []*QueryableField, incomingFields []*Field, fieldsInSearch []tsApi.Field) []tsApi.Field {
	incomingQueryable := BuildQueryableFields(incomingFields, fieldsInSearch)

	existingFieldSet := container.NewHashSet()
	for _, f := range existingFields {
		existingFieldSet.Insert(f.FieldName)
	}

	ptrTrue := true
	tsFields := make([]tsApi.Field, 0, len(incomingQueryable))
	for _, f := range incomingQueryable {
		if existingFieldSet.Contains(f.FieldName) {
			continue
		}

		tsFields = append(tsFields, tsApi.Field{
			Name:     f.FieldName,
			Type:     f.SearchType,
			Facet:    &f.Faceted,
			Index:    &f.Indexed,
			Optional: &ptrTrue,
		})
	}

	return tsFields
}

// ImplicitSearchIndex is a search index that is automatically created by Tigris when a collection is created. Lifecycle
// of this index is tied to the collection.
type ImplicitSearchIndex struct {
	// Name is the name of the index.
	Name string
	// StoreSchema is the search schema of the underlying search engine.
	StoreSchema *tsApi.CollectionSchema
	// QueryableFields are similar to Fields but these are flattened forms of fields. For instance, a simple field
	// will be one to one mapped to queryable field but complex fields like object type field there may be more than
	// one queryableFields. As queryableFields represent a flattened state these can be used as-is to index in memory.
	QueryableFields []*QueryableField
}

func NewImplicitSearchIndex(name string, searchStoreName string, fields []*Field, fieldsInSearch []tsApi.Field) *ImplicitSearchIndex {
	queryableFields := BuildQueryableFields(fields, fieldsInSearch)
	return &ImplicitSearchIndex{
		Name:            name,
		QueryableFields: queryableFields,
		StoreSchema:     buildSearchSchema(searchStoreName, queryableFields),
	}
}

func (s *ImplicitSearchIndex) StoreIndexName() string {
	return s.StoreSchema.Name
}
