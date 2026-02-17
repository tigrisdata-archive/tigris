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
	tsApi "github.com/tigrisdata/typesense-go/typesense/api"
)

// SearchIndexState represents the search state of collection search.
type SearchIndexState uint8

const (
	UnknownSearchState SearchIndexState = iota
	SearchIndexActive
	SearchIndexWriteMode
	SearchIndexBuilding
	NoSearchIndex
)

var SearchIndexStateNames = [...]string{
	UnknownSearchState:   "Search Status Unknown",
	SearchIndexActive:    "Search Active",
	SearchIndexWriteMode: "Search Write Mode",
	SearchIndexBuilding:  "Search Building",
	NoSearchIndex:        "No Search Index",
}

type SearchSourceType string

const (
	// SearchSourceTigris is when the source type is Tigris for the search index.
	SearchSourceTigris SearchSourceType = "tigris"
	// SearchSourceExternal is when the source type is external for the search index.
	SearchSourceExternal SearchSourceType = "external"
)

type SearchSource struct {
	// Type is the source type i.e. either it is Tigris or the index will be maintained by the user.
	Type SearchSourceType `json:"type,omitempty"`
	// CollectionName is the source name i.e. collection name in case of Tigris otherwise it is optional.
	CollectionName string `json:"collection,omitempty"`
	// DatabaseBranch is in case the collection is part of a database branch. Only applicable if Type is Tigris.
	DatabaseBranch string `json:"branch,omitempty"`
}

type SearchJSONSchema struct {
	Name        string               `json:"title,omitempty"`
	Description string               `json:"description,omitempty"`
	Properties  jsoniter.RawMessage  `json:"properties,omitempty"`
	Source      *SearchSource        `json:"source,omitempty"`
	Options     *SearchSchemaOptions `json:"options,omitempty"`
}

type SearchSchemaOptions struct {
	TokenSeparators *[]string `json:"token_separators,omitempty"`
}

func (s *SearchSchemaOptions) GetTokenSeparators() []string {
	if s.TokenSeparators == nil {
		return make([]string, 0)
	}
	return *s.TokenSeparators
}

// SearchFactory is used as an intermediate step so that collection can be initialized with properly encoded values.
type SearchFactory struct {
	// Name is the index name.
	Name string
	// Fields are derived from the user schema.
	Fields []*Field
	// Schema is the raw JSON schema received
	Schema  jsoniter.RawMessage
	Sub     string
	Source  SearchSource
	Options SearchSchemaOptions
}

func (fb *FactoryBuilder) BuildSearch(index string, reqSchema jsoniter.RawMessage) (*SearchFactory, error) {
	fb.setBuilderForSearch()

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
	fields, err := fb.deserializeProperties(schema.Properties, nil, nil)
	if err != nil {
		return nil, err
	}

	var source SearchSource
	if schema.Source == nil {
		source = SearchSource{
			Type: SearchSourceExternal,
		}
		schema.Source = &source

		if searchSchema, err = jsoniter.Marshal(schema); err != nil {
			return nil, err
		}
	} else {
		source = *schema.Source
	}
	if schema.Source.Type == SearchSourceTigris && len(schema.Source.DatabaseBranch) == 0 {
		// we set main branch by default if branch is not explicitly provided
		schema.Source.DatabaseBranch = "main"
		if searchSchema, err = jsoniter.Marshal(schema); err != nil {
			return nil, err
		}
	}
	var schemaOptions SearchSchemaOptions
	if schema.Options != nil {
		schemaOptions = *schema.Options
	}

	factory := &SearchFactory{
		Name:    index,
		Fields:  fields,
		Schema:  searchSchema,
		Source:  source,
		Options: schemaOptions,
	}

	idFound := false
	for _, f := range factory.Fields {
		if f.FieldName == SearchId {
			idFound = true
			break
		}
	}
	if !idFound {
		factory.Fields = append(factory.Fields, &Field{
			FieldName: SearchId,
			DataType:  StringType,
		})
	}

	if fb.onUserRequest {
		if err = fb.validateSearchSchema(factory); err != nil {
			return nil, err
		}
	}

	return factory, nil
}

func (*FactoryBuilder) validateSearchSchema(factory *SearchFactory) error {
	if factory.Source.Type != SearchSourceExternal {
		return errors.InvalidArgument("unsupported index source '%s'", factory.Source.Type)
	}

	for _, f := range factory.Fields {
		if err := ValidateFieldAttributes(true, f); err != nil {
			return err
		}
	}

	return nil
}

// SearchIndex is to manage search index created by the user.
type SearchIndex struct {
	// Name is the name of the index.
	Name string
	// index version
	Version uint32
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
	// TokenSeparators is a list of symbols or special characters to be used for splitting the text into individual
	// words in addition to space and new-line characters.
	TokenSeparators []string
	// Source of this index
	Source        SearchSource
	SearchIDField *QueryableField
	// Track all the int64 paths in the collection. For example, if top level object has an int64 field then key would be
	// obj.fieldName so that caller can easily navigate to this field.
	int64FieldsPath *int64PathBuilder
}

func NewSearchIndex(ver uint32, searchStoreName string, factory *SearchFactory, fieldsInSearch []tsApi.Field) *SearchIndex {
	queryableFields := NewQueryableFieldsBuilder().BuildQueryableFields(factory.Fields, fieldsInSearch, true)

	var searchIdField *QueryableField
	for _, q := range queryableFields {
		if q.SearchIdField {
			searchIdField = q
		}
	}

	index := &SearchIndex{
		Version:         ver,
		Name:            factory.Name,
		Fields:          factory.Fields,
		Schema:          factory.Schema,
		Source:          factory.Source,
		TokenSeparators: factory.Options.GetTokenSeparators(),
		SearchIDField:   searchIdField,
		QueryableFields: queryableFields,
		int64FieldsPath: buildInt64Path(factory.Fields),
	}
	index.buildSearchSchema(searchStoreName)

	return index
}

func (s *SearchIndex) StoreIndexName() string {
	return s.StoreSchema.Name
}

func (s *SearchIndex) GetField(name string) *Field {
	for _, r := range s.Fields {
		if r.FieldName == name {
			return r
		}
	}

	return nil
}

func (s *SearchIndex) GetQueryableField(name string) (*QueryableField, error) {
	for _, qf := range s.QueryableFields {
		if qf.Name() == name {
			return qf, nil
		}
		for _, nested := range qf.AllowedNestedQFields {
			if nested.Name() == name {
				return nested, nil
			}
		}
	}
	return nil, errors.InvalidArgument("Field `%s` is not present in collection", name)
}

func (s *SearchIndex) Validate(doc map[string]any) error {
	for _, f := range s.QueryableFields {
		if f.DataType == VectorType {
			keys := f.KeyPath()
			value, ok := doc[keys[0]]
			if !ok {
				continue
			}

			field := GetField(s.Fields, keys[0])
			if field == nil {
				continue
			}

			if err := s.validateLow(value, keys[1:], field); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *SearchIndex) validateLow(value any, keys []string, field *Field) error {
	if len(keys) > 0 {
		nested := field.GetNestedField(keys[0])
		if conv, ok := value.(map[string]any); ok {
			return s.validateLow(conv[nested.FieldName], keys[1:], nested)
		}
	}
	if value == nil || field.DataType != VectorType {
		return nil
	}

	v, ok := value.([]any)
	if !ok {
		return errors.InvalidArgument("unable to convert vector type field '%s' to []float64", field.FieldName)
	}
	if len(v) > field.GetDimensions() {
		return errors.InvalidArgument("field '%s' of vector type should not have dimensions greater than the "+
			"defined in schema, defined: '%d' found: '%d'", field.FieldName, field.GetDimensions(), len(v))
	}

	return nil
}

func (s *SearchIndex) buildSearchSchema(name string) {
	ptrTrue, ptrFalse := true, false
	tsFields := make([]tsApi.Field, 0, len(s.QueryableFields))
	for _, s := range s.QueryableFields {
		tsFields = append(tsFields, tsApi.Field{
			Name:     s.Name(),
			Type:     s.SearchType,
			Facet:    &s.Faceted,
			Index:    &s.SearchIndexed,
			Sort:     &s.Sortable,
			Optional: &ptrTrue,
			NumDim:   s.Dimensions,
		})

		if s.InMemoryName() != s.Name() {
			// we are storing this field differently in in-memory store
			tsFields = append(tsFields, tsApi.Field{
				Name:     s.InMemoryName(),
				Type:     s.SearchType,
				Facet:    &s.Faceted,
				Index:    &s.SearchIndexed,
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

	s.StoreSchema = &tsApi.CollectionSchema{
		Name:   name,
		Fields: tsFields,
	}
	if len(s.TokenSeparators) > 0 {
		s.StoreSchema.TokenSeparators = &s.TokenSeparators
	}
}

func (s *SearchIndex) GetSearchDeltaFields(existingFields []*QueryableField, fieldsInSearch []tsApi.Field) []tsApi.Field {
	ptrTrue := true

	incomingQueryable := NewQueryableFieldsBuilder().BuildQueryableFields(s.Fields, fieldsInSearch, true)

	existingFieldMap := make(map[string]*QueryableField)
	for _, f := range existingFields {
		existingFieldMap[f.FieldName] = f
	}

	fieldsInSearchMap := make(map[string]tsApi.Field)
	for _, f := range fieldsInSearch {
		fieldsInSearchMap[f.Name] = f
	}

	tsFields := make([]tsApi.Field, 0, len(incomingQueryable))
	for _, f := range incomingQueryable {
		e := existingFieldMap[f.FieldName]
		delete(existingFieldMap, f.FieldName)

		if e != nil && f.SearchType == e.SearchType && f.SearchIndexed == e.SearchIndexed && f.Faceted == e.Faceted && f.Sortable == e.Sortable {
			continue
		}

		// attribute changed, drop the field first
		if e != nil {
			tsFields = append(tsFields, tsApi.Field{
				Name: f.FieldName,
				Drop: &ptrTrue,
			})
		} else {
			// this can happen if update request is timed out on Tigris side but succeed on search
			if _, found := fieldsInSearchMap[f.FieldName]; found {
				tsFields = append(tsFields, tsApi.Field{
					Name: f.FieldName,
					Drop: &ptrTrue,
				})
			}
		}

		// add new field
		tsFields = append(tsFields, tsApi.Field{
			Name:     f.FieldName,
			Type:     f.SearchType,
			Facet:    &f.Faceted,
			Index:    &f.SearchIndexed,
			Sort:     &f.Sortable,
			Optional: &ptrTrue,
			NumDim:   f.Dimensions,
		})
	}

	// drop fields non existing in new schema
	for _, f := range existingFieldMap {
		tsField := tsApi.Field{
			Name: f.FieldName,
			Drop: &ptrTrue,
		}

		tsFields = append(tsFields, tsField)
	}

	return tsFields
}

func (s *SearchIndex) GetInt64FieldsPath() map[string]struct{} {
	return s.int64FieldsPath.get()
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
	QueryableFields     []*QueryableField
	prevVersionInSearch []tsApi.Field
	// State will start tracking whether collection search index is active or not
	state SearchIndexState
}

func NewImplicitSearchIndex(name string, searchStoreName string, fields []*Field, prevVersionInSearch []tsApi.Field) *ImplicitSearchIndex {
	// this is created by collection so the forSearchIndex is false.
	queryableFields := NewQueryableFieldsBuilder().BuildQueryableFields(fields, prevVersionInSearch, false)
	index := &ImplicitSearchIndex{
		Name:                name,
		QueryableFields:     queryableFields,
		prevVersionInSearch: prevVersionInSearch,
	}

	index.buildSearchSchema(searchStoreName)

	return index
}

func (s *ImplicitSearchIndex) SetState(state SearchIndexState) {
	s.state = state
}

func (s *ImplicitSearchIndex) GetState() SearchIndexState {
	return s.state
}

func (s *ImplicitSearchIndex) HasUserTaggedSearchIndexes() bool {
	for _, s := range s.QueryableFields {
		if s.SearchIndexed {
			return true
		}
	}

	return false
}

func (s *ImplicitSearchIndex) StoreIndexName() string {
	return s.StoreSchema.Name
}

func (s *ImplicitSearchIndex) buildSearchSchema(searchStoreName string) {
	ptrTrue, ptrFalse := true, false
	tsFields := make([]tsApi.Field, 0, len(s.QueryableFields))

	for _, f := range s.QueryableFields {
		tsFields = append(tsFields, tsApi.Field{
			Name:     f.Name(),
			Type:     f.SearchType,
			Facet:    &f.Faceted,
			Index:    &f.SearchIndexed,
			Sort:     &f.Sortable,
			Optional: &ptrTrue,
			NumDim:   f.Dimensions,
		})

		if f.InMemoryName() != f.Name() {
			// we are storing this field differently in in-memory store
			tsFields = append(tsFields, tsApi.Field{
				Name:     f.InMemoryName(),
				Type:     f.SearchType,
				Facet:    &f.Faceted,
				Index:    &f.SearchIndexed,
				Sort:     &f.Sortable,
				Optional: &ptrTrue,
			})
		}
		// Save original date as string to disk
		if !f.IsReserved() && f.DataType == DateTimeType {
			tsFields = append(tsFields, tsApi.Field{
				Name:     ToSearchDateKey(f.Name()),
				Type:     toSearchFieldType(StringType, UnknownType),
				Facet:    &ptrFalse,
				Index:    &ptrFalse,
				Sort:     &ptrFalse,
				Optional: &ptrTrue,
			})
		}
	}

	s.StoreSchema = &tsApi.CollectionSchema{
		Name:   searchStoreName,
		Fields: tsFields,
	}
}

func (s *ImplicitSearchIndex) GetSearchDeltaFields(existingFields []*QueryableField, incomingFields []*Field) []tsApi.Field {
	ptrTrue := true

	incomingQueryable := NewQueryableFieldsBuilder().BuildQueryableFields(incomingFields, s.prevVersionInSearch, false)

	existingFieldMap := make(map[string]*QueryableField)
	for _, f := range existingFields {
		existingFieldMap[f.FieldName] = f
	}

	fieldsInSearchMap := make(map[string]tsApi.Field)
	for _, f := range s.prevVersionInSearch {
		fieldsInSearchMap[f.Name] = f
	}

	tsFields := make([]tsApi.Field, 0, len(incomingQueryable))
	for _, f := range incomingQueryable {
		e := existingFieldMap[f.FieldName]
		delete(existingFieldMap, f.FieldName)

		stateChanged := false
		if e != nil {
			inSearchState, found := fieldsInSearchMap[f.FieldName]
			if found && inSearchState.Index != nil && *inSearchState.Index != f.SearchIndexed {
				stateChanged = true
			}
			if found && inSearchState.Facet != nil && *inSearchState.Facet != f.Faceted {
				stateChanged = true
			}
			if found && inSearchState.Sort != nil && *inSearchState.Sort != f.Sortable {
				stateChanged = true
			}

			if !stateChanged {
				continue
			}
		}

		// attribute changed, drop the field first
		if e != nil && stateChanged {
			tsFields = append(tsFields, tsApi.Field{
				Name: f.FieldName,
				Drop: &ptrTrue,
			})
		} else {
			// this can happen if update request is timed out on Tigris side but succeed on search
			if _, found := fieldsInSearchMap[f.FieldName]; found {
				tsFields = append(tsFields, tsApi.Field{
					Name: f.FieldName,
					Drop: &ptrTrue,
				})
			}
		}

		// add new field
		tsFields = append(tsFields, tsApi.Field{
			Name:     f.FieldName,
			Type:     f.SearchType,
			Facet:    &f.Faceted,
			Index:    &f.SearchIndexed,
			Sort:     &f.Sortable,
			Optional: &ptrTrue,
			NumDim:   f.Dimensions,
		})
	}

	// drop fields non existing in new schema
	for _, f := range existingFieldMap {
		tsField := tsApi.Field{
			Name: f.FieldName,
			Drop: &ptrTrue,
		}

		tsFields = append(tsFields, tsField)
	}

	return tsFields
}
