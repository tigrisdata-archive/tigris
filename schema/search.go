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

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
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
	Type string `json:"type,omitempty"`
	// Name is the source name i.e. collection name in case of Tigris otherwise it is optional.
	Name string `json:"name,omitempty"`
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
	Source *SearchSource
}

func BuildSearch(index string, reqSchema jsoniter.RawMessage) (*SearchFactory, error) {
	var err error
	err = jsonparser.ObjectEach(reqSchema, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		switch string(key) {
		case "description", "title", "properties", "source":
		default:
			return errors.InvalidArgument("invalid key found '%s'", string(key))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	schema := &SearchJSONSchema{}
	if err := jsoniter.Unmarshal(reqSchema, schema); err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, err.Error()).WithDetails(&api.ErrorDetails{
			Code:    api.Code_INTERNAL.String(),
			Message: fmt.Sprintf("schema: '%s', unmarshalling failed", string(reqSchema)),
		})
	}
	if len(schema.Properties) == 0 {
		return nil, errors.InvalidArgument("missing properties field in schema")
	}
	fields, err := deserializeProperties(schema.Properties, nil)
	if err != nil {
		return nil, err
	}

	factory := &SearchFactory{
		Name:   index,
		Fields: fields,
		Schema: reqSchema,
		Source: schema.Source,
	}

	return factory, nil
}

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
}

func NewSearchIndex(ver int, searchStoreName string, factory *SearchFactory, previous []tsApi.Field) *SearchIndex {
	queryableFields := BuildQueryableFields(factory.Fields, previous)

	return &SearchIndex{
		Version:         ver,
		Name:            factory.Name,
		Fields:          factory.Fields,
		Schema:          factory.Schema,
		QueryableFields: queryableFields,
		StoreSchema:     buildSearchSchema(searchStoreName, queryableFields),
	}
}
