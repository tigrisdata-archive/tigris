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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	jsoniter "github.com/json-iterator/go"
	"github.com/santhosh-tekuri/jsonschema/v5"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/lib/set"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

const (
	UserDefinedSchema   = "user_defined_schema"
	ObjFlattenDelimiter = "."
)

// DefaultCollection is used to represent a collection. The tenant in the metadata package is responsible for creating
// the collection.
type DefaultCollection struct {
	// Id is the dictionary encoded value for this collection.
	Id uint32
	// SchVer returns the schema version
	SchVer int
	// Name is the name of the collection.
	Name string
	// Fields are derived from the user schema.
	Fields []*Field
	// Indexes is a wrapper on the indexes part of this collection.
	Indexes *Indexes
	// Validator is used to validate the JSON document. As it is expensive to create this, it is only created once
	// during constructor of the collection.
	Validator *jsonschema.Schema
	// JSON schema
	Schema jsoniter.RawMessage
	// search schema
	Search *tsApi.CollectionSchema
	// QueryableFields are similar to Fields but these are flattened forms of fields. For instance, a simple field
	// will be one to one mapped to queryable field but complex fields like object type field there may be more than
	// one queryableFields. As queryableFields represent a flattened state these can be used as-is to index in memory.
	QueryableFields []*QueryableField
}

func NewDefaultCollection(name string, id uint32, schVer int, fields []*Field, indexes *Indexes, schema jsoniter.RawMessage, searchCollectionName string) *DefaultCollection {
	url := name + ".json"
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft7 // Format is only working for draft7
	if err := compiler.AddResource(url, bytes.NewReader(schema)); err != nil {
		panic(err)
	}

	validator, err := compiler.Compile(url)
	if err != nil {
		panic(err)
	}

	// Tigris doesn't allow additional fields as part of the write requests. Setting it to false ensures strict
	// schema validation.
	validator.AdditionalProperties = false

	queryableFields := buildQueryableFields(fields)

	return &DefaultCollection{
		Id:              id,
		SchVer:          schVer,
		Name:            name,
		Fields:          fields,
		Indexes:         indexes,
		Validator:       validator,
		Schema:          schema,
		Search:          buildSearchSchema(searchCollectionName, queryableFields),
		QueryableFields: queryableFields,
	}
}

func (d *DefaultCollection) GetName() string {
	return d.Name
}

func (d *DefaultCollection) Type() string {
	return UserDefinedSchema
}

func (d *DefaultCollection) GetFields() []*Field {
	return d.Fields
}

func (d *DefaultCollection) GetIndexes() *Indexes {
	return d.Indexes
}

// Validate expects an unmarshalled document which it will validate again the schema of this collection.
func (d *DefaultCollection) Validate(document interface{}) error {
	err := d.Validator.Validate(document)
	if err == nil {
		return nil
	}

	if v, ok := err.(*jsonschema.ValidationError); ok {
		if len(v.Causes) == 1 {
			field := v.Causes[0].InstanceLocation
			if len(field) > 0 && field[0] == '/' {
				field = field[1:]
			}
			return api.Errorf(api.Code_INVALID_ARGUMENT, "json schema validation failed for field '%s' reason '%s'", field, v.Causes[0].Message)
		}
	}

	return api.Errorf(api.Code_INVALID_ARGUMENT, err.Error())
}

func (d *DefaultCollection) SearchCollectionName() string {
	return d.Search.Name
}

func GetSearchDeltaFields(existingFields []*QueryableField, incomingFields []*Field) []tsApi.Field {
	incomingQueryable := buildQueryableFields(incomingFields)

	var existingFieldSet = set.New()
	for _, f := range existingFields {
		existingFieldSet.Insert(f.FieldName)
	}

	var ptrTrue = true
	var tsFields []tsApi.Field
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

func buildSearchSchema(name string, queryableFields []*QueryableField) *tsApi.CollectionSchema {
	var ptrTrue = true
	var tsFields []tsApi.Field
	for _, s := range queryableFields {
		tsFields = append(tsFields, tsApi.Field{
			Name:     s.FieldName,
			Type:     s.SearchType,
			Facet:    &s.Faceted,
			Index:    &s.Indexed,
			Optional: &ptrTrue,
		})
	}

	return &tsApi.CollectionSchema{
		Name:   name,
		Fields: tsFields,
	}
}

func init() {
	jsonschema.Formats[FieldNames[ByteType]] = func(i interface{}) bool {
		switch v := i.(type) {
		case string:
			_, err := base64.StdEncoding.DecodeString(v)
			return err == nil
		}
		return false
	}
	jsonschema.Formats[FieldNames[Int32Type]] = func(i interface{}) bool {
		val, err := parseInt(i)
		if err != nil {
			return false
		}

		if val < math.MinInt32 || val > math.MaxInt32 {
			return false
		}
		return true
	}
	jsonschema.Formats[FieldNames[Int64Type]] = func(i interface{}) bool {
		_, err := parseInt(i)
		return err == nil
	}
}

func parseInt(i interface{}) (int64, error) {
	switch i.(type) {
	case json.Number, float64, int, int32, int64:
		n, err := strconv.ParseInt(fmt.Sprint(i), 10, 64)
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	return 0, api.Errorf(api.Code_INVALID_ARGUMENT, "expected integer but found %T", i)
}
