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
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

const (
	UserDefinedSchema = "user_defined_schema"
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

// Index can be composite so it has a list of fields, each index has name and encoded id. The encoded is used for key
// building.
type Index struct {
	// Fields that are part of this index. An index can have a single or composite fields.
	Fields []*Field
	// Name is used by dictionary encoder for this index.
	Name string
	// Id is assigned to this index by the dictionary encoder.
	Id uint32
}

// DefaultCollection is used to represent a collection. The tenant in the metadata package is responsible for creating
// the collection.
type DefaultCollection struct {
	// Id is the dictionary encoded value for this collection.
	Id uint32
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
	SearchSchema *tsApi.CollectionSchema
}

func NewDefaultCollection(cname string, id uint32, fields []*Field, indexes *Indexes, schema jsoniter.RawMessage, searchCollectionName string) *DefaultCollection {
	url := cname + ".json"
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft7 // Format is only working for draft7
	if err := compiler.AddResource(url, bytes.NewReader(schema)); err != nil {
		panic(err)
	}

	validator, err := compiler.Compile(url)
	if err != nil {
		panic(err)
	}
	// this is to not support additional properties, this is intentional to avoid caller not passing additional properties
	// flag. Later probably we can relax it. Starting with strict validation is better than not validating extra keys.
	validator.AdditionalProperties = false

	search := buildSearchSchema(searchCollectionName, fields)

	return &DefaultCollection{
		Id:           id,
		Name:         cname,
		Fields:       fields,
		Indexes:      indexes,
		Validator:    validator,
		Schema:       schema,
		SearchSchema: search,
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
	return d.SearchSchema.Name
}

func buildSearchSchema(name string, fields []*Field) *tsApi.CollectionSchema {
	var searchFields []tsApi.Field

	var ptrTrue = true
	for _, f := range fields {
		if f.Type() == ArrayType || f.Type() == ObjectType {
			continue
		}

		var tsField tsApi.Field
		switch f.DataType {
		case StringType:
			tsField = tsApi.Field{
				Name:     f.FieldName,
				Facet:    &ptrTrue,
				Type:     FieldNames[StringType],
				Optional: &ptrTrue,
			}
		case ByteType, UUIDType, DateTimeType:
			tsField = tsApi.Field{
				Name:     f.FieldName,
				Type:     FieldNames[StringType],
				Optional: &ptrTrue,
			}
		case Int32Type, Int64Type:
			tsField = tsApi.Field{
				Name:     f.FieldName,
				Type:     FieldNames[f.DataType],
				Facet:    &ptrTrue,
				Optional: &ptrTrue,
			}
		case DoubleType:
			tsField = tsApi.Field{
				Name:     f.FieldName,
				Type:     "float",
				Facet:    &ptrTrue,
				Optional: &ptrTrue,
			}
		default:
			tsField = tsApi.Field{
				Name:     f.FieldName,
				Type:     FieldNames[f.DataType],
				Optional: &ptrTrue,
			}
		}
		searchFields = append(searchFields, tsField)
	}

	return &tsApi.CollectionSchema{
		Name:   name,
		Fields: searchFields,
	}
}

func init() {
	jsonschema.Formats[FieldNames[ByteType]] = func(i interface{}) bool {
		switch i.(type) {
		case string:
			_, err := base64.StdEncoding.DecodeString(i.(string))
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
		val, err := parseInt(i)
		if err != nil {
			return false
		}

		if val < math.MinInt64 || val > math.MaxInt64 {
			return false
		}
		return true
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
