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
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

const (
	ObjFlattenDelimiter = "."
)

// DefaultCollection is used to represent a collection. The tenant in the metadata package is responsible for creating
// the collection.
type DefaultCollection struct {
	// Id is the dictionary encoded value for this collection.
	Id uint32
	// SchVer returns the schema version
	SchVer int32
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
	// CollectionType is the type of the collection. Only two types of collections are supported "messages" and "documents"
	CollectionType CollectionType
	// Track all the int64 paths in the collection. For example, if top level object has a int64 field then key would be
	// obj.fieldName so that caller can easily navigate to this field.
	Int64FieldsPath map[string]struct{}
	// PartitionFields are the fields that make up the partition key, if applicable to the collection.
	PartitionFields []*Field
	// This is the existing fields in search
	FieldsInSearch []tsApi.Field
}

type CollectionType string

const (
	DocumentsType CollectionType = "documents"
	TopicType     CollectionType = "topic"
)

func disableAdditionalProperties(properties map[string]*jsonschema.Schema) {
	for _, p := range properties {
		if len(p.Properties) > 0 {
			p.AdditionalProperties = false
			disableAdditionalProperties(p.Properties)
		}
	}
}

func NewDefaultCollection(name string, id uint32, schVer int, ctype CollectionType, factory *Factory, searchCollectionName string, fieldsInSearch []tsApi.Field) *DefaultCollection {
	url := name + ".json"
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft7 // Format is only working for draft7
	if err := compiler.AddResource(url, bytes.NewReader(factory.Schema)); err != nil {
		panic(err)
	}

	validator, err := compiler.Compile(url)
	if err != nil {
		panic(err)
	}

	// Tigris doesn't allow additional fields as part of the write requests. Setting it to false ensures strict
	// schema validation.
	validator.AdditionalProperties = false
	disableAdditionalProperties(validator.Properties)

	queryableFields := BuildQueryableFields(factory.Fields, fieldsInSearch)
	partitionFields := BuildPartitionFields(factory.Fields)

	d := &DefaultCollection{
		Id:              id,
		SchVer:          int32(schVer),
		Name:            name,
		Fields:          factory.Fields,
		Indexes:         factory.Indexes,
		Validator:       validator,
		Schema:          factory.Schema,
		Search:          buildSearchSchema(searchCollectionName, queryableFields),
		QueryableFields: queryableFields,
		CollectionType:  ctype,
		Int64FieldsPath: make(map[string]struct{}),
		PartitionFields: partitionFields,
		FieldsInSearch:  fieldsInSearch,
	}

	// set paths for int64 fields
	d.setInt64Fields("", d.Fields)

	return d
}

func (d *DefaultCollection) GetName() string {
	return d.Name
}

func (d *DefaultCollection) GetVersion() int32 {
	return d.SchVer
}

func (d *DefaultCollection) Type() CollectionType {
	return d.CollectionType
}

func (d *DefaultCollection) GetFields() []*Field {
	return d.Fields
}

func (d *DefaultCollection) GetIndexes() *Indexes {
	return d.Indexes
}

func (d *DefaultCollection) GetQueryableFields() []*QueryableField {
	return d.QueryableFields
}

func (d *DefaultCollection) GetQueryableField(name string) (*QueryableField, error) {
	for _, qf := range d.QueryableFields {
		if qf.Name() == name {
			return qf, nil
		}
	}
	return nil, errors.InvalidArgument("Field `%s` is not present in collection", name)
}

func (d *DefaultCollection) GetField(name string) *Field {
	for _, r := range d.Fields {
		if r.FieldName == name {
			return r
		}
	}

	return nil
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
			return errors.InvalidArgument("json schema validation failed for field '%s' reason '%s'", field, v.Causes[0].Message)
		}
	}

	return errors.InvalidArgument(err.Error())
}

func (d *DefaultCollection) SearchCollectionName() string {
	return d.Search.Name
}

func (d *DefaultCollection) GetInt64FieldsPath() map[string]struct{} {
	return d.Int64FieldsPath
}

func (d *DefaultCollection) setInt64Fields(parent string, fields []*Field) {
	for _, f := range fields {
		if len(f.Fields) > 0 {
			d.setInt64Fields(buildPath(parent, f.FieldName), f.Fields)
		}

		if f.DataType == Int64Type {
			d.Int64FieldsPath[buildPath(parent, f.FieldName)] = struct{}{}
		}
	}
}

func buildPath(parent string, field string) string {
	if len(parent) > 0 {
		if len(field) > 0 {
			parent = parent + "." + field
		}
		return parent
	} else {
		return field
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

func buildSearchSchema(name string, queryableFields []*QueryableField) *tsApi.CollectionSchema {
	ptrTrue, ptrFalse := true, false
	tsFields := make([]tsApi.Field, 0, len(queryableFields))
	for _, s := range queryableFields {
		tsFields = append(tsFields, tsApi.Field{
			Name:     s.Name(),
			Type:     s.SearchType,
			Facet:    &s.Faceted,
			Index:    &s.Indexed,
			Optional: &ptrTrue,
		})
		if s.InMemoryName() != s.Name() {
			// we are storing this field differently in in-memory store
			tsFields = append(tsFields, tsApi.Field{
				Name:     s.InMemoryName(),
				Type:     s.SearchType,
				Facet:    &s.Faceted,
				Index:    &s.Indexed,
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

func init() {
	jsonschema.Formats[FieldNames[ByteType]] = func(i interface{}) bool {
		if v, ok := i.(string); ok {
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

		return !(val < math.MinInt32 || val > math.MaxInt32)
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
	return 0, errors.InvalidArgument("expected integer but found %T", i)
}
