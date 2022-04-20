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
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/santhosh-tekuri/jsonschema/v5"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"google.golang.org/grpc/codes"
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
}

func NewDefaultCollection(cname string, id uint32, fields []*Field, indexes *Indexes, schema jsoniter.RawMessage) *DefaultCollection {
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

	return &DefaultCollection{
		Id:        id,
		Name:      cname,
		Fields:    fields,
		Indexes:   indexes,
		Validator: validator,
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
			return api.Errorf(codes.InvalidArgument, "json schema validation failed for field '%s' reason '%s'", field, v.Causes[0].Message)
		}
	}

	return api.Errorf(codes.InvalidArgument, err.Error())
}

type Collection interface {
	Name() string
	Type() string
	Database() string
	GetFields() []*Field
	PrimaryKeys() []*Field
	StorageName() string // ToDo: this is a placeholder, will be replaced by encoding package
}

type SimpleCollection struct {
	CollectionName string
	DatabaseName   string
	Keys           []*Field
	Fields         []*Field
}

func NewCollection(database string, collection string, fields []*Field, keys []*Field) Collection {
	return &SimpleCollection{
		DatabaseName:   database,
		CollectionName: collection,
		Keys:           keys,
		Fields:         fields,
	}
}

func (s *SimpleCollection) Name() string {
	return s.CollectionName
}

func (s *SimpleCollection) Type() string {
	return UserDefinedSchema
}

func (s *SimpleCollection) Database() string {
	return s.DatabaseName
}

func (s *SimpleCollection) GetFields() []*Field {
	return s.Fields
}

func (s *SimpleCollection) PrimaryKeys() []*Field {
	return s.Keys
}

func (s *SimpleCollection) StorageName() string {
	return fmt.Sprintf("%s.%s", s.DatabaseName, s.CollectionName)
}

func GetIndexName(database, collection, index string) string {
	return fmt.Sprintf("%s.%s.%s", database, collection, index)
}

func GetCollectionName(database, collection string) string {
	return fmt.Sprintf("%s.%s", database, collection)
}
