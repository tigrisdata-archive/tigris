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

import "fmt"

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
	Fields []*Field
	Name   string
	Id     uint32
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
}

func NewDefaultCollection(cname string, id uint32, fields []*Field, indexes *Indexes) *DefaultCollection {
	return &DefaultCollection{
		Id:      id,
		Name:    cname,
		Fields:  fields,
		Indexes: indexes,
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
