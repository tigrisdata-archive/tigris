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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	jsoniter "github.com/json-iterator/go"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/tigrisdata/tigris/errors"
	tsApi "github.com/tigrisdata/typesense-go/typesense/api"
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
	SchVer uint32
	// Name is the name of the collection.
	Name string
	// EncodedName is the encoded name of the collection.
	EncodedName []byte
	// EncodedTableIndexName is the encoded name of the collection's Secondary Index.
	EncodedTableIndexName []byte
	// Fields are derived from the user schema.
	Fields []*Field
	// Indexes is a wrapper on the indexes part of this collection.
	// Primary Key contains the fields used to make up the primary key
	PrimaryKey *Index
	// Secondary SecondaryIndexes for this collection
	SecondaryIndexes *Indexes
	// Validator is used to validate the JSON document. As it is expensive to create this, it is only created once
	// during constructor of the collection.
	Validator *jsonschema.Schema
	// JSON schema
	Schema jsoniter.RawMessage
	// SchemaDeltas contains incompatible schema changes from version to version
	SchemaDeltas []VersionDelta
	// FieldVersions contains the list of schema versions at which the field had incompatible change
	FieldVersions map[string]*FieldVersions
	// ImplicitSearchIndex is created by the Tigris to use a search index for in-memory indexes. This is needed till we move
	// to secondary indexes which will be stored in FDB.
	ImplicitSearchIndex *ImplicitSearchIndex
	// search indexes are indexes that are explicitly created by the user and tagged Tigris as source. Collection will be
	// responsible for ensuring these indexes are in sync when any mutation happens to this collection.
	SearchIndexes map[string]*SearchIndex
	// QueryableFields are similar to Fields but these are flattened forms of fields. For instance, a simple field
	// will be one to one mapped to queryable field but complex fields like object type field there may be more than
	// one queryableFields. As queryableFields represent a flattened state these can be used as-is to index in memory.
	QueryableFields []*QueryableField
	// CollectionType is the type of the collection. Only two types of collections are supported "messages" and "documents"
	CollectionType CollectionType
	// Track all the int64 paths in the collection. For example, if top level object has an int64 field then key would be
	// obj.fieldName so that caller can easily navigate to this field.
	int64FieldsPath *int64PathBuilder
	// This is the existing fields in search
	FieldsInSearch []tsApi.Field

	fieldsWithInsertDefaults map[string]struct{}
	fieldsWithUpdateDefaults map[string]struct{}
}

type CollectionType string

const (
	DocumentsType CollectionType = "documents"
)

func (d *DefaultCollection) GetPrimaryKey() *Index {
	return d.PrimaryKey
}

func disableAdditionalPropertiesAndAllowNullable(required []string, properties map[string]*jsonschema.Schema) {
	for name, p := range properties {
		isRequired := false
		for _, r := range required {
			if r == name {
				isRequired = true
				break
			}
		}
		if isRequired {
			continue
		}

		// add additional null types so that validation can succeed if fields are explicitly set as null
		if len(p.Types) == 1 {
			switch p.Types[0] {
			case "string", "number", "object", "integer", "boolean":
				p.Types = append(p.Types, "null")
			case "array":
				p.Types = append(p.Types, "null")
				if items, ok := p.Items.(*jsonschema.Schema); ok {
					if len(items.Properties) == 0 {
						items.Types = append(items.Types, "null")
					} else {
						for _, itemsP := range items.Properties {
							switch itemsP.Types[0] {
							case "string", "number", "object", "integer", "boolean":
								itemsP.Types = append(itemsP.Types, "null")
							case "array":
								if itemsA, ok := itemsP.Items.(*jsonschema.Schema); ok {
									if len(itemsA.Properties) == 0 {
										items.Types = append(items.Types, "null")
									} else {
										if itemsA.AdditionalProperties == nil {
											itemsA.AdditionalProperties = false
										}
										disableAdditionalPropertiesAndAllowNullable(itemsA.Required, itemsA.Properties)
									}
								}
							}
							if len(itemsP.Properties) > 0 {
								if itemsP.AdditionalProperties == nil {
									itemsP.AdditionalProperties = false
								}
								disableAdditionalPropertiesAndAllowNullable(itemsP.Required, itemsP.Properties)
							}
						}
					}
				}
			}
		}

		// Enforce object schema if properties are specified and no additionalProperties explicitly set
		if len(p.Properties) > 0 {
			if p.AdditionalProperties == nil {
				p.AdditionalProperties = false
			}
			disableAdditionalPropertiesAndAllowNullable(p.Required, p.Properties)
		}
	}
}

func NewDefaultCollection(id uint32, schVer uint32, factory *Factory, schemas Versions,
	implicitSearchIndex *ImplicitSearchIndex,
) (*DefaultCollection, error) {
	url := factory.Name + ".json"
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft7 // Format is only working for draft7
	if err := compiler.AddResource(url, bytes.NewReader(factory.Schema)); err != nil {
		return nil, err
	}

	validator := compiler.MustCompile(url)

	// Tigris doesn't allow additional fields as part of the write requests. Setting it to false ensures strict
	// schema validation.
	if validator.AdditionalProperties == nil {
		validator.AdditionalProperties = false
	}
	disableAdditionalPropertiesAndAllowNullable(validator.Required, validator.Properties)

	var prevVersionInSearch []tsApi.Field
	if implicitSearchIndex != nil {
		prevVersionInSearch = implicitSearchIndex.prevVersionInSearch
	}
	queryableFields := NewQueryableFieldsBuilder().BuildQueryableFields(factory.Fields, prevVersionInSearch, factory.Indexes.IndexMetadata)

	schemaDeltas, err := buildSchemaDeltas(schemas)
	if err != nil {
		return nil, err
	}

	fieldVersions := buildFieldVersions(schemaDeltas)

	d := &DefaultCollection{
		Id:                       id,
		SchVer:                   schVer,
		Name:                     factory.Name,
		Fields:                   factory.Fields,
		PrimaryKey:               factory.PrimaryKey,
		SecondaryIndexes:         factory.Indexes,
		Validator:                validator,
		Schema:                   factory.Schema,
		QueryableFields:          queryableFields,
		CollectionType:           factory.CollectionType,
		ImplicitSearchIndex:      implicitSearchIndex,
		fieldsWithInsertDefaults: make(map[string]struct{}),
		fieldsWithUpdateDefaults: make(map[string]struct{}),
		SearchIndexes:            make(map[string]*SearchIndex),
		SchemaDeltas:             schemaDeltas,
		FieldVersions:            fieldVersions,
		int64FieldsPath:          buildInt64Path(factory.Fields),
	}

	// set fieldDefaulter for default fields
	d.setFieldsForDefaults("", d.Fields)

	return d, nil
}

func (d *DefaultCollection) AddSearchIndex(index *SearchIndex) {
	d.SearchIndexes[index.Name] = index
}

func (d *DefaultCollection) GetName() string {
	return d.Name
}

// SecondaryIndexKeyword is the subspace within a collection where the secondary index information
// is stored.
func (*DefaultCollection) SecondaryIndexKeyword() string {
	return "skey"
}

func (d *DefaultCollection) SecondaryIndexMetadata() bool {
	return d.SecondaryIndexes.IndexMetadata
}

func (d *DefaultCollection) GetVersion() uint32 {
	return d.SchVer
}

func (d *DefaultCollection) Type() CollectionType {
	return d.CollectionType
}

func (d *DefaultCollection) GetFields() []*Field {
	return d.Fields
}

func (d *DefaultCollection) GetIndexes() *Indexes {
	return d.SecondaryIndexes
}

func (d *DefaultCollection) GetQueryableFields() []*QueryableField {
	return d.QueryableFields
}

// GetActiveIndexedFields returns indexes that can be used for queries.
func (d *DefaultCollection) GetActiveIndexedFields() []*QueryableField {
	var indexed []*QueryableField
	for _, q := range d.QueryableFields {
		if q.Indexed && d.SecondaryIndexes.IsActiveIndex(q.FieldName) {
			indexed = append(indexed, q)
		}
	}
	return indexed
}

func (d *DefaultCollection) GetWriteModeIndexes() []*QueryableField {
	var indexed []*QueryableField
	for _, q := range d.QueryableFields {
		if q.Indexed && !d.SecondaryIndexes.IsActiveIndex(q.FieldName) {
			indexed = append(indexed, q)
		}
	}
	return indexed
}

func (d *DefaultCollection) GetIndexedFields() []*QueryableField {
	var indexed []*QueryableField
	for _, q := range d.QueryableFields {
		if q.Indexed {
			indexed = append(indexed, q)
		}
	}
	return indexed
}

func (d *DefaultCollection) GetPrimaryIndexedFields() []*QueryableField {
	var indexed []*QueryableField
	for _, q := range d.QueryableFields {
		if q.PrimaryIndexed {
			indexed = append(indexed, q)
		}
	}
	return indexed
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

func (d *DefaultCollection) GetSearchState() SearchIndexState {
	return d.ImplicitSearchIndex.GetState()
}

// Validate expects an unmarshalled document which it will validate again the schema of this collection.
func (d *DefaultCollection) Validate(document any) error {
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

func (d *DefaultCollection) GetImplicitSearchIndex() *ImplicitSearchIndex {
	return d.ImplicitSearchIndex
}

func (d *DefaultCollection) GetInt64FieldsPath() map[string]struct{} {
	return d.int64FieldsPath.get()
}

func (d *DefaultCollection) TaggedDefaultsForInsert() map[string]struct{} {
	return d.fieldsWithInsertDefaults
}

func (d *DefaultCollection) TaggedDefaultsForUpdate() map[string]struct{} {
	return d.fieldsWithUpdateDefaults
}

func (d *DefaultCollection) setFieldsForDefaults(parent string, fields []*Field) {
	for _, f := range fields {
		if len(f.Fields) > 0 {
			d.setFieldsForDefaults(buildPath(parent, f.FieldName), f.Fields)
		}

		if f.Defaulter != nil {
			if f.Defaulter.TaggedWithUpdatedAt() {
				d.fieldsWithUpdateDefaults[buildPath(parent, f.FieldName)] = struct{}{}
			} else {
				d.fieldsWithInsertDefaults[buildPath(parent, f.FieldName)] = struct{}{}
			}
		}
	}
}

func buildPath(parent string, field string) string {
	if len(parent) > 0 {
		if len(field) > 0 {
			parent = parent + "." + field
		}
		return parent
	}

	return field
}

func init() {
	jsonschema.Formats[FieldNames[ByteType]] = func(i any) bool {
		if i == nil {
			return true
		}

		if v, ok := i.(string); ok {
			_, err := base64.StdEncoding.DecodeString(v)
			return err == nil
		}
		return false
	}
	jsonschema.Formats[FieldNames[Int32Type]] = func(i any) bool {
		if i == nil {
			return true
		}

		val, err := parseInt(i)
		if err != nil {
			return false
		}

		return !(val < math.MinInt32 || val > math.MaxInt32)
	}
	jsonschema.Formats[FieldNames[Int64Type]] = func(i any) bool {
		if i == nil {
			return true
		}

		_, err := parseInt(i)
		return err == nil
	}
}

func parseInt(i any) (int64, error) {
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

type int64PathBuilder struct {
	int64FieldsPath map[string]struct{}
}

func buildInt64Path(fields []*Field) *int64PathBuilder {
	i := &int64PathBuilder{
		int64FieldsPath: make(map[string]struct{}),
	}
	i.buildInt64PathLow("", fields)

	return i
}

func (builder *int64PathBuilder) buildInt64PathLow(parent string, fields []*Field) {
	for _, f := range fields {
		if len(f.Fields) > 0 {
			builder.buildInt64PathLow(buildPath(parent, f.FieldName), f.Fields)
		}

		if f.DataType == Int64Type {
			builder.int64FieldsPath[buildPath(parent, f.FieldName)] = struct{}{}
		}
	}
}

func (builder *int64PathBuilder) get() map[string]struct{} {
	return builder.int64FieldsPath
}
