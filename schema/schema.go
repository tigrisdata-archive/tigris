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
	"strings"
	"time"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
	langSchema "github.com/tigrisdata/tigris/schema/lang"
	ulog "github.com/tigrisdata/tigris/util/log"
)

/**
A sample user JSON schema looks like below,
{
	"title": "Record of an order",
	"description": "This document records the details of an order",
	"properties": {
		"order_id": {
			"description": "A unique identifier for an order",
			"type": "integer"
		},
		"cust_id": {
			"description": "A unique identifier for a customer",
			"type": "integer"
		},
		"product": {
			"description": "name of the product",
			"type": "string",
			"max_length": 100,
		},
		"quantity": {
			"description": "number of products ordered",
			"type": "integer"
		},
		"price": {
			"description": "price of the product",
			"type": "number"
		},
		"date_ordered": {
			"description": "The date order was made",
			"type": "string",
			"format": "date-time"
		},
		"array_field": {
			"items": {
				"type": "string",
   			}
			"description": "Array field description",
			"type": "array",
		},
		"array_of_objects": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"field1": {
						"type": "integer"
					}
				}
   			}
		},
		"array_nested": {
			"type": "array",
			"items": {
				"type": "array",
				"items": {
					"type": "array",
					"items": {
						"type": "string"
					}
				}
   			}
		}
	},
	"primary_key": [
		"cust_id",
		"order_id"
	]
}
*/

const (
	PrimaryKeyIndexName = "pkey"
	AutoPrimaryKeyF     = "id"
	PrimaryKeySchemaK   = "primary_key"
	// DateTimeFormat represents the supported date time format.
	DateTimeFormat               = time.RFC3339Nano
	CollectionTypeF              = "collection_type"
	IndexingSchemaVersionKey     = "indexing_version"
	DefaultIndexingSchemaVersion = "v1"

	SecondaryKeyIndexName = "skey"
)

type JSONSchema struct {
	Name            string              `json:"title,omitempty"`
	Description     string              `json:"description,omitempty"`
	Properties      jsoniter.RawMessage `json:"properties,omitempty"`
	PrimaryKeys     []string            `json:"primary_key,omitempty"`
	CollectionType  string              `json:"collection_type,omitempty"`
	IndexingVersion string              `json:"indexing_version,omitempty"`
	Version         int32               `json:"version,omitempty"`
}

// Factory is used as an intermediate step so that collection can be initialized with properly encoded values.
type Factory struct {
	// Name is the collection name of this schema.
	Name string
	// Fields are derived from the user schema.
	Fields []*Field
	// Primary Key points to the fields used for the primary index. At this point the dictionary encoded value is not
	// set for these indexes which is set as part of collection creation.
	PrimaryKey *Index
	// Indexes is a wrapper on the secondary indexes part of this collection.
	Indexes *Indexes
	// Schema is the raw JSON schema received as part of CreateOrUpdateCollection request. This is stored as-is in the
	// schema subspace.
	Schema jsoniter.RawMessage
	// CollectionType is the type of the collection. Only two types of collections are supported "messages" and "documents"
	CollectionType  CollectionType
	IndexingVersion string
	Version         int32
}

func (f *Factory) SecondaryIndexes() []*Index {
	return f.Indexes.All
}

func RemoveIndexingVersion(schema jsoniter.RawMessage) jsoniter.RawMessage {
	if v, _, _, _ := jsonparser.Get(schema, IndexingSchemaVersionKey); len(v) > 0 {
		return jsonparser.Delete(schema, IndexingSchemaVersionKey)
	}
	return schema
}

func SetIndexingVersion(factory *Factory) error {
	// take a copy, so modification is not touching the existing req payload schema
	tmp := make([]byte, len(factory.Schema))
	copy(tmp, factory.Schema)
	factory.Schema = tmp

	if _, dt, _, _ := jsonparser.Get(factory.Schema, IndexingSchemaVersionKey); dt == jsonparser.NotExist {
		var err error
		var schema jsoniter.RawMessage
		if schema, err = jsonparser.Set(factory.Schema, []byte(fmt.Sprintf(`"%s"`, DefaultIndexingSchemaVersion)), IndexingSchemaVersionKey); err != nil {
			return err
		}

		factory.Schema = schema
		factory.IndexingVersion = DefaultIndexingSchemaVersion
	}
	return nil
}

func GetCollectionType(_ jsoniter.RawMessage) (CollectionType, error) {
	return DocumentsType, nil
}

// Build is used to deserialize the user json schema into a schema factory.
func (fb *FactoryBuilder) Build(collection string, reqSchema jsoniter.RawMessage) (*Factory, error) {
	cType, err := GetCollectionType(reqSchema)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, err.Error()).WithDetails(&api.ErrorDetails{
			Code:    api.Code_INTERNAL.String(),
			Message: fmt.Sprintf("schema: '%s', failed getting collection typ", string(reqSchema)),
		})
	}

	if reqSchema, err = setPrimaryKey(reqSchema, jsonSpecFormatUUID, true); err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, err.Error()).WithDetails(&api.ErrorDetails{
			Code:    api.Code_INTERNAL.String(),
			Message: fmt.Sprintf("schema: '%s', setting primary key failed", string(reqSchema)),
		})
	}

	schema := &JSONSchema{}
	if err = jsoniter.Unmarshal(reqSchema, schema); err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, err.Error()).WithDetails(&api.ErrorDetails{
			Code:    api.Code_INTERNAL.String(),
			Message: fmt.Sprintf("schema: '%s', unmarshalling failed", string(reqSchema)),
		})
	}
	if collection != "" && collection != schema.Name {
		return nil, errors.InvalidArgument("collection name is not same as schema name '%s' '%s'", collection, schema.Name)
	}
	if len(schema.Properties) == 0 {
		return nil, errors.InvalidArgument("missing properties field in schema")
	}
	if len(schema.PrimaryKeys) == 0 {
		return nil, errors.InvalidArgument("missing primary key field in schema")
	}

	primaryKeysSet := container.NewHashSet(schema.PrimaryKeys...)
	fields, err := fb.deserializeProperties(schema.Properties, &primaryKeysSet, nil)
	if err != nil {
		return nil, err
	}

	// ordering needs to same as in schema
	var primaryKeyFields []*Field
	for _, pkeyField := range schema.PrimaryKeys {
		found := false
		for _, f := range fields {
			if f.FieldName == pkeyField {
				primaryKeyFields = append(primaryKeyFields, f)
				found = true
			}
		}
		if !found {
			return nil, errors.InvalidArgument("missing primary key '%s' field in schema", pkeyField)
		}
	}

	// Create the secondary indexes with an unknown state
	// to determine the state, tigris will need to read from the index metadata
	secondaryIndex := []*Index{
		{
			Name:    ReservedFields[CreatedAt],
			IdxType: SECONDARY_INDEX,
			State:   UNKNOWN,
		},
		{
			Name:    ReservedFields[UpdatedAt],
			IdxType: SECONDARY_INDEX,
			State:   UNKNOWN,
		},
	}
	for _, field := range fields {
		if field.Indexed != nil && *field.Indexed {
			secondaryIndex = append(secondaryIndex, &Index{Name: field.Name(), IdxType: SECONDARY_INDEX, State: UNKNOWN, Fields: []*Field{field}})
		}
	}

	factory := &Factory{
		Fields: fields,
		PrimaryKey: &Index{
			Name:    PrimaryKeyIndexName,
			Fields:  primaryKeyFields,
			IdxType: PRIMARY_INDEX,
			State:   INDEX_ACTIVE,
		},
		Indexes: &Indexes{
			All: secondaryIndex,
		},
		Name:            collection,
		Schema:          reqSchema,
		CollectionType:  cType,
		IndexingVersion: schema.IndexingVersion,
		Version:         schema.Version,
	}

	if fb.onUserRequest {
		if err = fb.validateSchema(factory); err != nil {
			return nil, err
		}
	}

	return factory, nil
}

func (fb *FactoryBuilder) validateSchema(factory *Factory) error {
	for _, f := range factory.Fields {
		if err := ValidateFieldAttributes(false, f); err != nil {
			return err
		}
	}

	return nil
}

func setPrimaryKey(reqSchema jsoniter.RawMessage, format string, ifMissing bool) (jsoniter.RawMessage, error) {
	var schema map[string]interface{}
	if err := jsoniter.Unmarshal(reqSchema, &schema); err != nil {
		return nil, err
	}

	if _, ok := schema[PrimaryKeySchemaK]; ifMissing && ok {
		// primary key exists, no need to do anything.
		return reqSchema, nil
	}

	schema[PrimaryKeySchemaK] = []string{AutoPrimaryKeyF}
	if p, ok := schema["properties"]; ok {
		propertiesMap, ok := p.(map[string]interface{})
		if !ok {
			return nil, errors.InvalidArgument("properties object is invalid")
		}

		if _, ok = propertiesMap[AutoPrimaryKeyF]; !ifMissing || !ok {
			propertiesMap[AutoPrimaryKeyF] = map[string]interface{}{
				"type":         jsonSpecString,
				"format":       format,
				"autoGenerate": true,
			}
		}
	}

	return jsoniter.Marshal(schema)
}

// Generate schema in the requested format.
func Generate(jsonSchema []byte, format string) ([]byte, error) {
	schemas := make(map[string]string)

	for _, f := range strings.Split(format, ",") {
		f = strings.Trim(f, " ")
		if strings.ToLower(f) == "json" {
			schemas[f] = string(jsonSchema)
		} else {
			sch, err := langSchema.GenCollectionSchema(jsonSchema, f)
			if ulog.E(err) {
				return nil, errors.Internal("error generating schema")
			}

			schemas[f] = string(sch)
		}
	}

	b, err := jsoniter.Marshal(schemas)
	if ulog.E(err) {
		return nil, errors.Internal("error generating schema")
	}

	return b, nil
}
