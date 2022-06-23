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
	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/lib/set"
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
)

var (
	boolTrue = true
)

type JSONSchema struct {
	Name        string              `json:"title,omitempty"`
	Description string              `json:"description,omitempty"`
	Properties  jsoniter.RawMessage `json:"properties,omitempty"`
	PrimaryKeys []string            `json:"primary_key,omitempty"`
}

// Factory is used as an intermediate step so that collection can be initialized with properly encoded values.
type Factory struct {
	// Name is the collection name of this schema.
	Name string
	// Fields are derived from the user schema.
	Fields []*Field
	// Indexes is a wrapper on the indexes part of this collection. At this point the dictionary encoded value is not
	// set for these indexes which is set as part of collection creation.
	Indexes *Indexes
	// Schema is the raw JSON schema received as part of CreateOrUpdateCollection request. This is stored as-is in the
	// schema subspace.
	Schema jsoniter.RawMessage
}

// Build is used to deserialize the user json schema into a schema factory.
func Build(collection string, reqSchema jsoniter.RawMessage) (*Factory, error) {
	var err error
	if reqSchema, err = addPrimaryKeyIfMissing(reqSchema); err != nil {
		return nil, err
	}

	var schema = &JSONSchema{}
	if err = jsoniter.Unmarshal(reqSchema, schema); err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, errors.Wrap(err, "unmarshalling failed").Error())
	}
	if collection != schema.Name {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "collection name is not same as schema name '%s' '%s'", collection, schema.Name)
	}
	if len(schema.Properties) == 0 {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "missing properties field in schema")
	}

	if len(schema.PrimaryKeys) == 0 {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "missing primary key field in schema")
	}

	var primaryKeysSet = set.New(schema.PrimaryKeys...)
	fields, err := deserializeProperties(schema.Properties, primaryKeysSet)
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
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "missing primary key '%s' field in schema", pkeyField)
		}
	}

	return &Factory{
		Fields: fields,
		Indexes: &Indexes{
			PrimaryKey: &Index{
				Name:   PrimaryKeyIndexName,
				Fields: primaryKeyFields,
			},
		},
		Name:   collection,
		Schema: reqSchema,
	}, nil
}

func addPrimaryKeyIfMissing(reqSchema jsoniter.RawMessage) (jsoniter.RawMessage, error) {
	var schema map[string]interface{}
	if err := jsoniter.Unmarshal(reqSchema, &schema); err != nil {
		return nil, err
	}

	if _, ok := schema[PrimaryKeySchemaK]; ok {
		// primary key exists, no need to do anything.
		return reqSchema, nil
	}

	schema[PrimaryKeySchemaK] = []string{AutoPrimaryKeyF}
	if p, ok := schema["properties"]; ok {
		propertiesMap, ok := p.(map[string]interface{})
		if !ok {
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "properties object is invalid")
		}

		if _, ok = propertiesMap[AutoPrimaryKeyF]; !ok {
			// if user doesn't have the ID field then add it of type UUID
			propertiesMap[AutoPrimaryKeyF] = map[string]interface{}{
				"type":         jsonSpecString,
				"format":       jsonSpecFormatUUID,
				"autoGenerate": true,
			}
		}
	}

	return jsoniter.Marshal(schema)
}

func deserializeProperties(properties jsoniter.RawMessage, primaryKeysSet set.HashSet) ([]*Field, error) {
	var fields []*Field
	var err error
	err = jsonparser.ObjectEach(properties, func(key []byte, v []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return api.Errorf(api.Code_INTERNAL, errors.Wrap(err, "failed to iterate on user schema").Error())
		}

		var builder FieldBuilder
		if err = builder.Validate(v); err != nil {
			// builder validates against the supported schema attributes on properties
			return err
		}

		// set field name and try to unmarshal the value into field builder
		builder.FieldName = string(key)
		if err = jsoniter.Unmarshal(v, &builder); err != nil {
			return api.Errorf(api.Code_INTERNAL, err.Error())
		}
		if builder.Type == jsonSpecArray && builder.Items == nil {
			return api.Errorf(api.Code_INVALID_ARGUMENT, "missing items for array field")
		}
		if builder.Type == jsonSpecObject && len(builder.Properties) == 0 {
			return api.Errorf(api.Code_INVALID_ARGUMENT, "missing properties for object field")
		}

		if builder.Items != nil {
			// for arrays, items must be set, and it is possible that item type is object in that case deserialize those
			// fields
			var nestedFields []*Field
			if len(builder.Items.Properties) > 0 {
				if nestedFields, err = deserializeProperties(builder.Items.Properties, primaryKeysSet); err != nil {
					return err
				}
				builder.Fields = nestedFields
			} else {
				// if it is simple item type
				var f *Field
				if f, err = builder.Items.Build(true); err != nil {
					return err
				}
				builder.Fields = append(nestedFields, f)
			}
		}

		// for objects, properties are part of the field definitions in that case deserialize those
		// nested fields
		if len(builder.Properties) > 0 {
			var nestedFields []*Field
			if nestedFields, err = deserializeProperties(builder.Properties, primaryKeysSet); err != nil {
				return err
			}
			builder.Fields = nestedFields
		}
		if primaryKeysSet.Contains(builder.FieldName) {
			boolTrue := true
			builder.Primary = &boolTrue
		}

		var f *Field
		f, err = builder.Build(false)
		if err != nil {
			return err
		}
		fields = append(fields, f)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return fields, nil
}
