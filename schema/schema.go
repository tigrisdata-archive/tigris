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
	"fmt"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/lib/set"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/**
A sample user JSON schema looks like below,
{
	"title": "Record of an order",
	"description": "This document records the details of an order",
	"properties": {
		"order_id": {
			"description": "A unique identifier for an order",
			"type": "bigint"
		},
		"cust_id": {
			"description": "A unique identifier for a customer",
			"type": "bigint"
		},
		"product": {
			"description": "name of the product",
			"type": "string",
			"max_length": 100,
			"unique": true
		},
		"quantity": {
			"description": "number of products ordered",
			"type": "int"
		},
		"price": {
			"description": "price of the product",
			"type": "double"
		},
		"date_ordered": {
			"description": "The date order was made",
			"type": "datetime"
		}
	},
	"primary_key": [
		"cust_id",
		"order_id"
	]
}
*/

type JSONSchema struct {
	Properties  jsoniter.RawMessage `json:"properties,omitempty"`
	PrimaryKeys []string            `json:"primary_key,omitempty"`
}

func CreateCollection(database string, collection string, reqSchema jsoniter.RawMessage) (Collection, error) {
	var schema = &JSONSchema{}
	if err := jsoniter.Unmarshal(reqSchema, schema); err != nil {
		return nil, api.Errorf(codes.Internal, errors.Wrap(err, "unmarshalling failed").Error())
	}
	if len(schema.Properties) == 0 {
		return nil, api.Errorf(codes.InvalidArgument, "missing properties field in schema")
	}
	if len(schema.PrimaryKeys) == 0 {
		return nil, api.Errorf(codes.InvalidArgument, "missing primary key field in schema")
	}

	var primaryKeysSet = set.New(schema.PrimaryKeys...)
	var fields []*Field
	var err error
	err = jsonparser.ObjectEach(schema.Properties, func(key []byte, v []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return api.Errorf(codes.Internal, errors.Wrap(err, "failed to iterate on user schema").Error())
		}

		var builder FieldBuilder
		builder.FieldName = string(key)
		if err = jsoniter.Unmarshal(v, &builder); err != nil {
			return api.Errorf(codes.Internal, err.Error())
		}
		if primaryKeysSet.Contains(builder.FieldName) {
			boolTrue := true
			builder.Primary = &boolTrue
		}

		fields = append(fields, builder.Build())

		return nil
	})
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
			return nil, status.Errorf(codes.InvalidArgument, "missing primary key '%s' field in schema", pkeyField)
		}
	}

	return NewCollection(database, collection, fields, primaryKeyFields), nil
}

func StorageName(databaseName, collectionName string) string {
	return fmt.Sprintf("%s.%s", databaseName, collectionName)
}
