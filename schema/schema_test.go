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
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
)

func TestCreateCollectionFromSchema(t *testing.T) {
	t.Run("test_create_success", func(t *testing.T) {
		reqSchema := []byte(`{"name":"t1", "description":"This document records the details of an order","properties":{"order_id":{"description":"A unique identifier for an order","type":"integer"},"cust_id":{"description":"A unique identifier for a customer","type":"integer"},"product":{"description":"name of the product","type":"string","maxLength":100},"quantity":{"description":"number of products ordered","type":"integer"},"price":{"description":"price of the product","type":"number"}},"primary_key":["cust_id","order_id"]}`)
		schF, err := Build("t1", reqSchema)
		require.NoError(t, err)
		c := NewDefaultCollection("t1", 1, schF.Fields, schF.Indexes, schF.Schema)
		require.Equal(t, c.Name, "t1")
		require.Equal(t, c.Indexes.PrimaryKey.Fields[0].FieldName, "cust_id")
		require.Equal(t, c.Indexes.PrimaryKey.Fields[1].FieldName, "order_id")
	})
	t.Run("test_create_failure", func(t *testing.T) {
		reqSchema := []byte(`{"name":"Record of an order","properties":{"order_id":{"description":"A unique identifier for an order","type":"integer"},"cust_id":{"description":"A unique identifier for a customer","type":"integer"},"product":{"description":"name of the product","type":"string","maxLength":100},"quantity":{"description":"number of products ordered","type":"integer"},"price":{"description":"price of the product","type":"number"}},"primary_key":["cust_id","order_id"]}`)
		_, err := Build("t1", reqSchema)
		require.Equal(t, "collection name is not same as schema name 't1' 'Record of an order'", err.(*api.TigrisDBError).Error())
	})
	t.Run("test_supported_types", func(t *testing.T) {
		schema := []byte(`{
	"name": "t1",
	"properties": {
		"K1": {
			"type": "string"
		},
		"K2": {
			"type": "integer"
		},
		"K3": {
			"type": "number"
		},
		"K4": {
			"type": "boolean"
		},
		"K5": {
			"type": "string",
			"contentEncoding": "base64"
		},
		"K6": {
			"type": "string",
			"format": "uuid"
		},
		"K7": {
			"type": "string",
			"format": "date-time"
		}
	},
	"primary_key": ["K1", "K2"]
}`)
		sch, err := Build("t1", schema)
		require.NoError(t, err)
		c := NewDefaultCollection("t1", 1, sch.Fields, sch.Indexes, sch.Schema)
		fields := c.GetFields()
		require.Equal(t, StringType, fields[0].DataType)
		require.Equal(t, IntType, fields[1].DataType)
		require.Equal(t, DoubleType, fields[2].DataType)
		require.Equal(t, BoolType, fields[3].DataType)
		require.Equal(t, ByteType, fields[4].DataType)
		require.Equal(t, UUIDType, fields[5].DataType)
		require.Equal(t, DateTimeType, fields[6].DataType)
	})
	t.Run("test_supported_primary_keys", func(t *testing.T) {
		schema := []byte(`{
	"name": "t1",
	"properties": {
		"K1": {
			"type": "string"
		},
		"K2": {
			"type": "integer"
		},
		"K3": {
			"type": "string",
			"contentEncoding": "base64"
		},
		"K4": {
			"type": "string",
			"format": "uuid"
		},
		"K5": {
			"type": "string",
			"format": "date-time"
		}
	},
	"primary_key": ["K1", "K2", "K3", "K4", "K5"]
}`)
		sch, err := Build("t1", schema)
		require.NoError(t, err)
		c := NewDefaultCollection("t1", 1, sch.Fields, sch.Indexes, sch.Schema)
		require.NoError(t, err)
		require.Equal(t, StringType, c.Indexes.PrimaryKey.Fields[0].DataType)
		require.Equal(t, IntType, c.Indexes.PrimaryKey.Fields[1].DataType)
		require.Equal(t, ByteType, c.Indexes.PrimaryKey.Fields[2].DataType)
		require.Equal(t, UUIDType, c.Indexes.PrimaryKey.Fields[3].DataType)
		require.Equal(t, DateTimeType, c.Indexes.PrimaryKey.Fields[4].DataType)
	})
	t.Run("test_unsupported_primary_key", func(t *testing.T) {
		schema := []byte(`{
		"name": "t1",
		"properties": {
			"K1": {
				"type": "number"
			},
			"K2": {
				"type": "int"
			}
		},
		"primary_key": ["K1"]
	}`)
		_, err := Build("t1", schema)
		require.Equal(t, "unsupported primary key type detected 'number'", err.(*api.TigrisDBError).Error())
	})
	t.Run("test_complex_types", func(t *testing.T) {
		schema := []byte(`{
	"name": "t1",
	"properties": {
		"id": {
			"type": "integer"
		},
		"random": {
			"type": "string",
			"contentEncoding": "base64",
			"maxLength": 1024
		},
		"product": {
			"type": "string",
			"maxLength": 100
		},
		"price": {
			"type": "number"
		},
		"simple_items": {
			"type": "array",
			"items": {
				"type": "integer"
			}
		},
		"simple_object": {
			"type": "object",
			"properties": {
				"name": { "type": "string" }
			}
		},
		"product_items": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"id": {
						"type": "integer"
					},
					"item_name": {
						"type": "string"
					},
					"nested_array": {
						"type": "array",
						"items": {
							"type": "integer"
						}
					}
				}
			}
		}
	},
	"primary_key": ["id"]
}`)
		sch, err := Build("t1", schema)
		require.NoError(t, err)
		coll := NewDefaultCollection("t1", 1, sch.Fields, sch.Indexes, sch.Schema)
		require.Equal(t, "simple_items", coll.Fields[4].FieldName)
		require.Equal(t, IntType, coll.Fields[4].Fields[0].DataType)
		require.Equal(t, 1, len(coll.Fields[4].Fields))

		require.Equal(t, "simple_object", coll.Fields[5].FieldName)
		require.Equal(t, 1, len(coll.Fields[5].Fields))
		require.Equal(t, StringType, coll.Fields[5].Fields[0].DataType)
		require.Equal(t, "name", coll.Fields[5].Fields[0].FieldName)

		require.Equal(t, "product_items", coll.Fields[6].FieldName)
		require.Equal(t, 3, len(coll.Fields[6].Fields))
		require.Equal(t, IntType, coll.Fields[6].Fields[0].DataType)
		require.Equal(t, "id", coll.Fields[6].Fields[0].FieldName)
		require.Equal(t, StringType, coll.Fields[6].Fields[1].DataType)
		require.Equal(t, "item_name", coll.Fields[6].Fields[1].FieldName)
		require.Equal(t, ArrayType, coll.Fields[6].Fields[2].DataType)
		require.Equal(t, "nested_array", coll.Fields[6].Fields[2].FieldName)
	})
	t.Run("test_array_missing_items_error", func(t *testing.T) {
		schema := []byte(`{
	"name": "t1",
	"properties": {
		"id": {
			"type": "integer"
		},
		"simple_items": {
			"type": "array"
		}
	},
	"primary_key": ["id"]
}`)
		_, err := Build("t1", schema)
		require.Equal(t, api.Errorf(codes.InvalidArgument, "missing items for array field"), err)
	})
	t.Run("test_object_missing_properties_error", func(t *testing.T) {
		schema := []byte(`{
	"name": "t1",
	"properties": {
		"id": {
			"type": "integer"
		},
		"simple_object": {
			"type": "object"
		}
	},
	"primary_key": ["id"]
}`)
		_, err := Build("t1", schema)
		require.Equal(t, api.Errorf(codes.InvalidArgument, "missing properties for object field"), err)
	})
}
