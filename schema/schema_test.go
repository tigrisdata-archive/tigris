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

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
)

func TestCreateCollectionFromSchema(t *testing.T) {
	t.Run("test_create_success", func(t *testing.T) {
		reqSchema := []byte(`{"name":"t1", "description":"This document records the details of an order","properties":{"order_id":{"description":"A unique identifier for an order","type":"bigint"},"cust_id":{"description":"A unique identifier for a customer","type":"bigint"},"product":{"description":"name of the product","type":"string","maxLength":100},"quantity":{"description":"number of products ordered","type":"int"},"price":{"description":"price of the product","type":"double"}},"primary_key":["cust_id","order_id"]}`)
		c, err := CreateCollection("d1", "t1", reqSchema)
		require.NoError(t, err)
		require.Equal(t, c.Name(), "t1")
		require.Equal(t, c.PrimaryKeys()[0].FieldName, "cust_id")
		require.Equal(t, c.PrimaryKeys()[1].FieldName, "order_id")
	})
	t.Run("test_create_failure", func(t *testing.T) {
		reqSchema := []byte(`{"name":"Record of an order","properties":{"order_id":{"description":"A unique identifier for an order","type":"bigint"},"cust_id":{"description":"A unique identifier for a customer","type":"bigint"},"product":{"description":"name of the product","type":"string","maxLength":100,"unique":true},"quantity":{"description":"number of products ordered","type":"int"},"price":{"description":"price of the product","type":"double"}},"primary_key":["cust_id","order_id"]}`)
		_, err := CreateCollection("d1", "t1", reqSchema)
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
				"type": "int"
			},
			"K3": {
				"type": "bigint"
			},
			"K4": {
				"type": "boolean"
			},
			"K5": {
				"type": "bytes"
			},
			"K6": {
				"type": "double"
			}
		},
		"primary_key": ["K1", "K2"]
	}`)
		c, err := CreateCollection("d1", "t1", schema)
		require.NoError(t, err)
		fields := c.GetFields()
		require.Equal(t, StringType, fields[0].DataType)
		require.Equal(t, IntType, fields[1].DataType)
		require.Equal(t, BigIntType, fields[2].DataType)
		require.Equal(t, BoolType, fields[3].DataType)
		require.Equal(t, BytesType, fields[4].DataType)
		require.Equal(t, DoubleType, fields[5].DataType)
	})
	t.Run("test_supported_primary_keys", func(t *testing.T) {
		schema := []byte(`{
		"name": "t1",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "int"
			}
		},
		"primary_key": ["K1"]
	}`)
		c, err := CreateCollection("d1", "t1", schema)
		require.NoError(t, err)
		fields := c.PrimaryKeys()
		require.Equal(t, StringType, fields[0].DataType)
	})
	t.Run("test_unsupported_primary_key", func(t *testing.T) {
		schema := []byte(`{
		"name": "t1",
		"properties": {
			"K1": {
				"type": "double"
			},
			"K2": {
				"type": "int"
			}
		},
		"primary_key": ["K1"]
	}`)
		_, err := CreateCollection("d1", "t1", schema)
		require.Equal(t, "unsupported primary key type detected 'double'", err.(*api.TigrisDBError).Error())
	})
}
