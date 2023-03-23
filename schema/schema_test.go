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
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
)

func TestCreateCollectionFromSchema(t *testing.T) {
	t.Run("test_create_success", func(t *testing.T) {
		reqSchema := []byte(`{"title":"t1", "description":"This document records the details of an order","properties":{"order_id":{"description":"A unique identifier for an order","type":"integer"},"cust_id":{"description":"A unique identifier for a customer","type":"integer"},"product":{"description":"name of the product","type":"string","maxLength":100},"quantity":{"description":"number of products ordered","type":"integer"},"price":{"description":"price of the product","type":"number"}},"primary_key":["cust_id","order_id"]}`)
		schF, err := NewFactoryBuilder(true).Build("t1", reqSchema)
		require.NoError(t, err)

		c, err := NewDefaultCollection(1, 1, schF, nil, nil)
		require.NoError(t, err)

		require.Equal(t, c.Name, "t1")
		require.Equal(t, c.Indexes.PrimaryKey.Fields[0].FieldName, "cust_id")
		require.Equal(t, c.Indexes.PrimaryKey.Fields[1].FieldName, "order_id")
	})
	t.Run("test_create_failure", func(t *testing.T) {
		reqSchema := []byte(`{"title":"Record of an order","properties":{"order_id":{"description":"A unique identifier for an order","type":"integer"},"cust_id":{"description":"A unique identifier for a customer","type":"integer"},"product":{"description":"name of the product","type":"string","maxLength":100},"quantity":{"description":"number of products ordered","type":"integer"},"price":{"description":"price of the product","type":"number"}},"primary_key":["cust_id","order_id"]}`)
		_, err := NewFactoryBuilder(true).Build("t1", reqSchema)
		require.Equal(t, "collection name is not same as schema name 't1' 'Record of an order'", err.(*api.TigrisError).Error())
	})
	t.Run("test_supported_types", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
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
			"format": "byte"
		},
		"K6": {
			"type": "string",
			"format": "uuid"
		},
		"K7": {
			"type": "string",
			"format": "date-time"
		},
		"K8": {
			"type": "array",
			"maxItems": 0
		}
	},
	"primary_key": ["K1", "K2"]
}`)
		sch, err := NewFactoryBuilder(true).Build("t1", schema)
		require.NoError(t, err)
		c, err := NewDefaultCollection(1, 1, sch, nil, nil)
		require.NoError(t, err)
		fields := c.GetFields()
		require.Equal(t, StringType, fields[0].DataType)
		require.Equal(t, Int64Type, fields[1].DataType)
		require.Equal(t, DoubleType, fields[2].DataType)
		require.Equal(t, BoolType, fields[3].DataType)
		require.Equal(t, ByteType, fields[4].DataType)
		require.Equal(t, UUIDType, fields[5].DataType)
		require.Equal(t, DateTimeType, fields[6].DataType)
		require.Equal(t, ArrayType, fields[7].DataType)
	})
	t.Run("test_supported_primary_keys", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
	"properties": {
		"K1": {
			"type": "string"
		},
		"K2": {
			"type": "integer"
		},
		"K3": {
			"type": "string",
			"format": "byte"
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
		sch, err := NewFactoryBuilder(true).Build("t1", schema)
		require.NoError(t, err)
		c, err := NewDefaultCollection(1, 1, sch, nil, nil)
		require.NoError(t, err)
		require.Equal(t, StringType, c.Indexes.PrimaryKey.Fields[0].DataType)
		require.Equal(t, Int64Type, c.Indexes.PrimaryKey.Fields[1].DataType)
		require.Equal(t, ByteType, c.Indexes.PrimaryKey.Fields[2].DataType)
		require.Equal(t, UUIDType, c.Indexes.PrimaryKey.Fields[3].DataType)
		require.Equal(t, DateTimeType, c.Indexes.PrimaryKey.Fields[4].DataType)
	})
	t.Run("test_unsupported_primary_key", func(t *testing.T) {
		schema := []byte(`{
		"title": "t1",
		"properties": {
			"K1": {
				"type": "number"
			},
			"K2": {
				"type": "integer"
			}
		},
		"primary_key": ["K1"]
	}`)
		_, err := NewFactoryBuilder(true).Build("t1", schema)
		require.Equal(t, "unsupported primary key type detected 'double'", err.(*api.TigrisError).Error())
	})

	t.Run("test_complex_types", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
	"properties": {
		"id": {
			"type": "integer"
		},
		"random": {
			"type": "string",
			"format": "byte",
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
		},
		"array_2d_simple": {
			"type": "array",
			"items": {
				"type": "array",
				"items": {
					"type": "string"
				}
			}
		},
		"array_2d_object": {
			"type": "array",
			"items": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"id": {
							"type": "integer"
						},
						"item_name": {
							"type": "string"
						}
					}
				}
			}
		}
	},
	"primary_key": ["id"]
}`)
		sch, err := NewFactoryBuilder(true).Build("t1", schema)
		require.NoError(t, err)

		coll, err := NewDefaultCollection(1, 1, sch, nil, nil)
		require.NoError(t, err)

		coll.Validator = nil
		coll.QueryableFields = nil
		coll.int64FieldsPath = nil
		coll.fieldsWithInsertDefaults = nil
		coll.fieldsWithUpdateDefaults = nil
		coll.SchemaDeltas = nil

		b := true
		max1024 := int32(1024)
		max100 := int32(100)
		expColl := &DefaultCollection{
			CollectionType: "documents",
			Schema:         schema,
			Id:             1,
			SchVer:         1,
			Name:           "t1",
			Indexes: &Indexes{
				PrimaryKey: &Index{
					Fields: []*Field{{FieldName: "id", DataType: Int64Type, PrimaryKeyField: &b}},
					Name:   "pkey",
				},
				SecondaryIndex: &Index{
					Fields: []*Field{},
					Name:   "skey",
				},
			},
			Fields: []*Field{
				{FieldName: "id", DataType: Int64Type, PrimaryKeyField: &b},
				{FieldName: "random", DataType: ByteType, MaxLength: &max1024},
				{FieldName: "product", DataType: StringType, MaxLength: &max100},
				{FieldName: "price", DataType: DoubleType},
				{
					FieldName: "simple_items", DataType: ArrayType,
					Fields: []*Field{{DataType: Int64Type}},
				},
				{
					FieldName: "simple_object",
					DataType:  ObjectType,
					Fields: []*Field{
						{FieldName: "name", DataType: StringType},
					},
				},
				{
					FieldName: "product_items", DataType: ArrayType,
					Fields: []*Field{
						{
							FieldName: "", DataType: ObjectType,
							Fields: []*Field{
								{FieldName: "id", DataType: Int64Type},
								{FieldName: "item_name", DataType: StringType},
								{
									FieldName: "nested_array", DataType: ArrayType,
									Fields: []*Field{
										{FieldName: "", DataType: Int64Type},
									},
								},
							},
						},
					},
				},
				{
					FieldName: "array_2d_simple", DataType: ArrayType,
					Fields: []*Field{
						{
							FieldName: "", DataType: ArrayType,
							Fields: []*Field{
								{FieldName: "", DataType: StringType},
							},
						},
					},
				},
				{
					FieldName: "array_2d_object", DataType: ArrayType,
					Fields: []*Field{
						{
							FieldName: "", DataType: ArrayType,
							Fields: []*Field{
								{
									FieldName: "", DataType: ObjectType,
									Fields: []*Field{
										{FieldName: "id", DataType: Int64Type},
										{FieldName: "item_name", DataType: StringType},
									},
								},
							},
						},
					},
				},
			},
			SearchIndexes: make(map[string]*SearchIndex),
			FieldVersions: make(map[string]*FieldVersions),
		}

		require.Equal(t, expColl, coll)
	})

	t.Run("test_array_missing_items_error", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
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
		_, err := NewFactoryBuilder(true).Build("t1", schema)
		require.Equal(t, errors.InvalidArgument("missing items for array field"), err)
	})

	t.Run("test_array_items_and_props_error", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
	"properties": {
		"id": {
			"type": "integer"
		},
		"simple_items": {
			"type": "array",
			"properties": {
				"id": {
					"type": "integer"
				}
			},
			"items": {
				"type": "integer"
			}
		}
	},
	"primary_key": ["id"]
}`)
		_, err := NewFactoryBuilder(true).Build("t1", schema)
		require.Equal(t, "Properties only allowed for object type", err.Error())
	})

	t.Run("test_object_missing_properties_error", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
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
		sch, err := NewFactoryBuilder(true).Build("t1", schema)
		require.NoError(t, err)
		c, err := NewDefaultCollection(1, 1, sch, nil, nil)
		require.NoError(t, err)
		fields := c.GetFields()
		require.Equal(t, ObjectType, fields[1].DataType)
	})
	t.Run("test_auto-generated", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
	"properties": {
		"K1": {
			"type": "string",
			"autoGenerate": true
		},
		"K2": {
			"type": "string",
			"format": "byte"
		},
		"K3": {
			"type": "string",
			"format": "uuid"
		},
		"K4": {
			"type": "string",
			"format": "date-time"
		}
	},
	"primary_key": ["K1", "K2"]
}`)
		sch, err := NewFactoryBuilder(true).Build("t1", schema)
		require.NoError(t, err)
		c, err := NewDefaultCollection(1, 1, sch, nil, nil)
		require.NoError(t, err)
		fields := c.GetFields()
		require.True(t, *fields[0].PrimaryKeyField)
		require.True(t, *fields[0].AutoGenerated)
		require.True(t, *fields[1].PrimaryKeyField)
		require.Nil(t, fields[1].AutoGenerated)
	})
	t.Run("test_defaults", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
	"properties": {
		"K1": {
			"type": "string",
			"format": "date-time",
			"default": "now()"
		},
		"K2": {
			"type": "string",
			"default": "a"
		},
		"K3": {
			"type": "string",
			"format": "date-time",
			"createdAt": true
		},
		"K4": {
			"type": "string",
			"format": "date-time",
			"updatedAt": true
		}
	},
	"primary_key": ["K1"]
}`)
		sch, err := NewFactoryBuilder(true).Build("t1", schema)
		require.NoError(t, err)
		c, err := NewDefaultCollection(1, 1, sch, nil, nil)
		require.NoError(t, err)
		fields := c.GetFields()
		require.True(t, fields[0].Defaulter.createdAt)
		require.Equal(t, "a", fields[1].Defaulter.value)
		require.Nil(t, fields[2].Defaulter.value)
		require.False(t, fields[2].Defaulter.updatedAt)
		require.True(t, fields[2].Defaulter.createdAt)
		require.Nil(t, fields[3].Defaulter.value)
		require.False(t, fields[3].Defaulter.createdAt)
		require.True(t, fields[3].Defaulter.updatedAt)
	})
	t.Run("test_defaults_errors", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
	"properties": {
		"K1": {
			"type": "string",
			"default": "now()"
		},
		"K2": {
			"type": "string",
			"default": "a"
		}
	},
	"primary_key": ["K1"]
}`)
		_, err := NewFactoryBuilder(true).Build("t1", schema)
		require.Equal(t, "now() is only supported for date-time type", err.Error())

		schema = []byte(`{
	"title": "t1",
	"properties": {
		"K1": {
			"type": "string",
			"default": "random()"
		},
		"K2": {
			"type": "string"
		}
	},
	"primary_key": ["K1"]
}`)
		_, err = NewFactoryBuilder(true).Build("t1", schema)
		require.Equal(t, "'random()' function is not supported", err.Error())
	})
	t.Run("test_no-primary-key-default-id", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
	"properties": {
		"K1": {
			"type": "string"
		},
		"K2": {
			"type": "string",
			"format": "byte"
		}
	}
}`)
		sch, err := NewFactoryBuilder(true).Build("t1", schema)
		require.NoError(t, err)
		c, err := NewDefaultCollection(1, 1, sch, nil, nil)
		require.NoError(t, err)
		fields := c.GetFields()
		require.Equal(t, 3, len(fields))
		primaryKeyPresent := false
		for _, f := range fields {
			if f.FieldName == AutoPrimaryKeyF {
				primaryKeyPresent = true
				require.True(t, f.IsAutoGenerated())
				require.True(t, f.IsPrimaryKey())
				require.Equal(t, UUIDType, f.Type())
			} else {
				require.False(t, f.IsPrimaryKey())
			}
		}
		require.True(t, primaryKeyPresent)
		require.Equal(t, UUIDType, c.Indexes.PrimaryKey.Fields[0].DataType)
	})
	t.Run("test_no-primary-key-user-id", func(t *testing.T) {
		schema := []byte(`{
	"title": "t1",
	"properties": {
		"k1": {
			"type": "string"
		},
		"id": {
			"type": "integer"
		}
	}
}`)
		sch, err := NewFactoryBuilder(true).Build("t1", schema)
		require.NoError(t, err)
		c, err := NewDefaultCollection(1, 1, sch, nil, nil)
		require.NoError(t, err)
		fields := c.GetFields()
		require.Equal(t, 2, len(fields))
		primaryKeyPresent := false
		for _, f := range fields {
			if f.FieldName == AutoPrimaryKeyF {
				primaryKeyPresent = true
				require.True(t, f.IsPrimaryKey())
				require.Equal(t, Int64Type, f.Type())
			} else {
				require.False(t, f.IsPrimaryKey())
			}
		}
		require.True(t, primaryKeyPresent)
		require.Equal(t, Int64Type, c.Indexes.PrimaryKey.Fields[0].DataType)
	})
}

func TestGetCollectionType(t *testing.T) {
	schema := []byte(`{
	"title": "t1",
	"properties": {
		"k1": {
			"type": "string"
		},
		"id": {
			"type": "integer"
		}
	}
}`)

	ty, err := GetCollectionType(schema)
	require.Equal(t, DocumentsType, ty)
	require.NoError(t, err)
}
