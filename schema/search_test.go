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
	"github.com/tigrisdata/tigris/util"
)

func TestSearchIndex_CollectionSchema(t *testing.T) {
	reqSchema := []byte(`{
	"title": "t1",
	"properties": {
		"id": {
			"type": "integer"
		},
		"id_32": {
			"type": "integer",
			"format": "int32"
		},
		"product": {
			"type": "string",
			"maxLength": 100
		},
		"id_uuid": {
			"type": "string",
			"format": "uuid"
		},
		"ts": {
			"type": "string",
			"format": "date-time"
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
				"name": {
					"type": "string"
				},
				"phone": {
					"type": "string"
				},
				"address": {
					"type": "object",
					"properties": {
						"street": {
							"type": "string"
						}
					}
				},
				"details": {
					"type": "object",
					"properties": {
						"nested_id": {
							"type": "integer"
						},
						"nested_obj": {
							"type": "object",
							"properties": {
								"id": {
									"type": "integer"
								},
								"name": {
									"type": "string"
								}
							}
						},
						"nested_array": {
							"type": "array",
							"items": {
								"type": "integer"
							}
						},
						"nested_string": {
							"type": "string"
						}
					}
				}
			}
		}
	},
	"primary_key": ["id"]
}`)

	schFactory, err := NewFactoryBuilder(true).Build("t1", reqSchema)
	require.NoError(t, err)

	expFlattenedFields := []string{
		"id", "_tigris_id", "id_32", "product", "id_uuid", "ts", ToSearchDateKey("ts"), "price", "simple_items", "simple_object.name",
		"simple_object.phone", "simple_object.address.street", "simple_object.details.nested_id", "simple_object.details.nested_obj.id",
		"simple_object.details.nested_obj.name", "simple_object.details.nested_array", "simple_object.details.nested_string",
		"_tigris_created_at", "_tigris_updated_at",
	}

	implicitSearchIndex := NewImplicitSearchIndex("t1", "t1", schFactory.Fields, nil)
	for i, f := range implicitSearchIndex.StoreSchema.Fields {
		require.Equal(t, expFlattenedFields[i], f.Name)
	}
}

func TestSearchIndex_Schema(t *testing.T) {
	cases := []struct {
		schema      []byte
		expErrorMsg string
	}{
		{
			[]byte(`{"title": "t1", "properties": { "simple_items": {"type": "array", "items": {"type": "integer"}, "facet": true}}}`),
			"",
		},
		{
			[]byte(`{"title": "t1", "properties": { "simple_items": {"type": "object", "facet": true}}}`),
			"Cannot have sort or facet attribute on an object 'simple_items'",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string", "format": "byte", "facet": true}}}`),
			"Cannot enable search index on field 'a' of type 'byte'",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string", "format": "byte", "sort": true}}}`),
			"Cannot enable search index on field 'a' of type 'byte'",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "integer", "sort": true, "facet": true}, "b": {"type": "string", "sort": true, "facet": true}, "c": {"type": "number", "sort": true, "facet": true}}}`),
			"",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string"}, "b": {"type": "array", "format": "vector"}}}`),
			"Field 'b' type 'vector' is missing dimensions",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string"}, "b": {"type": "array", "format": "vector", "dimensions": 4}}}`),
			"",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string", "id": true}, "b": {"type": "array", "items": {"type": "integer"}}}}`),
			"",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string", "format": "uuid", "id": true}, "b": {"type": "array", "items": {"type": "integer"}}}}`),
			"",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string", "id": true}, "b": {"type": "object", "properties": { "a": {"type": "string", "id": true}}}}}`),
			"",
		},
		{
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string", "id": true}, "b": {"type": "object", "properties": { "a": {"type": "object", "properties": {"a": {"type": "string", "id": true}}}}}}}`),
			"",
		},
		{
			// top level non string field
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string", "id": true}, "b": {"type": "object", "properties": { "a": {"type": "integer", "id": true}}}}}`),
			"Cannot have field 'a' as 'id'. Only string type is supported as 'id' field",
		},
		{
			// marking object as an id field
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string", "id": true}, "b": {"type": "object", "properties": { "a": {"type": "object", "id": true, "properties": {"a": {"type": "string", "id": true}}}}}}}`),
			"Cannot have field 'a' as 'id'. Only string type is supported as 'id' field",
		},
		{
			// marking array as an id field
			[]byte(`{"title": "t1", "properties": { "a": {"type": "string"}, "b": {"type": "array", "items": {"type": "integer"}, "id": true}}}`),
			"Cannot have field 'b' as 'id'. Only string type is supported as 'id' field",
		},
	}
	for _, c := range cases {
		_, err := NewFactoryBuilder(true).BuildSearch("t1", c.schema)
		if len(c.expErrorMsg) > 0 {
			require.Contains(t, err.Error(), c.expErrorMsg)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestSearchIndex_Validate(t *testing.T) {
	reqSchema := []byte(`{
  "title": "t1",
  "properties": {
    "a": {
      "type": "integer",
      "sort": true,
      "facet": true
    },
    "b": {
      "type": "string",
      "sort": true,
      "facet": true
    },
    "c": {
      "type": "number",
      "sort": true,
      "facet": true
    },
    "d": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "e": {
      "type": "object",
      "properties": {
        "a": {
          "type": "integer"
        },
        "b": {
          "type": "string"
        },
        "c": {
          "type": "array",
          "format": "vector",
          "dimensions": 4
        },
        "d": {
          "type": "object",
          "properties": {
            "a": {
              "type": "array",
              "format": "vector",
              "dimensions": 4
            }
          }
        }
      }
    },
    "f": {
      "type": "array",
      "format": "vector",
      "dimensions": 2
    }
  }
}`)
	f, err := NewFactoryBuilder(true).BuildSearch("t1", reqSchema)
	require.NoError(t, err)

	idx := NewSearchIndex(0, "test", f, nil)
	cases := []struct {
		document []byte
		expError string
	}{
		{
			document: []byte(`{"a": 1, "b": "foo", "c": 1.01}`),
			expError: "",
		},
		{
			document: []byte(`{"a": 1, "b": "foo", "c": 1.01, "d": ["foo", "bar"]}`),
			expError: "",
		},
		{
			document: []byte(`{"a": 1, "b": "foo", "c": 1.01, "d": ["foo", "bar"], "e": {"a": 1, "b": "foo"}}`),
			expError: "",
		},
		{
			document: []byte(`{"a": 1, "b": "foo", "c": 1.01, "d": ["foo", "bar"], "e": {"a": 1, "b": "foo", "c": [0.12, 0.99]}, "f": [1.21, 2.93]}`),
			expError: "",
		},
		{
			document: []byte(`{"a": 1, "b": "foo", "c": 1.01, "d": ["foo", "bar"], "e": {"a": 1, "b": "foo", "c": [0.12, 0.99], "d": {"a": [0.12, 0.99]}}, "f": [1.21, 2.93]}`),
			expError: "",
		},
		{
			document: []byte(`{"a": 1, "b": "foo", "c": 1.01, "d": ["foo", "bar"], "e": {"a": 1, "b": "foo", "c": [0.12, 0.99, 1, 2, 3]}, "f": [1.21, 2.93]}`),
			expError: "field 'c' of vector type should not have dimensions greater than the defined in schema, defined: '4' found: '5'",
		},
		{
			document: []byte(`{"a": 1, "b": "foo", "c": 1.01, "d": ["foo", "bar"], "e": {"a": 1, "b": "foo", "c": [0.12, 0.99], "d": {"a": [0.12, 0.99, 1, 2, 3]}}, "f": [1.21, 2.93]}`),
			expError: "field 'a' of vector type should not have dimensions greater than the defined in schema, defined: '4' found: '5'",
		},
		{
			document: []byte(`{"a": 1, "b": "foo", "c": 1.01, "d": ["foo", "bar"], "e": {"a": 1, "b": "foo", "c": [0.12, 0.99], "d": {"a": [0.12, 0.99]}}, "f": [1.21, 2.93, 1]}`),
			expError: "field 'f' of vector type should not have dimensions greater than the defined in schema, defined: '2' found: '3'",
		},
	}
	for _, c := range cases {
		mp, err := util.JSONToMap(c.document)
		require.NoError(t, err)
		if len(c.expError) > 0 {
			require.ErrorContains(t, idx.Validate(mp), c.expError)
		} else {
			require.NoError(t, idx.Validate(mp))
		}
	}
}