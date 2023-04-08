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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/util"
)

func TestStringToInt64Converter_Convert(t *testing.T) {
	reqCollectionSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"simple_object": {
				"type": "object"
			},
			"nested_object": {
				"type": "object",
				"properties": {
					"name": { "type": "string" },
					"obj": {
						"type": "object",
						"properties": {
							"intField": { "type": "integer" }
						}
					}
				}
			},
			"array_items": {
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
			},
			"array_simple_items": {
				"type": "array",
				"items": {
					"type": "integer"
				}
			}
		},
		"primary_key": ["id"]
	}`)

	schFactory, err := schema.NewFactoryBuilder(true).Build("t1", reqCollectionSchema)
	require.NoError(t, err)
	coll, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(t, err)

	testStringToInt64Converter(t, coll.GetField, coll.GetInt64FieldsPath())

	reqIndexSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"simple_object": {
				"type": "object"
			},
			"nested_object": {
				"type": "object",
				"properties": {
					"name": { "type": "string" },
					"obj": {
						"type": "object",
						"properties": {
							"intField": { "type": "integer" }
						}
					}
				}
			},
			"array_items": {
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
			},
			"array_simple_items": {
				"type": "array",
				"items": {
					"type": "integer"
				}
			}
		},
		"primary_key": ["id"]
	}`)

	searchFactory, err := schema.NewFactoryBuilder(true).BuildSearch("t1", reqIndexSchema)
	require.NoError(t, err)
	index := schema.NewSearchIndex(1, "t1", searchFactory, nil)
	require.NoError(t, err)

	testStringToInt64Converter(t, index.GetField, index.GetInt64FieldsPath())
}

func testStringToInt64Converter(t *testing.T, accessor fieldAccessor, paths map[string]struct{}) {
	cases := []struct {
		input   []byte
		mutated bool
		output  []byte
	}{
		// top level
		{
			[]byte(`{"name":"test","id":"9223372036854775800","brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
			true,
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
		},
		// nested object
		{
			[]byte(`{"name":"test","id":"9223372036854775800","brand":"random","nested_object":{"obj": {"intField": "9223372036854775800"}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
			true,
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
		},
		// all elements of array

		{
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": "9223372036854775800"}, {"name": "test1", "id": "9223372036854775801"}, {"name": "test2", "id": "9223372036854775802"}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
			true,
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
		},
		// all elements of primitive array

		{
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":["9223372036854775800", "9223372036854775801", "9223372036854775802"]}`),
			true,
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
		},
		// mixed int and string combos
		{
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": "9223372036854775800"}},"array_items":[{"name": "test0", "id": "9223372036854775800"}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": "9223372036854775802"}],"array_simple_items":[9223372036854775800, "9223372036854775801", 9223372036854775802]}`),
			true,
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
		},
		// mixed int and string combos
		{
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": "9223372036854775800"}},"array_items":[{"name": "test0", "id": "9223372036854775800"}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": "9223372036854775802"}],"array_simple_items":[9223372036854775800, "9223372036854775801", 9223372036854775802]}`),
			true,
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
		},
		// no changes
		{
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
			false,
			[]byte(`{"name":"test","id":9223372036854775800,"brand":"random","nested_object":{"obj": {"intField": 9223372036854775800}},"array_items":[{"name": "test0", "id": 9223372036854775800}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": 9223372036854775802}],"array_simple_items":[9223372036854775800, 9223372036854775801, 9223372036854775802]}`),
		},
	}
	for _, c := range cases {
		doc, err := util.JSONToMap(c.input)
		require.NoError(t, err)

		p := NewStringToInt64Converter(accessor)
		converted, err := p.Convert(doc, paths)
		require.NoError(t, err)
		require.Equal(t, c.mutated, converted)

		actualJS, err := util.MapToJSON(doc)
		require.NoError(t, err)
		require.JSONEq(t, string(c.output), string(actualJS))
	}
}
