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

package v1

import (
	"bytes"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/lib/json"
	"github.com/tigrisdata/tigris/schema"
)

func TestMutatePayload(t *testing.T) {
	reqSchema := []byte(`{
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

	schFactory, err := schema.Build("t1", reqSchema)
	require.NoError(t, err)
	coll := schema.NewDefaultCollection("t1", 1, 1, schFactory.CollectionType, schFactory.Fields, schFactory.Indexes, schFactory.Schema, "t1")
	require.Equal(t, 4, len(coll.Int64FieldsPath))

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
		doc, err := json.Decode(c.input)
		require.NoError(t, err)

		p := newPayloadMutator(coll)
		require.NoError(t, p.convertStringToInt64(doc))
		require.Equal(t, c.mutated, p.isMutated())

		actualJS, err := json.Encode(doc)
		require.NoError(t, err)
		require.JSONEq(t, string(c.output), string(actualJS))
	}
}

func BenchmarkStringToInteger(b *testing.B) {
	reqSchema := []byte(`{
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

	schFactory, err := schema.Build("t1", reqSchema)
	require.NoError(b, err)
	coll := schema.NewDefaultCollection("t1", 1, 1, schFactory.CollectionType, schFactory.Fields, schFactory.Indexes, schFactory.Schema, "t1")
	require.Equal(b, 4, len(coll.Int64FieldsPath))

	data := []byte(`{"name":"fiona handbag","id":9223372036854775800,"brand":"michael kors","nested_object":{"obj": {"intField": "9223372036854775800"}},"array_items":[{"name": "test0", "id": "9223372036854775800"}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": "9223372036854775802"}],"array_simple_items":[9223372036854775800, "9223372036854775801", 9223372036854775802]}`)
	var deserializedDoc map[string]interface{}
	dec := jsoniter.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		p := newPayloadMutator(coll)
		require.NoError(b, p.convertStringToInt64(deserializedDoc))
	}
}
