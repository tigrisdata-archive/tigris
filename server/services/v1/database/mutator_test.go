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

package database

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/util"
)

func TestMutateSetDefaults(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"double_f": {
				"type": "number",
				"default": 1.5
			},
			"created": {
				"type": "string",
				"format": "date-time",
				"createdAt": true
			},
			"updated": {
				"type": "string",
				"format": "date-time",
				"updatedAt": true
			},
			"arr": {
				"type": "array",
				"items": {
					"type": "integer"
				},
				"default": [10,20,30]
			}
		},
		"primary_key": ["id"]
	}`)

	schFactory, err := schema.NewFactoryBuilder(true).Build("t1", reqSchema)
	require.NoError(t, err)
	coll, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(t, err)
	p := newInsertPayloadMutator(coll, time.Now().UTC().String())

	cases := []struct {
		input   []byte
		mutated bool
		output  []byte
	}{
		{
			// created will be populated
			[]byte(`{"double_f":2,"arr":[1,2]}`),
			true,
			[]byte(fmt.Sprintf(`{"double_f":2,"arr":[1,2],"created":"%s"}`, p.(*insertPayloadMutator).createdAt)),
		},
		{
			// double_f will be populated
			[]byte(`{"arr":[1,2]}`),
			true,
			[]byte(fmt.Sprintf(`{"double_f":1.5,"arr":[1,2],"created":"%s"}`, p.(*insertPayloadMutator).createdAt)),
		},
		{
			// arr will be populated
			[]byte(`{"double_f":1.8}`),
			true,
			[]byte(fmt.Sprintf(`{"double_f":1.8,"arr":[10,20,30],"created":"%s"}`, p.(*insertPayloadMutator).createdAt)),
		},
	}
	for _, c := range cases {
		doc, err := util.JSONToMap(c.input)
		require.NoError(t, err)

		require.NoError(t, p.setDefaultsInIncomingPayload(doc))
		require.Equal(t, c.mutated, p.isMutated())
		actualJS, err := util.MapToJSON(doc)
		require.NoError(t, err)
		require.JSONEq(t, string(c.output), string(actualJS))
	}
}

func TestMutateSetDefaultsComplexSchema(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"int32": {
				"type": "integer",
				"default": 1
			},
			"str": {
				"type": "string",
				"default": "a"
			},
			"obj_a": {
				"type": "object"
			},
			"obj_n": {
				"type": "object",
				"properties": {
					"uid": { "type": "string", "default": "abc" },
					"obj": {
						"type": "object",
						"properties": {
							"int64_any": { "type": "integer", "default": 2 },
							"str_f": { "type": "string"}
						}
					}
				}
			},
			"arr_o": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"id": {
							"type": "integer",
							 "default": 3
						},
						"str_f": {
							"type": "string"
						}
					}
				}
			},
			"arr_s": {
				"type": "array",
				"items": {
					"type": "integer"
				}
			}
		},
		"primary_key": ["id"]
	}`)

	schFactory, err := schema.NewFactoryBuilder(true).Build("t1", reqSchema)
	require.NoError(t, err)
	coll, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(t, err)

	cases := []struct {
		input   []byte
		mutated bool
		output  []byte
	}{
		{
			// no defaults, doc has all the value pre-populated
			[]byte(`{"obj_n":{"obj":{"str_f":"test","int64_any":20},"uid":"a"},"arr_o":[{"str_f":"t0","id":1},{"str_f":"t1","id":2},{"str_f":"t2","id":3}],"int32":10,"str":"aaa"}`),
			false,
			[]byte(`{"obj_n":{"obj":{"str_f":"test","int64_any":20},"uid":"a"},"arr_o":[{"str_f":"t0","id":1},{"str_f":"t1","id":2},{"str_f":"t2","id":3}],"int32":10,"str":"aaa"}`),
		},
		{
			// obj_n is missing
			[]byte(`{"arr_o":[{"str_f":"t0","id":1},{"str_f":"t1","id":2},{"str_f":"t2","id":3}]}`),
			true,
			[]byte(`{"obj_n":{"obj":{"int64_any":2},"uid":"abc"},"int32":1,"str":"a","arr_o":[{"str_f":"t0","id":1},{"str_f":"t1","id":2},{"str_f":"t2","id":3}]}`),
		},
		{
			// arr_o is missing
			[]byte(`{"obj_n":{"obj":{"int64_any":20},"uid":"a"}}`),
			true,
			[]byte(`{"obj_n":{"obj":{"int64_any":20},"uid":"a"},"int32":1,"str":"a"}`),
		},
		{
			[]byte(`{"id":1,"obj_a":{"s":"a"},"obj_n":{"obj":{"str_f":"test"}},"arr_o":[{"str_f":"t0","id":1},{"str_f":"t1","id":2},{"str_f":"t2","id":3}],"arr_s":[1,2,3]}`),
			true,
			[]byte(`{"id":1,"obj_a":{"s":"a"},"obj_n":{"obj":{"str_f":"test","int64_any":2},"uid":"abc"},"arr_o":[{"str_f":"t0","id":1},{"str_f":"t1","id":2},{"str_f":"t2","id":3}],"arr_s":[1,2,3],"int32":1,"str":"a"}`),
		},
	}
	for _, c := range cases {
		doc, err := util.JSONToMap(c.input)
		require.NoError(t, err)

		p := newInsertPayloadMutator(coll, time.Now().UTC().String())
		require.NoError(t, p.setDefaultsInIncomingPayload(doc))
		require.Equal(t, c.mutated, p.isMutated())
		actualJS, err := util.MapToJSON(doc)
		require.NoError(t, err)
		require.JSONEq(t, string(c.output), string(actualJS))
	}
}

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

	schFactory, err := schema.NewFactoryBuilder(true).Build("t1", reqSchema)
	require.NoError(t, err)
	coll, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 4, len(coll.GetInt64FieldsPath()))

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

		p := newInsertPayloadMutator(coll, time.Now().UTC().String())
		require.NoError(t, p.stringToInt64(doc))
		require.Equal(t, c.mutated, p.isMutated())

		actualJS, err := util.MapToJSON(doc)
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

	schFactory, err := schema.NewFactoryBuilder(true).Build("t1", reqSchema)
	require.NoError(b, err)
	coll, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(b, err)
	require.Equal(b, 4, len(coll.GetInt64FieldsPath()))

	data := []byte(`{"name":"fiona handbag","id":9223372036854775800,"brand":"michael kors","nested_object":{"obj": {"intField": "9223372036854775800"}},"array_items":[{"name": "test0", "id": "9223372036854775800"}, {"name": "test1", "id": 9223372036854775801}, {"name": "test2", "id": "9223372036854775802"}],"array_simple_items":[9223372036854775800, "9223372036854775801", 9223372036854775802]}`)
	var deserializedDoc map[string]interface{}
	dec := jsoniter.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		p := newInsertPayloadMutator(coll, time.Now().UTC().String())
		require.NoError(b, p.stringToInt64(deserializedDoc))
	}
}
