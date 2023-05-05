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
	"bytes"
	"fmt"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/lib/uuid"
)

func TestCollection_SchemaValidate(t *testing.T) {
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
			"id_64": {
				"type": "integer",
				"format": "int64"
			},
			"random": {
				"type": "string",
				"format": "byte",
				"maxLength": 1024
			},
			"random_binary": {
				"type": "string",
				"format": ""
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
			"simple_items_string": {
				"type": "array",
				"items": {
					"type": "string"
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
						"id_product_items": {
							"type": "integer"
						}
					}
				}
			}
		},
		"primary_key": ["id"]
	}`)

	base64Encoded, err := jsoniter.Marshal([]byte(`"base64 string"`))
	require.NoError(t, err)
	cases := []struct {
		document []byte
		expError string
	}{
		{
			document: []byte(`{"id": 1, "product": "hello", "price": 1.01}`),
			expError: "",
		},
		{
			document: []byte(fmt.Sprintf(`{"id": 1, "product": "hello", "price": 1.01, "random": %s}`, string(base64Encoded))),
			expError: "",
		},
		{
			document: []byte(`{"id": 1, "price": 1}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 1.01}`),
			expError: "expected integer or null, but got number",
		},
		{
			document: []byte(`{"id": 1, "product": 1.01}`),
			expError: "expected string or null, but got number",
		},
		{
			document: []byte(`{"id": 1, "random": 1}`),
			expError: "expected string or null, but got number",
		},
		{
			document: []byte(`{"id": 1, "simple_items": ["1"]}`),
			expError: "expected integer or null, but got string",
		},
		{
			document: []byte(`{"id": 1, "simple_items": [1, 1.2]}`),
			expError: "expected integer or null, but got number",
		},
		{
			document: []byte(`{"id": 1, "simple_items": [1, 2]}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 1, "product_items": [1, 2]}`),
			expError: "expected object, but got number",
		},
		{
			document: []byte(`{"id": 1, "product_items": [{"id": 1, "item_name": 2}]}`),
			expError: "expected string or null, but got number",
		},
		{
			document: []byte(`{"id": 1, "id_32": null, "id_64": null}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 1, "product_items": [{"id": 1, "item_name": null}]}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 1, "product_items": [{"id": 1, "item_name": "foo"}]}`),
			expError: "",
		},
		{
			document: []byte(fmt.Sprintf(`{"id": 1, "id_uuid": "%s"}`, uuid.New().String())),
			expError: "",
		},
		{
			document: []byte(`{"id": 1, "id_uuid": "hello"}`),
			expError: "field 'id_uuid' reason ''hello' is not valid 'uuid'",
		},
		{
			document: []byte(`{"id": 1, "ts": "2015-12-21T17:42:34Z"}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 1, "ts": "2021-09-29T16:04:33.01234567Z"}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 1, "ts": "2016-02-15"}`),
			expError: "field 'ts' reason ''2016-02-15' is not valid 'date-time'",
		},
		{
			document: []byte(`{"id": 1, "random_binary": 1}`),
			expError: "expected string or null, but got number",
		},
		{
			document: []byte(fmt.Sprintf(`{"id": 1, "random_binary": "%s"}`, []byte(`1`))),
			expError: "",
		},
		{
			// if additional properties are set then reject the request
			document: []byte(fmt.Sprintf(`{"id": 1, "random_binary": "%s", "extra_key": "hello"}`, []byte(`1`))),
			expError: "reason 'additionalProperties 'extra_key' not allowed",
		},
		{
			document: []byte(`{"id": 123456789, "id_32": 2147483647}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 123456789, "id_32": 2147483648}`),
			expError: "reason '2147483648 is not valid 'int32'",
		},
		{
			document: []byte(`{"id": 123456789, "id_32": 2147483647, "id_64": 2147483648}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 123456789, "id_32": 2147483647, "id_64": 9223372036854775808}`),
			expError: "reason '9223372036854775808 is not valid 'int64'",
		},
		{
			document: []byte(`{"id": 1, "ts": null, "product": null, "simple_object": null, "simple_items": null}`),
			expError: "",
		},
		{
			document: []byte(`{"id": 1, "ts": null, "product": null, "simple_object": {"name": null}, "simple_items": [1, null], "simple_items_string": ["1", null]}`),
			expError: "",
		},
	}
	for _, c := range cases {
		schFactory, err := NewFactoryBuilder(true).Build("t1", reqSchema)
		require.NoError(t, err)

		coll, err := NewDefaultCollection(1, 1, schFactory, nil, nil)
		require.NoError(t, err)

		dec := jsoniter.NewDecoder(bytes.NewReader(c.document))
		dec.UseNumber()
		var v any
		require.NoError(t, dec.Decode(&v))
		if len(c.expError) > 0 {
			require.Contains(t, coll.Validate(v).Error(), c.expError)
		} else {
			require.NoError(t, coll.Validate(v))
		}
	}
}

func TestCollection_AdditionalProperties(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"simple_object": {
				"type": "object",
				"properties": {
					"name": { "type": "string" }
				}
			},
			"complex_object": {
				"type": "object",
				"properties": {
					"name": { "type": "string" },
					"obj": {
						"type": "object",
						"properties": {
							"name": { "type": "string" }
						}
					}
				}
			},
			"map": {
				"type": "object",
				"properties": {
					"name": { "type": "string" }
				},
				"additionalProperties":true
			}
		},
		"primary_key": ["id"]
	}`)

	cases := []struct {
		document []byte
		expError string
	}{
		{
			document: []byte(`{"id": 1, "simple_object": {"name": "hello", "price": 1.01}}`),
			expError: "json schema validation failed for field 'simple_object' reason 'additionalProperties 'price' not allowed'",
		},
		{
			document: []byte(`{"id": 1, "complex_object": {"name": "hello", "price": 1.01}}`),
			expError: "json schema validation failed for field 'complex_object' reason 'additionalProperties 'price' not allowed'",
		},
		{
			document: []byte(`{"id": 1, "complex_object": {"name": "hello", "obj": {"name": "hello", "price": 1.01}}}`),
			expError: "json schema validation failed for field 'complex_object/obj' reason 'additionalProperties 'price' not allowed'",
		},
		{
			document: []byte(`{"id": 1, "map": {"name": "hello", "some_prop" : "value1"}}`),
			expError: "",
		},
	}
	for _, c := range cases {
		schFactory, err := NewFactoryBuilder(true).Build("t1", reqSchema)
		require.NoError(t, err)

		coll, err := NewDefaultCollection(1, 1, schFactory, nil, nil)
		require.NoError(t, err)

		dec := jsoniter.NewDecoder(bytes.NewReader(c.document))
		dec.UseNumber()
		var v any
		require.NoError(t, dec.Decode(&v))

		var resMsg string
		if err = coll.Validate(v); err != nil {
			resMsg = err.Error()
		}
		require.Equal(t, c.expError, resMsg)
	}
}

func TestCollection_Object(t *testing.T) {
	reqSchema := []byte(`{
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

	cases := []struct {
		document []byte
	}{
		{
			document: []byte(`{"id": 1, "simple_object": {"name": "hello", "price": 1.01}}`),
		}, {
			document: []byte(`{"id": 1, "simple_object": {"name": "hello", "obj": {"name": "hello", "price": 1.01}}}`),
		},
	}
	for _, c := range cases {
		schFactory, err := NewFactoryBuilder(true).Build("t1", reqSchema)
		require.NoError(t, err)

		coll, err := NewDefaultCollection(1, 1, schFactory, nil, nil)
		require.NoError(t, err)

		dec := jsoniter.NewDecoder(bytes.NewReader(c.document))
		dec.UseNumber()
		var v any
		require.NoError(t, dec.Decode(&v))
		require.NoError(t, coll.Validate(v))
	}
}

func TestCollection_Int64(t *testing.T) {
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

	schFactory, err := NewFactoryBuilder(true).Build("t1", reqSchema)
	require.NoError(t, err)
	coll, err := NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(t, err)

	int64Paths := coll.int64FieldsPath.get()
	require.Equal(t, 4, len(int64Paths))
	_, ok := int64Paths["id"]
	require.True(t, ok)
	_, ok = int64Paths["nested_object.obj.intField"]
	require.True(t, ok)
	_, ok = int64Paths["array_items.id"]
	require.True(t, ok)
	_, ok = int64Paths["array_simple_items"]
	require.True(t, ok)
}
