package schema

import (
	"fmt"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func TestCollection_SchemaValidate(t *testing.T) {
	reqSchema := []byte(`{
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
					}
				}
			}
		}
	},
	"primary_key": ["id"]
}`)

	cases := []struct {
		document []byte
		expError string
	}{
		{
			document: []byte(`{"id": 1, "product": "hello", "price": 1.01}`),
			expError: "",
		}, {
			document: []byte(fmt.Sprintf(`{"id": 1, "product": "hello", "price": 1.01, "random": "%v"}`, []byte(`hello`))),
			expError: "",
		}, {
			document: []byte(`{"id": 1, "price": 1}`),
			expError: "",
		}, {
			document: []byte(`{"id": 1.01}`),
			expError: "expected integer, but got number",
		}, {
			document: []byte(`{"id": 1, "product": 1.01}`),
			expError: "expected string, but got number",
		}, {
			document: []byte(`{"id": 1, "random": 1}`),
			expError: "expected string, but got number",
		}, {
			document: []byte(`{"id": 1, "simple_items": ["1"]}`),
			expError: "expected integer, but got string",
		}, {
			document: []byte(`{"id": 1, "simple_items": [1, 1.2]}`),
			expError: "expected integer, but got number",
		}, {
			document: []byte(`{"id": 1, "simple_items": [1, 2]}`),
			expError: "",
		}, {
			document: []byte(`{"id": 1, "product_items": [1, 2]}`),
			expError: "expected object, but got number",
		}, {
			document: []byte(`{"id": 1, "product_items": [{"id": 1, "item_name": 2}]}`),
			expError: "expected string, but got number",
		}, {
			document: []byte(`{"id": 1, "product_items": [{"id": 1, "item_name": "foo"}]}`),
			expError: "",
		},
	}
	for _, c := range cases {
		schFactory, err := Build("t1", reqSchema)
		require.NoError(t, err)

		coll := NewDefaultCollection("t1", 1, schFactory.Fields, schFactory.Indexes, schFactory.Schema)
		var v interface{}
		err = jsoniter.Unmarshal(c.document, &v)
		require.NoError(t, err)
		if len(c.expError) > 0 {
			require.Contains(t, coll.Validate(v).Error(), c.expError)
		} else {
			require.NoError(t, coll.Validate(v))
		}
	}
}
