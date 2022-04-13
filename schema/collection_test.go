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
