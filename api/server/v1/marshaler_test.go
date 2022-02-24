package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDocument(t *testing.T) {
	// ToDo: add marshaler tests
	inputDoc := []byte(`{"pkey_int": 1, "int_value": 2, "str_value": "foo"}`)
	b, err := json.Marshal(inputDoc)
	require.NoError(t, err)

	var bb []byte
	require.NoError(t, json.Unmarshal(b, &bb))
}
