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

package update

import (
	"fmt"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func TestMergeAndGet(t *testing.T) {
	cases := []struct {
		inputDoc    jsoniter.RawMessage
		existingDoc jsoniter.RawMessage
		outputDoc   jsoniter.RawMessage
		apply       FieldOPType
	}{
		{
			[]byte(`{"a": 10}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 10, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			set,
		}, {
			[]byte(`{"b": "bar", "a": 10}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 10, "b": "bar", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			set,
		}, {
			[]byte(`{"b": "test", "c": 10.22}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 1, "b": "test", "c": 10.22, "d": {"f": 22, "g": 44}}`),
			set,
		}, {
			[]byte(`{"c": 10.000022, "e": "new"}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 1, "b": "foo", "c": 10.000022, "d": {"f": 22, "g": 44},"e":"new"}`),
			set,
		}, {
			[]byte(`{"e": "again", "a": 1.000000022, "c": 23}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 1.000000022, "b": "foo", "c": 23, "d": {"f": 22, "g": 44},"e":"again"}`),
			set,
		},
	}
	for _, c := range cases {
		reqInput := []byte(fmt.Sprintf(`{"%s": %s}`, c.apply, c.inputDoc))
		f, err := BuildFieldOperators(reqInput)
		require.NoError(t, err)

		actualOut, err := f.MergeAndGet(c.existingDoc)
		require.NoError(t, err)
		require.Equal(t, c.outputDoc, actualOut, fmt.Sprintf("exp '%s' actual '%s'", string(c.outputDoc), string(actualOut)))
	}
}

func TestMergeAndGet_MarshalInput(t *testing.T) {
	cases := []struct {
		inputDoc    map[string]interface{}
		existingDoc map[string]interface{}
		outputDoc   jsoniter.RawMessage
		apply       FieldOPType
	}{
		{
			map[string]interface{}{
				"int_value":    200,
				"string_value": "simple_insert1_update_modified",
				"bool_value":   false,
				"double_value": 200.00001,
				"bytes_value":  []byte(`"simple_insert1_update_modified"`),
			},
			map[string]interface{}{
				"pkey_int":     100,
				"int_value":    100,
				"string_value": "simple_insert1_update",
				"bool_value":   true,
				"double_value": 100.00001,
				"bytes_value":  []byte(`"simple_insert1_update"`),
			},
			[]byte(`{"pkey_int":100,"int_value":200,"string_value":"simple_insert1_update_modified","bool_value":false,"double_value":200.00001,"bytes_value":"InNpbXBsZV9pbnNlcnQxX3VwZGF0ZV9tb2RpZmllZCI="}`),
			set,
		},
	}
	for _, c := range cases {
		var reqInput = make(map[string]interface{})
		reqInput[string(c.apply)] = c.inputDoc
		input, err := jsoniter.Marshal(reqInput)
		require.NoError(t, err)
		f, err := BuildFieldOperators(input)
		require.NoError(t, err)
		existingDoc, err := jsoniter.Marshal(c.existingDoc)
		require.NoError(t, err)
		actualOut, err := f.MergeAndGet(existingDoc)
		require.NoError(t, err)
		require.JSONEqf(t, string(c.outputDoc), string(actualOut), fmt.Sprintf("exp '%s' actual '%s'", string(c.outputDoc), string(actualOut)))
	}
}
