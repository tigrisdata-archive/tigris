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

package update

import (
	"fmt"
	"testing"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/util"
)

func TestMergeAndGet(t *testing.T) {
	cases := []struct {
		inputDoc        jsoniter.RawMessage
		existingDoc     jsoniter.RawMessage
		outputDoc       jsoniter.RawMessage
		expKeysToRemove []string
		apply           FieldOPType
	}{
		{
			[]byte(`{"a": 10}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 10, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			nil,
			Set,
		}, {
			[]byte(`{"b": "bar", "a": 10}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 10, "b": "bar", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			nil,
			Set,
		}, {
			[]byte(`{"b": "test", "c": 10.22}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 1, "b": "test", "c": 10.22, "d": {"f": 22, "g": 44}}`),
			nil,
			Set,
		}, {
			[]byte(`{"c": 10.000022, "e": "new"}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 1, "b": "foo", "c": 10.000022, "d": {"f": 22, "g": 44},"e":"new"}`),
			nil,
			Set,
		}, {
			[]byte(`{"e": "again", "a": 1.000000022, "c": 23}`),
			[]byte(`{"a": 1, "b": "foo", "c": 1.01, "d": {"f": 22, "g": 44}}`),
			[]byte(`{"a": 1.000000022, "b": "foo", "c": 23, "d": {"f": 22, "g": 44},"e":"again"}`),
			nil,
			Set,
		}, {
			[]byte(`{"e": "again", "d.f": 29, "d.g": "bar", "d.h": "new nested"}`),
			[]byte(`{"a":1, "b":"foo", "c":1.01, "d": {"f": 22, "g": "foo"}}`),
			[]byte(`{"a":1, "b":"foo", "c":1.01, "d": {"f": 29, "g": "bar","h":"new nested"},"e":"again"}`),
			nil,
			Set,
		}, {
			[]byte(`{"e": "again", "d": {"f": 10}}`),
			[]byte(`{"a":1, "b":"foo", "c":1.01, "d": {"f": 22, "g": "foo"}}`),
			[]byte(`{"a":1, "b":"foo", "c":1.01, "d": {"f": 10},"e":"again"}`),
			[]string{"d.f", "d.g"},
			Set,
		}, {
			[]byte(`{"d": {"n": {"b": "BB"}}}`),
			[]byte(`{"a":1, "d": {"f": 22, "g": "foo", "n": {"a": "A", "b": "B"}}}`),
			[]byte(`{"a":1, "d": {"n": {"b": "BB"}}}`),
			[]string{"d.f", "d.g", "d.n.a", "d.n.b"},
			Set,
		},
	}
	for _, c := range cases {
		reqInput := []byte(fmt.Sprintf(`{"%s": %s}`, c.apply, c.inputDoc))
		f, err := BuildFieldOperators(reqInput)
		require.NoError(t, err)

		actualOut, keysToRemove, pkeyMutation, err := f.MergeAndGet(c.existingDoc, testCollection(t))
		require.False(t, pkeyMutation)
		require.NoError(t, err)
		require.Equal(t, c.expKeysToRemove, keysToRemove)
		require.Equal(t, c.outputDoc, actualOut, fmt.Sprintf("exp '%s' actual '%s'", string(c.outputDoc), string(actualOut)))
	}
}

func TestMergeAndGet_Unset(t *testing.T) {
	cases := []struct {
		inputSet    jsoniter.RawMessage
		inputRemove jsoniter.RawMessage
		existingDoc jsoniter.RawMessage
		outputDoc   jsoniter.RawMessage
	}{
		{
			[]byte(`{"a":10}`),
			[]byte(`["a", "nested"]`),
			[]byte(`{"a":1,"b":"first","c":1.01,"nested":{"f":22,"g":44}}`),
			[]byte(`{"b":"first","c":1.01}`),
		}, {
			[]byte(`{"b":"second","a":10}`),
			[]byte(`["c"]`),
			[]byte(`{"a":1,"b":"first","c":1.01,"nested":{"f":22,"g":44}}`),
			[]byte(`{"a":10,"b":"second","nested":{"f":22,"g":44}}`),
		}, {
			[]byte(`{"b":"second","c":10.22}`),
			[]byte(`["nested.f"]`),
			[]byte(`{"a":1,"b":"first","c":1.01,"nested":{"f":22,"g":44}}`),
			[]byte(`{"a":1,"b":"second","c":10.22,"nested":{"g":44}}`),
		}, {
			[]byte(`{"c":10.000022,"e":"not_present"}`),
			[]byte(`["nested.f", "nested.g"]`),
			[]byte(`{"a":1,"b":"first","c":1.01,"nested":{"f":22,"g": 4}}`),
			[]byte(`{"a":1,"b":"first","c":10.000022,"nested":{},"e":"not_present"}`),
		}, {
			[]byte(`{"e":"not_present","a":1.000000022,"c":23}`),
			[]byte(`["c", "b"]`),
			[]byte(`{"a":1,"b":"first","c":1.01,"nested":{"f":22,"g":44}}`),
			[]byte(`{"a":1.000000022,"nested":{"f":22,"g":44},"e":"not_present"}`),
		}, {
			[]byte(`{"e":"not_present","nested.f":29,"nested.g":"bar","nested.h":"new nested"}`),
			[]byte(`["z", "y"]`),
			[]byte(`{"a":1,"b":"first","c":1.01,"nested":{"f":22,"g":"foo"}}`),
			[]byte(`{"a":1,"b":"first","c":1.01,"nested":{"f":29,"g":"bar","h":"new nested"},"e":"not_present"}`),
		},
	}
	for _, c := range cases {
		reqInput := []byte(fmt.Sprintf(`{"$set": %s, "$unset": %s}`, c.inputSet, c.inputRemove))
		f, err := BuildFieldOperators(reqInput)
		require.NoError(t, err)

		actualOut, keysToRemove, pkeyMutation, err := f.MergeAndGet(c.existingDoc, testCollection(t))
		require.False(t, pkeyMutation)
		require.NoError(t, err)
		require.Nil(t, keysToRemove)
		require.Equal(t, c.outputDoc, actualOut, fmt.Sprintf("exp '%s' actual '%s'", string(c.outputDoc), string(actualOut)))
	}
}

func TestMergeAndGet_Atomic(t *testing.T) {
	cases := []struct {
		inputDoc    jsoniter.RawMessage
		existingDoc jsoniter.RawMessage
		outputDoc   jsoniter.RawMessage
		apply       FieldOPType
	}{
		{
			[]byte(`{"f_32": 10, "f_num": 2.12, "f_obj.f_64": -1}`),
			[]byte(`{"f_32": 1, "f_str": "foo", "f_num": 1.01, "f_obj": {"f_64": 22}}`),
			[]byte(`{"f_32": 11, "f_str": "foo", "f_num": 3.13, "f_obj": {"f_64": 21}}`),
			Increment,
		}, {
			[]byte(`{"f_32": 1, "f_obj.f_64": -1}`),
			[]byte(`{"f_obj": {"f_str": "hello"}}`),
			[]byte(`{"f_obj": {"f_str": "hello"}}`),
			Increment,
		}, {
			[]byte(`{"f_32": 1, "f_num": 1}`),
			[]byte(`{"f_32": 1, "f_num": 2.1}`),
			[]byte(`{"f_32": 0, "f_num": 1.1}`),
			Decrement,
		}, {
			[]byte(`{"f_32": 4, "f_num": -2.01}`),
			[]byte(`{"f_32": 2, "f_num": 1.01}`),
			[]byte(`{"f_32": 8, "f_num": -2.0301}`),
			Multiply,
		}, {
			[]byte(`{"f_32": 4, "f_num": 4}`),
			[]byte(`{"f_32": 2, "f_num": 2}`),
			[]byte(`{"f_32": 0, "f_num": 0.50}`),
			Divide,
		},
	}
	for _, c := range cases {
		reqInput := []byte(fmt.Sprintf(`{"%s": %s}`, c.apply, c.inputDoc))
		f, err := BuildFieldOperators(reqInput)
		require.NoError(t, err)

		actualOut, keysToRemove, pkeyMutation, err := f.MergeAndGet(c.existingDoc, testCollection(t))
		require.False(t, pkeyMutation)
		require.NoError(t, err)
		require.Nil(t, keysToRemove)
		require.JSONEq(t, string(c.outputDoc), string(actualOut), fmt.Sprintf("exp '%s' actual '%s'", string(c.outputDoc), string(actualOut)))
	}
}

func TestMergeAndGet_AtomicErrors(t *testing.T) {
	cases := []struct {
		inputDoc    jsoniter.RawMessage
		existingDoc jsoniter.RawMessage
		error       error
		apply       FieldOPType
	}{
		{
			[]byte(`{"f_32": 10.01}`),
			[]byte(`{"f_32": 1, "f_str": "foo", "f_num": 1.01, "f_obj": {"f_64": 22}}`),
			errors.InvalidArgument("floating operations are not allowed on integer field"),
			Increment,
		}, {
			[]byte(`{"f_32": 0}`),
			[]byte(`{"f_32": 1, "f_str": "foo", "f_num": 1.01, "f_obj": {"f_64": 22}}`),
			errors.InvalidArgument("division by 0 is not allowed"),
			Divide,
		},
	}
	for _, c := range cases {
		reqInput := []byte(fmt.Sprintf(`{"%s": %s}`, c.apply, c.inputDoc))
		f, err := BuildFieldOperators(reqInput)
		require.NoError(t, err)

		actualOut, keysToRemove, pkeyMutation, err := f.MergeAndGet(c.existingDoc, testCollection(t))
		require.Equal(t, c.error, err)
		require.False(t, pkeyMutation)
		require.Nil(t, keysToRemove)
		require.Nil(t, actualOut)
	}
}

func TestMergeAndGet_Push(t *testing.T) {
	cases := []struct {
		inputDoc    jsoniter.RawMessage
		existingDoc jsoniter.RawMessage
		outputDoc   jsoniter.RawMessage
	}{
		{
			[]byte(`{"f_int": 1}`),
			[]byte(`{}`),
			[]byte(`{"f_int": [1]}`),
		},
		{
			[]byte(`{"f_int": 2}`),
			[]byte(`{"f_int": [1]}`),
			[]byte(`{"f_int": [1, 2]}`),
		},
		{
			[]byte(`{"f_str": "world"}`),
			[]byte(`{"f_str": ["hello"]}`),
			[]byte(`{"f_str": ["hello", "world"]}`),
		},
		{
			[]byte(`{"f_obj.a": 4}`),
			[]byte(`{}`),
			[]byte(`{"f_obj": {"a": [4]}}`),
		},
		{
			[]byte(`{"f_obj.a": 4}`),
			[]byte(`{"f_obj": {"a": [1, 2, 3]}}`),
			[]byte(`{"f_obj": {"a": [1, 2, 3, 4]}}`),
		},
		{
			[]byte(`{"f_obj.b": "hello"}`),
			[]byte(`{"f_obj": {"b": []}}`),
			[]byte(`{"f_obj": {"b": ["hello"]}}`),
		},
		{
			[]byte(`{"f_obj_arr": {"c": 1 , "d": "hello"}}`),
			[]byte(`{"g": "hello"}`),
			[]byte(`{"g": "hello", "f_obj_arr": [{"c": 1 , "d": "hello"}]}`),
		},
		{
			[]byte(`{"f_obj_arr": {"c": 2 , "d": "world"}}`),
			[]byte(`{"f_obj_arr": [{"c": 1 , "d": "hello"}]}`),
			[]byte(`{"f_obj_arr": [{"c": 1 , "d": "hello"}, {"c": 2 , "d": "world"}]}`),
		},
	}

	for _, c := range cases {
		reqInput := []byte(fmt.Sprintf(`{"%s": %s}`, Push, c.inputDoc))
		f, err := BuildFieldOperators(reqInput)
		require.NoError(t, err)

		actualOut, keysToRemove, pkeyMutation, err := f.MergeAndGet(c.existingDoc, testCollection3(t))
		require.False(t, pkeyMutation)
		require.NoError(t, err)
		require.Nil(t, keysToRemove)
		require.JSONEq(t, string(c.outputDoc), string(actualOut), fmt.Sprintf("exp '%s' actual '%s'", string(c.outputDoc), string(actualOut)))
	}
}

func TestMergeAndGet_PrimaryKeyMutation(t *testing.T) {
	cases := []struct {
		inputDoc           jsoniter.RawMessage
		existingDoc        jsoniter.RawMessage
		outputDoc          jsoniter.RawMessage
		apply              FieldOPType
		primaryKeyMutation bool
		expError           error
	}{
		{
			[]byte(`{"a": 10}`),
			[]byte(`{"a": 1, "b": "foo", "c": "c", "f": {"f_str": "f"}}`),
			[]byte(`{"a": 10, "b": "foo", "c": "c", "f": {"f_str": "f"}}`),
			Set,
			true,
			nil,
		},
		{
			[]byte(`{"b": 10, "a": 10}`),
			[]byte(`{"a": 1, "b": 1, "c": "c", "f": {"f_str": "f"}}`),
			[]byte(`{"a": 10, "b": 10, "c": "c", "f": {"f_str": "f"}}`),
			Set,
			true,
			nil,
		},
		{
			[]byte(`{"b": 1, "d": 10.22}`),
			[]byte(`{"a": 1, "b": 10, "d": 1.22}`),
			[]byte(`{"a": 1, "b": 1, "d": 10.22}`),
			Set,
			false,
			nil,
		},
		{
			[]byte(`{"f.f_str": "ff", "g": "new"}`),
			[]byte(`{"a": 1, "c": "c", "f": {"f_str": "f"}}`),
			[]byte(`{"a": 1, "c": "c", "f": {"f_str": "ff"},"g":"new"}`),
			Set,
			false,
			nil,
		},
		{
			[]byte(`{"a": 1, "c": "foo"}`),
			[]byte(`{"a": 10, "b": 10, "d": 1.22}`),
			[]byte(`{"a": 1, "b": 10, "d": 1.22,"c":"foo"}`),
			Set,
			true,
			nil,
		},
		{
			[]byte(`["a", "b"]`),
			[]byte(`{"a": 10, "b": 10, "d": 1.22}`),
			[]byte(`{ "d": 1.22}`),
			UnSet,
			true,
			errors.InvalidArgument("primary key field can't be unset"),
		},
		{
			[]byte(`["b", "d"]`),
			[]byte(`{"a": 10, "b": 10, "d": 1.22}`),
			[]byte(`{"a": 10}`),
			UnSet,
			false,
			nil,
		},
		{
			[]byte(`{"a": 10}`),
			[]byte(`{"a": 1, "b": 10, "d": 1.22}`),
			[]byte(`{"a": 11, "b": 10, "d": 1.22}`),
			Increment,
			true,
			nil,
		},
		{
			[]byte(`{"b": 10}`),
			[]byte(`{"a": 1, "b": 10, "d": 1.22}`),
			[]byte(`{"a": 1, "b": 20, "d": 1.22}`),
			Increment,
			false,
			nil,
		},
		{
			inputDoc:           []byte(`{"e": 4}`),
			existingDoc:        []byte(`{"a": 1, "e": [], "d": 1.22}`),
			outputDoc:          []byte(`{"a": 1, "e": [4], "d": 1.22}`),
			apply:              Push,
			primaryKeyMutation: false,
			expError:           nil,
		},
		{
			inputDoc:           []byte(`{"e": 4}`),
			existingDoc:        []byte(`{"a": 1, "e": [1,2,3], "d": 1.22}`),
			outputDoc:          []byte(`{"a": 1, "e": [1,2,3,4], "d": 1.22}`),
			apply:              Push,
			primaryKeyMutation: false,
			expError:           nil,
		},
	}
	for _, c := range cases {
		reqInput := []byte(fmt.Sprintf(`{"%s": %s}`, c.apply, c.inputDoc))
		f, err := BuildFieldOperators(reqInput)
		require.NoError(t, err)

		actualOut, keysToRemove, pkeyMutation, err := f.MergeAndGet(c.existingDoc, testCollection2(t))
		require.Equal(t, c.expError, err)
		if c.expError != nil {
			continue
		}
		require.Nil(t, keysToRemove)
		require.Equal(t, c.primaryKeyMutation, pkeyMutation)
		require.Equal(t, c.outputDoc, actualOut, fmt.Sprintf("exp '%s' actual '%s'", string(c.outputDoc), string(actualOut)))
	}
}

func TestMergeAndGet_MarshalInput(t *testing.T) {
	cases := []struct {
		inputDoc    map[string]any
		existingDoc map[string]any
		outputDoc   jsoniter.RawMessage
		apply       FieldOPType
	}{
		{
			map[string]any{
				"int_value":    200,
				"string_value": "simple_insert1_update_modified",
				"bool_value":   false,
				"double_value": 200.00001,
				"bytes_value":  []byte(`"simple_insert1_update_modified"`),
			},
			map[string]any{
				"pkey_int":     100,
				"int_value":    100,
				"string_value": "simple_insert1_update",
				"bool_value":   true,
				"double_value": 100.00001,
				"bytes_value":  []byte(`"simple_insert1_update"`),
			},
			[]byte(`{"pkey_int":100,"int_value":200,"string_value":"simple_insert1_update_modified","bool_value":false,"double_value":200.00001,"bytes_value":"InNpbXBsZV9pbnNlcnQxX3VwZGF0ZV9tb2RpZmllZCI="}`),
			Set,
		},
	}
	for _, c := range cases {
		reqInput := make(map[string]any)
		reqInput[string(c.apply)] = c.inputDoc
		input, err := jsoniter.Marshal(reqInput)
		require.NoError(t, err)
		f, err := BuildFieldOperators(input)
		require.NoError(t, err)
		existingDoc, err := jsoniter.Marshal(c.existingDoc)
		require.NoError(t, err)
		actualOut, keysToRemove, pkeyMutation, err := f.MergeAndGet(existingDoc, testCollection(t))
		require.NoError(t, err)
		require.False(t, pkeyMutation)
		require.Nil(t, keysToRemove)
		require.JSONEqf(t, string(c.outputDoc), string(actualOut), fmt.Sprintf("exp '%s' actual '%s'", string(c.outputDoc), string(actualOut)))
	}
}

func BenchmarkSetNoDeserialization(b *testing.B) {
	existingDoc := []byte(`{
	"name": "Women's Fiona Handbag",
	"brand": "Michael Cors",
	"labels": "Handbag, Purse, Women's fashion",
	"price": 99999.12345,
	"key": "1",
	"categories": ["random", "fashion", "handbags", "women's"],
	"description": "A typical product catalog will have many json objects like this. This benchmark is testing if not deserializing is better than deserializing JSON inputs and existing doc.",
	"random": "abc defg hij klm nopqr stuv wxyz 1234 56 78 90 abcd efghijkl mnopqrstuvwxyzA BCD EFGHIJKL MNOPQRS TUVW XYZ"
}`)

	f, err := BuildFieldOperators([]byte(`{"$set": {"b": "bar", "a": 10}}`))
	require.NoError(b, err)
	for i := 0; i < b.N; i++ {
		err = f.testSetNoDeserialization(existingDoc, []byte(`{"$set": {"name": "Men's Wallet", "labels": "Handbag, Purse, Men's fashion, shoes, clothes", "price": 75}}`))
		require.NoError(b, err)
	}
}

func BenchmarkSetDeserializeInput(b *testing.B) {
	existingDoc := []byte(`{
	"name": "Women's Fiona Handbag",
	"brand": "Michael Cors",
	"labels": "Handbag, Purse, Women's fashion",
	"price": 99999.12345,
	"key": "1",
	"categories": ["random", "fashion", "handbags", "women's"],
	"description": "A typical product catalog will have many json objects like this. This benchmark is testing if deserializing is better than not deserializing JSON inputs and existing doc.",
	"random": "abc defg hij klm nopqr stuv wxyz 1234 56 78 90 abcd efghijkl mnopqrstuvwxyzA BCD EFGHIJKL MNOPQRS TUVW XYZ"
}`)

	f, err := BuildFieldOperators([]byte(`{"$set": {"b": "bar", "a": 10}}`))
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		mp, err := util.JSONToMap(existingDoc)
		require.NoError(b, err)

		err = f.testSetDeserializeInput(mp, []byte(`{"$set": {"name": "Men's Wallet", "labels": "Handbag, Purse, Men's fashion, shoes, clothes", "price": 75}}`))
		require.NoError(b, err)
	}
}

func (*FieldOperatorFactory) testSetDeserializeInput(outMap map[string]any, setDoc jsoniter.RawMessage) error {
	setMap, err := util.JSONToMap(setDoc)
	if err != nil {
		return err
	}

	for key, value := range setMap {
		outMap[key] = value
	}

	return nil
}

func (*FieldOperatorFactory) testSetNoDeserialization(input jsoniter.RawMessage, setDoc jsoniter.RawMessage) error {
	var (
		output []byte = input
		err    error
	)
	err = jsonparser.ObjectEach(setDoc, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		if dataType == jsonparser.String {
			value = []byte(fmt.Sprintf(`"%s"`, value))
		}
		output, err = jsonparser.Set(output, value, string(key))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func testCollection(t *testing.T) *schema.DefaultCollection {
	reqSchema := []byte(`{
	"title": "test_update",
	"properties": {
		"id": {
			"type": "integer"
		},
		"f_32": {
			"type": "integer",
			"format": "int32"
		},
		"f_64": {
			"type": "integer"
		},
		"f_str": {
			"type": "string",
			"maxLength": 100
		},
		"f_num": {
			"type": "number"
		},
		"f_arr": {
			"type": "array",
			"items": {
				"type": "integer"
			}
		},
		"f_obj": {
			"type": "object",
			"properties": {
				"f_str": {
					"type": "string"
				},
				"f_64": {
					"type": "integer"
				}
			}
		}
	},
	"primary_key": ["id"]
}`)

	schFactory, err := schema.NewFactoryBuilder(true).Build("test_update", reqSchema)
	require.NoError(t, err)

	c, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(t, err)

	return c
}

func testCollection2(t *testing.T) *schema.DefaultCollection {
	reqSchema := []byte(`{
	"title": "test_update",
	"properties": {
		"id": {
			"type": "integer"
		},
		"a": {
			"type": "integer",
			"format": "int32"
		},
		"b": {
			"type": "integer"
		},
		"c": {
			"type": "string",
			"maxLength": 128
		},
		"d": {
			"type": "number"
		},
		"e": {
			"type": "array",
			"items": {
				"type": "integer"
			}
		},
		"f": {
			"type": "object",
			"properties": {
				"f_str": {
					"type": "string"
				},
				"f_64": {
					"type": "integer"
				}
			}
		},
		"g": {
			"type": "string",
			"maxLength": 128
		}
	},
	"primary_key": ["id", "a", "c"]
}`)

	schFactory, err := schema.NewFactoryBuilder(true).Build("test_update", reqSchema)
	require.NoError(t, err)

	c, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(t, err)

	return c
}

func testCollection3(t *testing.T) *schema.DefaultCollection {
	reqSchema := []byte(`{
	"title": "test_array_update",
	"properties": {
		"id": {
			"type": "integer"
		},
		"f_int": {
			"type": "array",
			"items": {
				"type": "integer"
			}
		},
		"f_str": {
			"type": "array",
			"items": {
				"type": "string",
				"maxLength": 128
			}
		},
		"f_obj": {
			"type": "object",
			"properties": {
				"a": {
					"type": "array",
					"items": {
						"type": "integer"
					}
				},
				"b": {
					"type": "array",
					"items": {
						"type": "string"
					}
				}
			}
		},
		"f_obj_arr": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"c": {
						"type": "integer"
					},
					"d": {
						"type": "string",
						"maxLength": 128
					}
				}
			}
		},
		"g": {
			"type": "string",
			"maxLength": 128
		}
	},
	"primary_key": ["id"]
}`)
	schFactory, err := schema.NewFactoryBuilder(true).Build("test_array_update", reqSchema)
	require.NoError(t, err)

	c, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	require.NoError(t, err)

	return c
}
