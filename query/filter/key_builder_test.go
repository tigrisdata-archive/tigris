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

package filter

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
)

func testFilters(t testing.TB, fields []*schema.QueryableField, input []byte) []Filter {
	var factory = Factory{
		fields: fields,
	}
	filters, err := factory.Factorize(input)
	require.NoError(t, err)
	require.NotNil(t, filters)

	return filters
}

func TestKeyBuilder(t *testing.T) {
	cases := []struct {
		userFields []*schema.QueryableField
		userKeys   []*schema.Field
		userInput  []byte
		expError   error
		expKeys    []keys.Key
	}{
		{
			// fewer fields in user input
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]byte(`{"a": 10, "c": 10}`),
			api.Errorf(api.Code_INVALID_ARGUMENT, "filters doesn't contains primary key fields"),
			nil,
		},
		{
			// some fields are not with eq
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]byte(`{"a": 10, "b": {"$eq": 10}, "c": {"$gt": 15}}`),
			api.Errorf(api.Code_INVALID_ARGUMENT, "filters only supporting $eq comparison, found '$gt'"),
			nil,
		},
		{
			// some fields are repeated
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]byte(`{"$and": [{"a": 10}, {"b": {"$eq": 10}}, {"b": 15}]}`),
			api.Errorf(api.Code_INVALID_ARGUMENT, "reusing same fields for conditions on equality"),
			nil,
		},
		{
			// single user defined key
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"b": 10, "a": {"$eq": 1}}`),
			nil,
			[]keys.Key{keys.NewKey(nil, int64(1))},
		},
		{
			// composite user defined key
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.BoolType}, {FieldName: "c", DataType: schema.StringType}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.BoolType}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.StringType}},
			[]byte(`{"b": 10, "a": {"$eq": true}, "c": "foo"}`),
			nil,
			[]keys.Key{keys.NewKey(nil, true, int64(10), "foo")},
		},
		{
			// single with AND/OR filter
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "f1", DataType: schema.Int64Type}, {FieldName: "f2", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"$or": [{"a": 1}, {"$and": [{"a":2}, {"f1": 3}]}], "$and": [{"a": 4}, {"$or": [{"a":5}, {"f2": 6}]}, {"$or": [{"a":5}, {"a": 6}]}]}`),
			nil,
			[]keys.Key{keys.NewKey(nil, int64(1)), keys.NewKey(nil, int64(4)), keys.NewKey(nil, int64(2)), keys.NewKey(nil, int64(5)), keys.NewKey(nil, int64(5)), keys.NewKey(nil, int64(6))},
		},
		{
			// composite with AND filter
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.StringType}, {FieldName: "c", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.StringType}},
			[]byte(`{"$and":[{"a":1},{"b":"aaa"},{"$and":[{"a":2},{"c":5},{"b":"bbb"}]}]}`),
			nil,
			[]keys.Key{keys.NewKey(nil, int64(1), "aaa"), keys.NewKey(nil, int64(2), "bbb")},
		}, {
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.StringType}, {FieldName: "f1", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"b":10,"a":1,"c":"ccc","$or":[{"f1":10},{"a":2}]}`),
			nil,
			[]keys.Key{keys.NewKey(nil, int64(1)), keys.NewKey(nil, int64(2))},
		}, {
			// composite with OR parent filter
			[]*schema.QueryableField{{FieldName: "K1", DataType: schema.StringType}, {FieldName: "K2", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "K1", DataType: schema.StringType}, {FieldName: "K2", DataType: schema.Int64Type}},
			[]byte(`{"$or":[{"$and":[{"K1":"bar"},{"K2":3}]},{"$and":[{"K1":"foo"},{"K2":2}]}]}`),
			nil,
			[]keys.Key{keys.NewKey(nil, "bar", int64(3)), keys.NewKey(nil, "foo", int64(2))},
		},
	}
	for _, c := range cases {
		b := NewKeyBuilder(NewStrictEqKeyComposer(dummyEncodeFunc))
		filters := testFilters(t, c.userFields, c.userInput)
		buildKeys, err := b.Build(filters, c.userKeys)
		require.Equal(t, c.expError, err)
		require.Equal(t, len(c.expKeys), len(buildKeys))
		for _, k := range c.expKeys {
			found := false
			for _, build := range buildKeys {
				if reflect.DeepEqual(k, build) {
					found = true
					break
				}
			}
			require.True(t, found)
		}
	}
}

func BenchmarkStrictEqKeyComposer_Compose(b *testing.B) {
	for i := 0; i < b.N; i++ {
		kb := NewKeyBuilder(NewStrictEqKeyComposer(dummyEncodeFunc))
		filters := testFilters(b, []*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}}, []byte(`{"b": 10, "a": {"$eq": 10}, "c": "foo"}}`))
		_, err := kb.Build(filters, []*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}})
		require.NoError(b, err)
	}
	b.ReportAllocs()
}

func dummyEncodeFunc(indexParts ...interface{}) (keys.Key, error) {
	return keys.NewKey(nil, indexParts...), nil
}
