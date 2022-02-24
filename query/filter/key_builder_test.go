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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisdb/keys"
	"github.com/tigrisdata/tigrisdb/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testFilters(t testing.TB, input []byte) []Filter {
	filters, err := Build(input)
	require.NoError(t, err)
	require.NotNil(t, filters)

	return filters
}

func TestKeyBuilder(t *testing.T) {
	cases := []struct {
		userKeys  []*schema.Field
		userInput []byte
		expError  error
		expKeys   []keys.Key
	}{
		{
			// fewer fields in user input
			[]*schema.Field{{FieldName: "a", DataType: schema.IntType}, {FieldName: "c", DataType: schema.IntType}, {FieldName: "b", DataType: schema.IntType}},
			[]byte(`{"a": 10, "c": 10}`),
			status.Errorf(codes.InvalidArgument, "filters doesn't contains primary key fields"),
			nil,
		},
		{
			// some fields are not with eq
			[]*schema.Field{{FieldName: "a", DataType: schema.IntType}, {FieldName: "c", DataType: schema.IntType}, {FieldName: "b", DataType: schema.IntType}},
			[]byte(`{"a": 10, "b": {"$eq": 10}, "c": {"$gt": 15}}`),
			status.Errorf(codes.InvalidArgument, "filters only supporting $eq comparison, found '$gt'"),
			nil,
		},
		{
			// some fields are repeated
			[]*schema.Field{{FieldName: "a", DataType: schema.IntType}, {FieldName: "b", DataType: schema.IntType}},
			[]byte(`{"$and": [{"a": 10}, {"b": {"$eq": 10}}, {"b": 15}]}`),
			status.Errorf(codes.InvalidArgument, "reusing same fields for conditions on equality"),
			nil,
		},
		{
			// single user defined key
			[]*schema.Field{{FieldName: "a", DataType: schema.IntType}},
			[]byte(`{"b": 10, "a": {"$eq": 1}}`),
			nil,
			[]keys.Key{keys.NewKey("", int64(1))},
		},
		{
			// composite user defined key
			[]*schema.Field{{FieldName: "a", DataType: schema.BoolType}, {FieldName: "b", DataType: schema.IntType}, {FieldName: "c", DataType: schema.StringType}},
			[]byte(`{"b": 10, "a": {"$eq": true}, "c": "foo"}`),
			nil,
			[]keys.Key{keys.NewKey("", true, int64(10), "foo")},
		},
		{
			// single with AND/OR filter
			[]*schema.Field{{FieldName: "a", DataType: schema.IntType}},
			[]byte(`{"$or": [{"a": 1}, {"$and": [{"a":2}, {"f1": 3}]}], "$and": [{"a": 4}, {"$or": [{"a":5}, {"f2": 6}]}, {"$or": [{"a":5}, {"a": 6}]}]}`),
			nil,
			[]keys.Key{keys.NewKey("", int64(1)), keys.NewKey("", int64(4)), keys.NewKey("", int64(2)), keys.NewKey("", int64(5)), keys.NewKey("", int64(5)), keys.NewKey("", int64(6))},
		},
		{
			// composite with AND filter
			[]*schema.Field{{FieldName: "a", DataType: schema.IntType}, {FieldName: "b", DataType: schema.StringType}},
			[]byte(`{"$and":[{"a":1},{"b":"aaa"},{"$and":[{"a":2},{"c":5},{"b":"bbb"}]}]}`),
			nil,
			[]keys.Key{keys.NewKey("", int64(1), "aaa"), keys.NewKey("", int64(2), "bbb")},
		}, {
			[]*schema.Field{{FieldName: "a", DataType: schema.IntType}},
			[]byte(`{"b":10,"a":1,"c":"ccc","$or":[{"f1":10},{"a":2}]}`),
			nil,
			[]keys.Key{keys.NewKey("", int64(1)), keys.NewKey("", int64(2))},
		},
	}
	for _, c := range cases {
		b := NewKeyBuilder(NewStrictEqKeyComposer(""))
		filters := testFilters(t, c.userInput)
		buildKeys, err := b.Build(filters, c.userKeys)
		require.Equal(t, c.expError, err)
		require.Equal(t, c.expKeys, buildKeys)
	}
}

func BenchmarkStrictEqKeyComposer_Compose(b *testing.B) {
	for i := 0; i < b.N; i++ {
		kb := NewKeyBuilder(NewStrictEqKeyComposer(""))
		filters := testFilters(b, []byte(`{"b": 10, "a": {"$eq": 10}, "c": "foo"}}`))
		_, err := kb.Build(filters, []*schema.Field{{FieldName: "a", DataType: schema.IntType}, {FieldName: "b", DataType: schema.IntType}, {FieldName: "c", DataType: schema.IntType}})
		require.NoError(b, err)
	}
	b.ReportAllocs()
}
