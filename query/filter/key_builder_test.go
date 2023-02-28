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

package filter

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/value"
)

func testFilters(t testing.TB, fields []*schema.QueryableField, input []byte, stringEncoding bool) []Filter {
	collation := value.NewCollation()
	buildForSecondaryIndex := false

	if stringEncoding {
		collation = value.NewSortKeyCollation()
		buildForSecondaryIndex = true
	}

	factory := Factory{
		fields,
		collation,
		buildForSecondaryIndex,
	}
	filters, err := factory.Factorize(input)
	require.NoError(t, err)
	require.NotNil(t, filters)

	return filters
}

func TestKeyBuilderStrictEq(t *testing.T) {
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
			errors.InvalidArgument("filters doesn't contains primary key fields"),
			nil,
		},
		{
			// some fields are not with eq
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]byte(`{"a": 10, "b": {"$eq": 10}, "c": {"$gt": 15}}`),
			errors.InvalidArgument("filters only supporting $eq comparison, found '$gt'"),
			nil,
		},
		{
			// some fields are repeated
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]byte(`{"$and": [{"a": 10}, {"b": {"$eq": 10}}, {"b": 15}]}`),
			errors.InvalidArgument("reusing same fields for conditions on equality"),
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
		},
		{
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.StringType}, {FieldName: "f1", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"b":10,"a":1,"c":"ccc","$or":[{"f1":10},{"a":2}]}`),
			nil,
			[]keys.Key{keys.NewKey(nil, int64(1)), keys.NewKey(nil, int64(2))},
		},
		{
			// composite with OR parent filter
			[]*schema.QueryableField{{FieldName: "K1", DataType: schema.StringType}, {FieldName: "K2", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "K1", DataType: schema.StringType}, {FieldName: "K2", DataType: schema.Int64Type}},
			[]byte(`{"$or":[{"$and":[{"K1":"bar"},{"K2":3}]},{"$and":[{"K1":"foo"},{"K2":2}]}]}`),
			nil,
			[]keys.Key{keys.NewKey(nil, "bar", int64(3)), keys.NewKey(nil, "foo", int64(2))},
		},
	}
	for _, c := range cases {
		b := NewKeyBuilder[*schema.Field](NewStrictEqKeyComposer[*schema.Field](dummyEncodeFunc, PKBuildIndexPartsFunc, true), true)
		filters := testFilters(t, c.userFields, c.userInput, false)
		buildKeys, err := b.Build(filters, c.userKeys)
		require.Equal(t, c.expError, err)
		if c.expError != nil {
			continue
		}
		require.Equal(t, len(c.expKeys), len(buildKeys[0].Keys), string(c.userInput))
		for _, k := range c.expKeys {
			found := false
			for _, build := range buildKeys[0].Keys {
				if reflect.DeepEqual(k, build) {
					found = true
					break
				}
			}
			require.True(t, found)
		}
	}
}

func TestKeyBuilderSecondaryEq(t *testing.T) {
	cases := []struct {
		userFields []*schema.QueryableField
		userKeys   []*schema.Field
		userInput  []byte
		expError   error
		queryPlans []QueryPlan
	}{
		{
			// single user defined key
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"b": 10, "a": {"$eq": 1}}`),
			nil,
			[]QueryPlan{newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(1))})},
		},
		{
			// single user defined key
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}},
			[]byte(`{"b": 10}`),
			nil,
			[]QueryPlan{newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(10))})},
		},
		{
			// multiple defined query keys
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]byte(`{"b": 10, "a": {"$eq": 1}}`),
			nil,
			[]QueryPlan{
				newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(1))}),
				newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(10))}),
			},
		},
		{
			// composite user defined key
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.BoolType}, {FieldName: "c", DataType: schema.StringType}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.BoolType}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.StringType}},
			[]byte(`{"b": 10, "a": {"$eq": true}, "c": "foo"}`),
			nil,
			[]QueryPlan{
				newQueryPlan(EQUAL, schema.BoolType, []keys.Key{keys.NewKey(nil, true)}),
				newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(10))}),
				newQueryPlan(EQUAL, schema.StringType, []keys.Key{keys.NewKey(nil, encodeString("foo"))}),
			},
		},
		{
			// composite with AND filter
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.StringType}, {FieldName: "c", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.StringType}},
			[]byte(`{"$and":[{"a":1},{"b":"aaa"},{"$and":[{"a":2},{"c":5},{"b":"bbb"}]}]}`),
			nil,
			[]QueryPlan{
				newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(1))}),
				newQueryPlan(EQUAL, schema.StringType, []keys.Key{keys.NewKey(nil, encodeString("aaa"))}),
				newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(2))}),
				newQueryPlan(EQUAL, schema.StringType, []keys.Key{keys.NewKey(nil, encodeString("bbb"))}),
			},
		},
		{
			// composite with OR filter
			[]*schema.QueryableField{{FieldName: "K1", DataType: schema.StringType}, {FieldName: "K2", DataType: schema.StringType}},
			[]*schema.Field{{FieldName: "K1", DataType: schema.StringType}, {FieldName: "K1", DataType: schema.StringType}},
			[]byte(`{"$or" :[{"$and":[{"K1":"vK1"},{"K2":1}]},{"$and":[{"K1": "vK1"}, {"K2":3}]}]}`),
			errors.InvalidArgument("$or filter is not yet supported for secondary index"),
			nil,
		},
		// NOT SUPPORTED YET
		// {
		// 	// simple OR filter
		// 	[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "f1", DataType: schema.Int64Type}, {FieldName: "f2", DataType: schema.Int64Type}},
		// 	[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
		// 	[]byte(`{"$or": [{"a": 1}, {"a": 30}]}`),
		// 	nil,
		// 	[]QueryPlan{
		// 		newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(1)), keys.NewKey(nil, int64(30))}),
		// 	},
		// },
		// {
		// 	// single with AND/OR filter
		// 	[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "f1", DataType: schema.Int64Type}, {FieldName: "f2", DataType: schema.Int64Type}},
		// 	[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
		// 	[]byte(`{"$or": [{"a": 1}, {"$and": [{"a":2}, {"f1": 3}]}], "$and": [{"a": 4}, {"$or": [{"a":5}, {"f2": 6}]}, {"$or": [{"a":5}, {"a": 6}]}]}`),
		// 	nil,
		// 	[]QueryPlan{
		// 		newQueryPlan(EQUAL, schema.Int64Type, []keys.Key{keys.NewKey(nil, int64(1)), keys.NewKey(nil, int64(2))}), //keys.NewKey(nil, int64(2)), keys.NewKey(nil, int64(5)), keys.NewKey(nil, int64(5)), keys.NewKey(nil, int64(6))}),
		// 	},
		// },

		// {
		// 	[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.StringType}, {FieldName: "f1", DataType: schema.Int64Type}},
		// 	[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
		// 	[]byte(`{"b":10,"a":1,"c":"ccc","$or":[{"f1":10},{"a":2}]}`),
		// 	nil,
		// 	[]keys.Key{keys.NewKey(nil, int64(1)), keys.NewKey(nil, int64(2))},
		// },
		// {
		// 	// composite with OR parent filter
		// 	[]*schema.QueryableField{{FieldName: "K1", DataType: schema.StringType}, {FieldName: "K2", DataType: schema.Int64Type}},
		// 	[]*schema.Field{{FieldName: "K1", DataType: schema.StringType}, {FieldName: "K2", DataType: schema.Int64Type}},
		// 	[]byte(`{"$or":[{"$and":[{"K1":"bar"},{"K2":3}]},{"$and":[{"K1":"foo"},{"K2":2}]}]}`),
		// 	nil,
		// 	[]keys.Key{keys.NewKey(nil, "bar", int64(3)), keys.NewKey(nil, "foo", int64(2))},
		// },
	}
	for _, c := range cases {
		b := NewKeyBuilder[*schema.Field](NewStrictEqKeyComposer[*schema.Field](dummyEncodeFunc, PKBuildIndexPartsFunc, false), false)
		filters := testFilters(t, c.userFields, c.userInput, true)
		queryPlans, err := b.Build(filters, c.userKeys)
		require.Equal(t, c.expError, err)
		require.Equal(t, len(c.queryPlans), len(queryPlans))
		for i, plan := range c.queryPlans {
			for j, key := range plan.Keys {
				assert.Equal(t, key, queryPlans[i].Keys[j])
			}
		}
	}
}

func TestKeyBuilderRangeKey(t *testing.T) {
	cases := []struct {
		userFields []*schema.QueryableField
		userKeys   []*schema.Field
		userInput  []byte
		queryType  QueryPlanType
		expKeys    []keys.Key
	}{
		{
			// single gt
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"a": {"$gt": 1}}`),
			FULLRANGE,
			[]keys.Key{keys.NewKey(nil, "a", int64(1), 0xFF), keys.NewKey(nil, "a", math.MaxInt64, 0xFF)},
		},
		{
			// single gte
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"a": {"$gte": 1}}`),
			FULLRANGE,
			[]keys.Key{keys.NewKey(nil, "a", int64(1)), keys.NewKey(nil, "a", math.MaxInt64, 0xFF)},
		},
		{
			// single lt
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"a": {"$lt": 30}}`),
			FULLRANGE,
			[]keys.Key{keys.NewKey(nil, "a", nil), keys.NewKey(nil, "a", int64(30))},
		},
		{
			// single lte
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"a": {"$lte": 30}}`),
			FULLRANGE,
			[]keys.Key{keys.NewKey(nil, "a", nil), keys.NewKey(nil, "a", int64(30), 0xFF)},
		},
		{
			// single range user defined key
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}},
			[]*schema.Field{{FieldName: "a", DataType: schema.Int64Type}},
			[]byte(`{"$and": [{"a": {"$gte": 1}}, {"a": {"$lt": 10}}]}`),
			RANGE,
			[]keys.Key{keys.NewKey(nil, "a", int64(1)), keys.NewKey(nil, "a", int64(10))},
		},
		{
			// single range user defined string key
			[]*schema.QueryableField{{FieldName: "a", DataType: schema.StringType}},
			[]*schema.Field{{FieldName: "a", DataType: schema.StringType}},
			[]byte(`{"$and": [{"a": {"$gte": "f"}}, {"a": {"$lt": "m"}}]}`),
			RANGE,
			[]keys.Key{keys.NewKey(nil, "a", encodeString("f")), keys.NewKey(nil, "a", encodeString("m"))},
		},
		// NOT SUPPORTED YET
		// {
		// 	// single range user defined key
		// 	[]*schema.QueryableField{{FieldName: "a.b.c", DataType: schema.Int64Type}},
		// 	[]*schema.Field{{FieldName: "z", DataType: schema.Int64Type}},
		// 	[]byte(`{"a": {"b": {"c": {"$gt": 1}}}}`),
		// 	nil,
		// 	[]keys.Key{keys.NewKey(nil, int64(1)), nil},
		// },
	}

	for _, c := range cases {
		b := NewKeyBuilder[*schema.Field](NewRangeKeyComposer[*schema.Field](dummyEncodeFunc, dummyBuildIndexParts), false)
		filters := testFilters(t, c.userFields, c.userInput, true)
		queryPlans, err := b.Build(filters, c.userKeys)
		assert.NoError(t, err)
		assert.Len(t, queryPlans, 1)
		queryPlan := queryPlans[0]
		assert.Equal(t, len(c.expKeys), len(queryPlan.Keys))
		assert.Equal(t, c.queryType, queryPlan.QueryType)
		for i, k := range c.expKeys {
			assert.Equal(t, k, queryPlan.Keys[i], string(c.userInput))
		}
	}
}

func TestKeyBuilderMultipleRangeKey(t *testing.T) {
	userFields := []*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}}
	userKeys := []*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}}
	filter := []byte(`{"$and": [{"a": {"$gte": 1}}, {"a": {"$lt": 10}}, {"b": {"$gt": 3}}, {"b": {"$lte": 30}}]}`)

	b := NewKeyBuilder[*schema.Field](NewRangeKeyComposer[*schema.Field](dummyEncodeFunc, dummyBuildIndexParts), false)
	filters := testFilters(t, userFields, filter, true)
	keyReads, err := b.Build(filters, userKeys)

	assert.NoError(t, err)
	assert.Len(t, keyReads, 2)
	assert.Equal(t, []keys.Key{keys.NewKey(nil, "a", int64(1)), keys.NewKey(nil, "a", int64(10))}, keyReads[0].Keys)
	assert.Equal(t, []keys.Key{keys.NewKey(nil, "b", int64(3), 0xFF), keys.NewKey(nil, "b", int64(30), 0xFF)}, keyReads[1].Keys)
}

func BenchmarkStrictEqKeyComposer_Compose(b *testing.B) {
	for i := 0; i < b.N; i++ {
		kb := NewKeyBuilder[*schema.Field](NewStrictEqKeyComposer[*schema.Field](dummyEncodeFunc, PKBuildIndexPartsFunc, true), true)
		filters := testFilters(b, []*schema.QueryableField{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}}, []byte(`{"b": 10, "a": {"$eq": 10}, "c": "foo"}}`), false)
		_, err := kb.Build(filters, []*schema.Field{{FieldName: "a", DataType: schema.Int64Type}, {FieldName: "b", DataType: schema.Int64Type}, {FieldName: "c", DataType: schema.Int64Type}})
		require.NoError(b, err)
	}
	b.ReportAllocs()
}

func dummyEncodeFunc(indexParts ...interface{}) (keys.Key, error) {
	return keys.NewKey(nil, indexParts...), nil
}

func dummyBuildIndexParts(fieldName string, datatype schema.FieldType, value interface{}) []interface{} {
	return []interface{}{fieldName, value}
}

func encodeString(val string) interface{} {
	return value.NewStringValue(val, value.NewSortKeyCollation()).AsInterface()
}
