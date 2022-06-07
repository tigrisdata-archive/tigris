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
	"github.com/tigrisdata/tigris/schema"
)

func TestFilterUsingJSON(t *testing.T) {
	t.Run("basic_filter", func(t *testing.T) {
		js := []byte(`{"f1": 10, "f2": 10}`)
		var factory = Factory{
			fields: []*schema.Field{
				{FieldName: "f1", DataType: schema.Int64Type},
				{FieldName: "f2", DataType: schema.Int64Type},
			},
		}
		filters, err := factory.Factorize(js)
		require.NoError(t, err)
		require.Len(t, filters, 2)
		for _, f := range filters {
			require.True(t, f.(*Selector).Field == "f1" || f.(*Selector).Field == "f2")
		}
	})
	t.Run("filter_or_nested_and", func(t *testing.T) {
		js := []byte(`{"$or": [{"f1": 20}, {"$and": [{"f2":5}, {"f3": 6}]}]}`)
		var factory = Factory{
			fields: []*schema.Field{
				{FieldName: "f1", DataType: schema.Int64Type},
				{FieldName: "f2", DataType: schema.Int64Type},
				{FieldName: "f3", DataType: schema.Int64Type},
			},
		}
		filters, err := factory.Factorize(js)
		require.NoError(t, err)
		require.Len(t, filters, 1)
		require.Len(t, filters[0].(*OrFilter).filter, 2)
		require.Equal(t, "f1", filters[0].(*OrFilter).filter[0].(*Selector).Field)
		require.Len(t, filters[0].(*OrFilter).filter[1].(*AndFilter).filter, 2)
		require.Equal(t, "f2", filters[0].(*OrFilter).filter[1].(*AndFilter).filter[0].(*Selector).Field)
		require.Equal(t, "f3", filters[0].(*OrFilter).filter[1].(*AndFilter).filter[1].(*Selector).Field)
	})
	t.Run("filter_and_or_nested", func(t *testing.T) {
		js := []byte(`{"$and": [{"a": 20}, {"$or": [{"b":5}, {"c": 6}]}, {"$and": [{"e":5}, {"f": 6}]}]}`)
		var factory = Factory{
			fields: []*schema.Field{
				{FieldName: "a", DataType: schema.Int64Type},
				{FieldName: "b", DataType: schema.Int64Type},
				{FieldName: "c", DataType: schema.Int64Type},
				{FieldName: "e", DataType: schema.Int64Type},
				{FieldName: "f", DataType: schema.Int64Type},
			},
		}
		filters, err := factory.Factorize(js)
		require.NoError(t, err)
		require.Len(t, filters, 1)
		require.Len(t, filters[0].(*AndFilter).filter, 3)
		require.Equal(t, "a", filters[0].(*AndFilter).filter[0].(*Selector).Field)
		require.Len(t, filters[0].(*AndFilter).filter[1].(*OrFilter).filter, 2)
		require.Len(t, filters[0].(*AndFilter).filter[2].(*AndFilter).filter, 2)
		require.Equal(t, "b", filters[0].(*AndFilter).filter[1].(*OrFilter).filter[0].(*Selector).Field)
		require.Equal(t, "e", filters[0].(*AndFilter).filter[2].(*AndFilter).filter[0].(*Selector).Field)
	})
	t.Run("filter_mix", func(t *testing.T) {
		js := []byte(`{"f1": 10, "f2": 10, "$or": [{"f3": 20}, {"$and": [{"f4":5}, {"f5": 6}]}], "$and": [{"a": 20}, {"$or": [{"b":5}, {"c": 6}]}, {"$and": [{"e":5}, {"f": 6}]}]}`)
		var factory = Factory{
			fields: []*schema.Field{
				{FieldName: "f1", DataType: schema.Int64Type},
				{FieldName: "f2", DataType: schema.Int64Type},
				{FieldName: "f3", DataType: schema.Int64Type},
				{FieldName: "f4", DataType: schema.Int64Type},
				{FieldName: "f5", DataType: schema.Int64Type},
				{FieldName: "a", DataType: schema.Int64Type},
				{FieldName: "b", DataType: schema.Int64Type},
				{FieldName: "c", DataType: schema.Int64Type},
				{FieldName: "e", DataType: schema.Int64Type},
				{FieldName: "f", DataType: schema.Int64Type},
			},
		}
		filters, err := factory.Factorize(js)
		require.NoError(t, err)
		require.Len(t, filters, 4)

		countSelectors, countAnd, countOr := 0, 0, 0
		for _, f := range filters {
			if _, ok := f.(*Selector); ok {
				countSelectors++
				require.True(t, f.(*Selector).Field == "f1" || f.(*Selector).Field == "f2")
			}
			if _, ok := f.(*OrFilter); ok {
				require.Len(t, f.(*OrFilter).filter, 2)
				countOr++
			}
			if _, ok := f.(*AndFilter); ok {
				require.Len(t, f.(*AndFilter).filter, 3)
				countAnd++
			}
		}
		require.Equal(t, 2, countSelectors)
		require.Equal(t, 1, countOr)
		require.Equal(t, 1, countAnd)
	})
}

func TestFilterDuplicateKey(t *testing.T) {
	var factory = Factory{
		fields: []*schema.Field{
			{FieldName: "a", DataType: schema.Int64Type},
			{FieldName: "b", DataType: schema.Int64Type},
		},
	}
	filters, err := factory.Factorize([]byte(`{"a": 10, "b": {"$eq": 10}, "b": 15}`))
	require.Nil(t, filters)
	require.Contains(t, err.Error(), "duplicate filter 'b'")
}
