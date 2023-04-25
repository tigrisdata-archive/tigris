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

package database

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/schema"
)

func TestPrimaryIndexPlanner_IsPrefixQueryWithSuffixSort(t *testing.T) {
	factory := filter.NewFactory(
		[]*schema.QueryableField{
			{FieldName: "tenant", DataType: schema.StringType},
			{FieldName: "createdAt", DataType: schema.StringType},
			{FieldName: "f3", DataType: schema.Int64Type},
			{FieldName: "f4", DataType: schema.Int64Type},
		}, nil)

	cases := []struct {
		idxFields    []*schema.QueryableField
		reqFilter    []byte
		sorting      *sort.Ordering
		expPrefix    bool
		isPrefixSort bool
	}{
		{
			[]*schema.QueryableField{{FieldName: "tenant", DataType: schema.StringType}},
			[]byte(`{"tenant": "a"}`),
			&sort.Ordering{{Name: "tenant", Ascending: false}},
			true,
			true,
		},
		{
			[]*schema.QueryableField{{FieldName: "tenant", DataType: schema.StringType}},
			[]byte(`{"f3": 10}`),
			&sort.Ordering{{Name: "tenant", Ascending: false}},
			true,
			true,
		},

		{
			[]*schema.QueryableField{{FieldName: "tenant", DataType: schema.StringType}, {FieldName: "createdAt", DataType: schema.StringType}},
			[]byte(`{"tenant": "a"}`),
			&sort.Ordering{{Name: "createdAt", Ascending: false}},
			true,
			false,
		},
		{
			[]*schema.QueryableField{{FieldName: "tenant", DataType: schema.StringType}, {FieldName: "createdAt", DataType: schema.StringType}},
			[]byte(`{"f3": 10}`),
			&sort.Ordering{{Name: "createdAt", Ascending: false}},
			false,
			false,
		},
		{
			[]*schema.QueryableField{{FieldName: "tenant", DataType: schema.StringType}, {FieldName: "createdAt", DataType: schema.StringType}},
			[]byte(`{"$or": [{"tenant": "a"}, {"tenant": "b"}, {"tenant": "c"}]}`),
			&sort.Ordering{{Name: "createdAt", Ascending: false}},
			true,
			false,
		},
		{
			[]*schema.QueryableField{{FieldName: "tenant", DataType: schema.StringType}, {FieldName: "createdAt", DataType: schema.StringType}},
			[]byte(`{"$or": [{"tenant": "a"}, {"tenant": "b"}, {"f3": 10}]}`),
			&sort.Ordering{{Name: "createdAt", Ascending: false}},
			false,
			false,
		},
		{
			[]*schema.QueryableField{{FieldName: "tenant", DataType: schema.StringType}, {FieldName: "createdAt", DataType: schema.StringType}},
			[]byte(`{"$and": [{"tenant": "a"}, {"f3": 4}, {"f4": 7}]}`),
			&sort.Ordering{{Name: "createdAt", Ascending: false}},
			true,
			false,
		},
		{
			[]*schema.QueryableField{{FieldName: "tenant", DataType: schema.StringType}, {FieldName: "createdAt", DataType: schema.StringType}},
			[]byte(`{"$or": [{"f3": 20}, {"$and": [{"f3":5}, {"f4": 10}]}]}`),
			&sort.Ordering{{Name: "tenant", Ascending: false}},
			false,
			true,
		},
	}
	for _, c := range cases {
		filters, err := factory.Factorize(c.reqFilter)
		require.NoError(t, err)

		p := &PrimaryIndexPlanner{
			idxFields: c.idxFields,
			filter:    filters,
			encFunc:   noopEncFunc,
		}

		sp, err := p.SortPlan(c.sorting)
		require.NoError(t, err)
		require.Equal(t, c.expPrefix, p.IsPrefixQueryWithSuffixSort(sp))
		require.Equal(t, c.isPrefixSort, p.isPrefixSort(sp))
	}
}

var noopEncFunc = func(indexParts ...interface{}) (keys.Key, error) {
	return nil, nil
}