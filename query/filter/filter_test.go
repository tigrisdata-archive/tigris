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
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestFilterUsingJSON(t *testing.T) {
	t.Run("basic_filter", func(t *testing.T) {
		var r = &api.ReadRequest{}
		js := []byte(`{"filter": [{"f1": 10}, {"f2": 10}]}`)
		require.NoError(t, protojson.Unmarshal(js, r))

		filters, err := Build(r.Filter)
		require.NoError(t, err)
		require.Len(t, filters, 2)
		require.Equal(t, "f1", filters[0].(*Selector).Field)
		require.Equal(t, "f2", filters[1].(*Selector).Field)
	})
	t.Run("filter_or_nested_and", func(t *testing.T) {
		var r = &api.ReadRequest{}
		js := []byte(`{"filter": [{"$or": [{"f1": 20}, {"$and": [{"f2":5}, {"f3": 6}]}]}]}`)
		require.NoError(t, protojson.Unmarshal(js, r))

		filters, err := Build(r.Filter)
		require.NoError(t, err)
		require.Len(t, filters, 1)
		require.Len(t, filters[0].(*OrFilter).filter, 2)
		require.Equal(t, "f1", filters[0].(*OrFilter).filter[0].(*Selector).Field)
		require.Len(t, filters[0].(*OrFilter).filter[1].(*AndFilter).filter, 2)
		require.Equal(t, "f2", filters[0].(*OrFilter).filter[1].(*AndFilter).filter[0].(*Selector).Field)
		require.Equal(t, "f3", filters[0].(*OrFilter).filter[1].(*AndFilter).filter[1].(*Selector).Field)
	})
	t.Run("filter_and_or_nested", func(t *testing.T) {
		var r = &api.ReadRequest{}
		js := []byte(`{"filter": [{"$and": [{"a": 20}, {"$or": [{"b":5}, {"c": 6}]}, {"$and": [{"e":5}, {"f": 6}]}]}]}`)
		require.NoError(t, protojson.Unmarshal(js, r))

		filters, err := Build(r.Filter)
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
		var r = &api.ReadRequest{}
		js := []byte(`{"filter": [{"f1": 10}, {"f2": 10}, {"$or": [{"f3": 20}, {"$and": [{"f4":5}, {"f5": 6}]}]}, {"$and": [{"a": 20}, {"$or": [{"b":5}, {"c": 6}]}, {"$and": [{"e":5}, {"f": 6}]}]}]}`)
		require.NoError(t, protojson.Unmarshal(js, r))

		filters, err := Build(r.Filter)
		require.NoError(t, err)
		require.Len(t, filters, 4)
		require.Equal(t, "f1", filters[0].(*Selector).Field)
		require.Equal(t, "f2", filters[1].(*Selector).Field)
		require.Len(t, filters[2].(*OrFilter).filter, 2)
		require.Len(t, filters[3].(*AndFilter).filter, 3)
	})
}

func TestFilterProto(t *testing.T) {
	var orClause []interface{}
	orClause = append(orClause, map[string]interface{}{
		"f3": 30,
	})
	orClause = append(orClause, map[string]interface{}{
		"f4": 40,
	})
	l, err := structpb.NewList(orClause)
	require.NoError(t, err)

	gtStruct, err := structpb.NewStruct(map[string]interface{}{
		"$gt": 20,
	})
	require.NoError(t, err)

	var f = []*structpb.Struct{
		{
			Fields: map[string]*structpb.Value{
				"f1": structpb.NewNumberValue(10),
			},
		},
		{
			Fields: map[string]*structpb.Value{
				"f2": structpb.NewStructValue(gtStruct),
			},
		},
		{
			Fields: map[string]*structpb.Value{
				"$or": structpb.NewListValue(l),
			},
		},
	}

	filters, err := Build(f)
	require.NoError(t, err)
	require.Len(t, filters, 3)
}
