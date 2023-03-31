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

package search

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/query/search"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

func TestNewFacetResponse(t *testing.T) {
	t.Run("query with empty facets", func(t *testing.T) {
		query := search.Facets{}
		fr := NewFacetResponse(query)
		require.NotNil(t, fr)
		require.Empty(t, fr.facetSizes)
	})

	t.Run("query with some facets", func(t *testing.T) {
		query := search.Facets{
			Fields: []search.FacetField{
				{
					Name: "a",
					Size: 10,
				},
			},
		}
		fr := NewFacetResponse(query)
		require.NotNil(t, fr)
		require.Len(t, fr.facetSizes, 1)
		require.Equal(t, fr.facetSizes["a"], 10)
	})
}

func TestFacetResponse_Build(t *testing.T) {
	inputData := [][]byte{
		[]byte(`{"field_name":"a",
				"counts":[
      				{"count":20,"value":"value_1"},
      				{"count":30,"value":"value_2"},
      				{"count":10,"value":null}
				]}`),
		[]byte(`{"field_name":"b","counts":[
					{"count":14,"value":"value_1"},
					{"count":12,"value":"value_2"}
				]}`),
		[]byte(`{"field_name":null,
				"counts":[
					{"count":20,"value":"value_1"},
					{"count":30,"value":"value_2"},
					{"count":10,"value":null}
				]}`),
		[]byte(`{"field_name":"c",
				"counts":[
					{"count":20,"value":"value_1"},
					{"count":30,"value":"value_2"},
					{"count":10,"value":null},
					{"count":10,"value":"value_3"}
				],
				"stats":{
					"total_values":3,
					"min":0,
					"max":453.12,
					"avg":23.254,
					"sum":12345
				}}`),
		[]byte(`{"field_name":"d",
				"counts":[
					{"count":12,"value":"value_1"},
					{"count":16,"value":"value_2"},
					{"count":0,"value":"value_3"}
				],
				"stats":{
					"total_values":3,
					"sum": 0
				}}`),
	}

	tsCounts := make([]tsApi.FacetCounts, len(inputData))
	for i, data := range inputData {
		var tsCount tsApi.FacetCounts
		err := jsoniter.Unmarshal(data, &tsCount)
		assert.NoError(t, err)
		tsCounts[i] = tsCount
	}

	t.Run("build with nil input", func(t *testing.T) {
		fr := FacetResponse{facetSizes: map[string]int{
			"a": 10,
		}}
		result := fr.Build(nil)
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("build with empty facetSizes", func(t *testing.T) {
		fr := FacetResponse{}
		result := fr.Build(&tsCounts)
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("query field is not included in ts response", func(t *testing.T) {
		fr := FacetResponse{facetSizes: map[string]int{
			"f": 10,
		}}
		result := fr.Build(&tsCounts)
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("requested facet size is lower than ts response", func(t *testing.T) {
		fr := FacetResponse{facetSizes: map[string]int{
			"a": 2,
			"b": 1,
			"c": 1,
		}}
		result := fr.Build(&tsCounts)
		require.NotNil(t, result)
		require.Len(t, result, len(fr.facetSizes))

		for field, size := range fr.facetSizes {
			require.Contains(t, result, field)
			require.LessOrEqual(t, len(result[field].GetCounts()), size)
		}
	})

	t.Run("validate all built facets", func(t *testing.T) {
		fr := FacetResponse{facetSizes: map[string]int{
			"a": 10,
			"b": 10,
			"c": 10,
			"d": 10,
		}}
		result := fr.Build(&tsCounts)
		require.NotNil(t, result)
		require.Len(t, result, len(fr.facetSizes))

		// field: 'a'
		a := result["a"]
		require.Len(t, a.Counts, 2)
		require.Equal(t, a.GetCounts(), []*api.FacetCount{
			{Count: 20, Value: "value_1"},
			{Count: 30, Value: "value_2"},
		})
		require.Equal(t, a.GetStats(), &api.FacetStats{Count: 0})

		// field: 'b'
		b := result["b"]
		require.Len(t, b.Counts, 2)
		require.Equal(t, b.GetCounts(), []*api.FacetCount{
			{Count: 14, Value: "value_1"},
			{Count: 12, Value: "value_2"},
		})
		require.Equal(t, b.GetStats(), &api.FacetStats{Count: 0})

		// field: 'c'
		c := result["c"]
		require.Len(t, c.Counts, 3)
		require.Equal(t, c.GetCounts(), []*api.FacetCount{
			{Count: 20, Value: "value_1"},
			{Count: 30, Value: "value_2"},
			{Count: 10, Value: "value_3"},
		})
		avg, max, min, sum := 23.254, 453.12, float64(0), float64(12345)
		require.Equal(t, c.GetStats(), &api.FacetStats{
			Avg:   &avg,
			Max:   &max,
			Min:   &min,
			Sum:   &sum,
			Count: 3,
		})

		// field: 'd'
		d := result["d"]
		require.Len(t, d.Counts, 3)
		require.Equal(t, d.GetCounts(), []*api.FacetCount{
			{Count: 12, Value: "value_1"},
			{Count: 16, Value: "value_2"},
			{Count: 0, Value: "value_3"},
		})
		sum = float64(0)
		require.Equal(t, d.GetStats(), &api.FacetStats{
			Avg:   nil,
			Max:   nil,
			Min:   nil,
			Sum:   &sum,
			Count: 3,
		})
	})
}
