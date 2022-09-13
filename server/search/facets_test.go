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

package search

import (
	"testing"
	"github.com/stretchr/testify/assert"
	tsApi "github.com/typesense/typesense-go/typesense/api"
	"encoding/json"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

func TestFacetCountComparator(t *testing.T) {
	testCases := []struct {
		name     string
		this     *FacetCount
		that     *FacetCount
		expected bool
	}{
		{
			"both values nil", nil, nil, true,
		},
		{
			"this is nil", nil, &FacetCount{"that", 20}, false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, facetCountComparator(tc.this, tc.that))
		})
	}
}

func TestSortedFacets(t *testing.T) {

	t.Run("Retrieves facet counts in a descending order", func(t *testing.T) {
		var tsCounts tsApi.FacetCounts
		facets := NewSortedFacets()

		inputData := [][]byte{
			[]byte(`{"field_name":"field_1","counts":[{"count":20,"value":"value_1"},{"count":30,"value":"value_2"},{"count":10,"value":null}]}`),
			[]byte(`{"field_name":"field_2","counts":[{"count":14,"value":"value_1"},{"count":12,"value":"value_2"}]}`),
			[]byte(`{"field_name":null,"counts":[{"count":20,"value":"value_1"},{"count":30,"value":"value_2"},{"count":10,"value":null}]}`),
			[]byte(`{"field_name":"field_1","counts":[{"count":20,"value":"value_1"},{"count":30,"value":"value_2"},{"count":10,"value":null},{"count":10,"value":"value_3"}]}`),
		}

		for _, data := range inputData {
			err := json.Unmarshal(data, &tsCounts)
			assert.NoError(t, err)
			_ = facets.Add(&tsCounts)
		}

		expectedField1Order := []FacetCount{
			{"value_2", int64(60)},
			{"value_1", int64(40)},
			{"value_3", int64(10)},
		}

		for _, o := range expectedField1Order {
			fc, _ := facets.GetFacetCount("field_1")
			assert.Equal(t, o, *fc)
		}

		fc, hasMore := facets.GetFacetCount("field_1")
		assert.Nil(t, fc)
		assert.False(t, hasMore)

		expectedField2Order := []FacetCount{
			{"value_1", int64(14)},
			{"value_2", int64(12)},
		}
		for _, o := range expectedField2Order {
			fc, _ := facets.GetFacetCount("field_2")
			assert.Equal(t, o, *fc)
		}
		fc, hasMore = facets.GetFacetCount("field_2")
		assert.Nil(t, fc)
		assert.False(t, hasMore)
	})

	t.Run("add nil facet counts", func(t *testing.T) {
		facets := NewSortedFacets()
		err := facets.Add(nil)
		assert.NoError(t, err)
		assert.Empty(t, facets.facetAttrs)
	})

	t.Run("facet count with nil field name", func(t *testing.T) {
		facets := NewSortedFacets()
		tsCounts := &tsApi.FacetCounts{FieldName: nil}
		err := facets.Add(tsCounts)
		assert.NoError(t, err)
		assert.Empty(t, facets.facetAttrs)
	})

	t.Run("facet count with nil values only", func(t *testing.T) {
		field1 := "field_1"
		tsCounts := &tsApi.FacetCounts{FieldName: &field1}
		facets := NewSortedFacets()
		err := facets.Add(tsCounts)
		assert.NoError(t, err)
		assert.Empty(t, facets.facetAttrs[field1].counts)
	})

	t.Run("build facet stats", func(t *testing.T) {
		facets := NewSortedFacets()

		inputData := [][]byte{
			[]byte(`{"field_name":"field_1", "stats": {"total_values": 2, "avg": 10, "max": 20.21, "min": -30, "sum": 50}}`),
			[]byte(`{"field_name":"field_2", "stats": {"total_values": 64, "avg": 10.56, "min": -30.50}}`),
			[]byte(`{"field_name":"field_3", "stats": null}`),
			[]byte(`{"field_name":"field_1", "stats": {"total_values": 128, "avg": 10, "max": 20, "min": -10.34, "sum": 50}}`),
		}

		for _, data := range inputData {
			var tsCounts tsApi.FacetCounts
			err := json.Unmarshal(data, &tsCounts)
			assert.NoError(t, err)

			err = facets.Add(&tsCounts)
			assert.NoError(t, err)
		}

		assert.Equal(t, &api.FacetStats{Count: 130}, facets.GetStats("field_1"))
		f2Avg, f2Min := float64(float32(10.56)), -30.5
		assert.Equal(t, &api.FacetStats{Avg: &f2Avg, Min: &f2Min, Count: 64}, facets.GetStats("field_2"))
		assert.Equal(t, &api.FacetStats{Count: 0}, facets.GetStats("field_3"))
	})

	t.Run("Cannot insert once sorted", func(t *testing.T) {
		var tsCounts tsApi.FacetCounts
		data := []byte(`{"field_name":"field_2","counts":[{"count":14,"value":"value_1"},{"count":12,"value":"value_2"}]}`)
		err := json.Unmarshal(data, &tsCounts)
		assert.NoError(t, err)

		facets := NewSortedFacets()
		err = facets.Add(&tsCounts)
		assert.NoError(t, err)

		_, _ = facets.GetFacetCount("field_2")
		err = facets.Add(&tsCounts)
		assert.ErrorContains(t, err, "Already initialized and sorted")
	})
}
