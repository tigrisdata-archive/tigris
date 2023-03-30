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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/query/search"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

// FacetResponse is a builder utility to convert facets from search backend to tigris response
type FacetResponse struct {
	// count of facet values requested for each field
	facetSizes map[string]int
}

func NewFacetResponse(query search.Facets) *FacetResponse {
	facetSizeRequested := map[string]int{}
	for _, f := range query.Fields {
		facetSizeRequested[f.Name] = f.Size
	}
	return &FacetResponse{facetSizes: facetSizeRequested}
}

// Build converts search backend response to api.SearchFacet
func (fb *FacetResponse) Build(r *[]tsApi.FacetCounts) map[string]*api.SearchFacet {
	result := map[string]*api.SearchFacet{}

	// return empty map
	if r == nil {
		return result
	}

	for _, fc := range *r {
		// skip if no field name
		if fc.FieldName == nil {
			continue
		}
		fieldName := *fc.FieldName
		// skip if this facets for this field name were not requested, likely not to happen
		if _, ok := fb.facetSizes[fieldName]; !ok {
			continue
		}

		stats := &api.FacetStats{}
		if fc.Stats != nil {
			if fc.Stats.Avg != nil {
				stats.Avg = fc.Stats.Avg
			}
			if fc.Stats.Max != nil {
				stats.Max = fc.Stats.Max
			}
			if fc.Stats.Min != nil {
				stats.Min = fc.Stats.Min
			}
			if fc.Stats.Sum != nil {
				stats.Sum = fc.Stats.Sum
			}
			if fc.Stats.TotalValues != nil {
				stats.Count = int64(*fc.Stats.TotalValues)
			}
		}

		facet := &api.SearchFacet{
			Counts: []*api.FacetCount{},
			Stats:  stats,
		}

		if fc.Counts != nil {
			for _, tsCount := range *fc.Counts {
				// skip null 'value' for a faceted field
				// skip if the user requested size for a facet field has been met
				if tsCount.Value == nil || len(facet.Counts) >= fb.facetSizes[fieldName] {
					continue
				}
				facet.Counts = append(facet.Counts, &api.FacetCount{
					Count: int64(*tsCount.Count),
					Value: *tsCount.Value,
				})
			}
		}
		result[fieldName] = facet
	}

	return result
}
