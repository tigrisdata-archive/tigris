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

package v1

import tsApi "github.com/typesense/typesense-go/typesense/api"

type SearchResponse struct {
	Hits   *HitsResponse
	Facets *FacetResponse
}

type HitsResponse struct {
	Hits *[]tsApi.SearchResultHit
}

func NewHitsResponse() *HitsResponse {
	var hits []tsApi.SearchResultHit
	return &HitsResponse{
		Hits: &hits,
	}
}

func (h *HitsResponse) Append(hits *[]tsApi.SearchResultHit) {
	if hits != nil {
		*h.Hits = append(*h.Hits, *hits...)
	}
}

func (h *HitsResponse) GetDocument(idx int) (*map[string]interface{}, bool) {
	if idx < len(*h.Hits) {
		return (*h.Hits)[idx].Document, true
	}

	return nil, false
}

func (h *HitsResponse) HasMoreHits(idx int) bool {
	return idx < len(*h.Hits)
}

type FacetResponse struct {
	Facets *[]tsApi.FacetCounts
}

func CreateFacetResponse(facets *[]tsApi.FacetCounts) *FacetResponse {
	if facets != nil {
		return &FacetResponse{
			Facets: facets,
		}
	}

	return nil
}
