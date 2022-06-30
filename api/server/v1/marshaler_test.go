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

package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONEncoding(t *testing.T) {
	// ToDo: add marshaler tests
	inputDoc := []byte(`{"pkey_int": 1, "int_value": 2, "str_value": "foo"}`)
	b, err := json.Marshal(inputDoc)
	require.NoError(t, err)

	var bb []byte
	require.NoError(t, json.Unmarshal(b, &bb))

	t.Run("unmarshal SearchRequest", func(t *testing.T) {
		inputDoc := []byte(`{"q":"my search text","search_fields":["first_name","last_name"],
							"filter":{"last_name":"Steve"},"facet":{"facet stat":0},
							"sort":[{"salary":"$asc"}],"fields":["employment","history"]}`)

		req := &SearchRequest{}
		err := json.Unmarshal(inputDoc, req)
		require.NoError(t, err)
		require.Equal(t, "my search text", req.GetQ())
		require.Equal(t, []string{"first_name", "last_name"}, req.GetSearchFields())
		require.Equal(t, []byte(`{"last_name":"Steve"}`), req.GetFilter())
		require.Equal(t, []byte(`{"facet stat":0}`), req.GetFacet())
		require.Equal(t, []byte(`[{"salary":"$asc"}]`), req.GetSort())
		require.Equal(t, []byte(`["employment","history"]`), req.GetFields())
	})

	t.Run("marshal SearchResponse", func(t *testing.T) {
		resp := &SearchResponse{
			Hits: []*SearchHit{{
				Data:     nil,
				Metadata: &SearchHitMeta{},
			}},
			Facets: map[string]*SearchFacet{
				"myField": {
					Counts: []*FacetCount{{
						Count: 32,
						Value: "adidas",
					}},
					Stats: &FacetStats{
						Count: 50,
						Avg:   40,
					},
				},
			},
			Meta: &SearchMetadata{
				Found: 1234,
				Page: &Page{
					Current: 2,
					Size:    10,
				},
			}}
		r, err := json.Marshal(resp)
		require.NoError(t, err)
		require.Equal(t, []byte(`{"hits":[{"metadata":{}}],"facets":{"myField":{"counts":[{"count":32,"value":"adidas"}],"stats":{"avg":40,"count":50}}},"meta":{"found":1234,"page":{"current":2,"per_page":10}}}`), r)
	})
}
