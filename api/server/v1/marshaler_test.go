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

package api

import (
	"testing"

	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestJSONEncoding(t *testing.T) {
	// ToDo: add marshaler tests
	inputDoc := []byte(`{"pkey_int": 1, "int_value": 2, "str_value": "foo"}`)
	b, err := jsoniter.Marshal(inputDoc)
	require.NoError(t, err)

	var bb []byte
	require.NoError(t, jsoniter.Unmarshal(b, &bb))

	t.Run("unmarshal SearchRequest", func(t *testing.T) {
		inputDoc := []byte(`{"q":"my search text","search_fields":["first_name","last_name"],
							"filter":{"last_name":"Steve"},"facet":{"facet stat":0},
							"sort":[{"salary":"$asc"}],"include_fields":["employment","history"]}`)

		req := &SearchRequest{}
		err := jsoniter.Unmarshal(inputDoc, req)
		require.NoError(t, err)
		require.Equal(t, "my search text", req.GetQ())
		require.Equal(t, []string{"first_name", "last_name"}, req.GetSearchFields())
		require.Equal(t, []byte(`{"last_name":"Steve"}`), req.GetFilter())
		require.Equal(t, []byte(`{"facet stat":0}`), req.GetFacet())
		require.Equal(t, []byte(`[{"salary":"$asc"}]`), req.GetSort())
		require.Equal(t, []string{"employment", "history"}, req.GetIncludeFields())
	})

	t.Run("marshal SearchResponse", func(t *testing.T) {
		avg := float64(40)
		resp := &SearchResponse{
			Hits: nil,
			Facets: map[string]*SearchFacet{
				"myField": {
					Counts: []*FacetCount{{
						Count: 32,
						Value: "adidas",
					}},
					Stats: &FacetStats{
						Count: 50,
						Avg:   &avg,
					},
				},
			},
			Meta: &SearchMetadata{
				TotalPages: 0,
				Found:      1234,
				Page: &Page{
					Current: 2,
					Size:    10,
				},
			},
		}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"hits":[],"facets":{"myField":{"counts":[{"count":32,"value":"adidas"}],"stats":{"avg":40,"count":50}}},"meta":{"found":1234, "matched_fields":null, "total_pages":0,"page":{"current":2,"size":10}}}`, string(r))
	})
}

func TestUsage_MarshalJSON(t *testing.T) {
	ts := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
	t.Run("empty", func(t *testing.T) {
		resp := &Usage{}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(r))
	})

	t.Run("complete object", func(t *testing.T) {
		resp := &Usage{
			StartTime: timestamppb.New(ts),
			EndTime:   timestamppb.New(ts),
			Value:     22,
		}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"start_time":"2023-05-01T00:00:00Z","end_time":"2023-05-01T00:00:00Z","value":22}`, string(r))
	})
	t.Run("partial object", func(t *testing.T) {
		resp := &Usage{
			StartTime: timestamppb.New(ts),
			Value:     12,
		}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"start_time":"2023-05-01T00:00:00Z","value":12}`, string(r))
	})

	t.Run("zero value", func(t *testing.T) {
		resp := &Usage{
			EndTime: timestamppb.New(ts),
			Value:   0,
		}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"end_time":"2023-05-01T00:00:00Z","value":0}`, string(r))
	})
}

func TestInvoice_MarshalJSON(t *testing.T) {
	ts := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
	t.Run("empty", func(t *testing.T) {
		resp := &Invoice{}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"total":0,"subtotal":0,"entries":[]}`, string(r))
	})

	t.Run("complete object", func(t *testing.T) {
		resp := &Invoice{
			Id: "1",
			Entries: []*InvoiceLineItem{
				{
					Name:     "db",
					Quantity: 1.5,
					Total:    4.55,
					Charges: []*Charge{
						{
							Name:     "dru",
							Quantity: 5.0,
							Subtotal: 8.70,
							Tiers: []*ChargeTier{
								{
									StartingAt: 3.2,
									Quantity:   4.0,
									Price:      1.8,
									Subtotal:   7.2,
								},
							},
						},
					},
				},
			},
			StartTime: timestamppb.New(ts),
			EndTime:   timestamppb.New(ts),
			Subtotal:  15.10,
			Total:     30.50,
			PlanName:  "personal db",
		}

		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"id":"1","entries":[{"name":"db","quantity":1.5,"total":4.55,"charges":[{"name":"dru","quantity":5,"subtotal":8.7,"tiers":[{"starting_at":3.2,"quantity":4,"price":1.8,"subtotal":7.2}]}]}],"start_time":"2023-05-01T00:00:00Z","end_time":"2023-05-01T00:00:00Z","subtotal":15.1,"total":30.5,"plan_name":"personal db"}`, string(r))
	})

	t.Run("partial Invoice", func(t *testing.T) {
		resp := &Invoice{
			Id:       "123",
			EndTime:  nil,
			Total:    0,
			PlanName: "personal db",
		}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"id":"123","plan_name":"personal db","subtotal":0,"total":0,"entries":[]}`, string(r))
	})

	t.Run("empty InvoiceLineItem", func(t *testing.T) {
		resp := &InvoiceLineItem{}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"total":0}`, string(r))
	})

	t.Run("empty charge", func(t *testing.T) {
		resp := &Charge{}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"quantity":0,"subtotal":0}`, string(r))
	})
	t.Run("empty charge tier", func(t *testing.T) {
		resp := &ChargeTier{}
		r, err := jsoniter.Marshal(resp)
		require.NoError(t, err)
		require.JSONEq(t, `{"starting_at":0,"quantity":0,"price":0,"subtotal":0}`, string(r))
	})
}

func TestQueryMetricsRequest(t *testing.T) {
	exp := `{
    "db": "db1",
    "branch": "br1",
    "collection": "coll1",
    "from": 123,
    "to": 234,
    "metric_name": "metric1",
    "quantile": 0.99,
    "tigris_operation": "ALL",
    "space_aggregation": "SUM",
    "space_aggregated_by": [ "field1", "field2" ],
    "function": "RATE",
    "additional_functions": [{
            "rollup": {
				"aggregator": "SUM",
                "interval": 12
            }
        }]
	}`

	var r1 QueryTimeSeriesMetricsRequest
	err := jsoniter.Unmarshal([]byte(exp), &r1)
	require.NoError(t, err)

	expReq := QueryTimeSeriesMetricsRequest{
		Db:                "db1",
		Branch:            "br1",
		Collection:        "coll1",
		From:              123,
		To:                234,
		MetricName:        "metric1",
		TigrisOperation:   TigrisOperation_ALL,
		SpaceAggregation:  MetricQuerySpaceAggregation_SUM,
		SpaceAggregatedBy: []string{"field1", "field2"},
		Function:          MetricQueryFunction_RATE,
		Quantile:          0.99,
		AdditionalFunctions: []*AdditionalFunction{
			{
				Rollup: &RollupFunction{
					Aggregator: RollupAggregator_ROLLUP_AGGREGATOR_SUM,
					Interval:   12,
				},
			},
		},
	}

	require.Equal(t, &expReq, &r1)
}
