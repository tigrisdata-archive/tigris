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
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/lib/date"
	tsApi "github.com/tigrisdata/typesense-go/typesense/api"
)

func TestNewSearchHit(t *testing.T) {
	allDocs := make([]document, 0, len(documents))
	for _, d := range documents {
		allDocs = append(allDocs, d)
	}
	tsHits := generateTsHits(allDocs...)

	t.Run("with valid input", func(t *testing.T) {
		searchHits := make([]*Hit, len(tsHits))
		for i := range tsHits {
			searchHits[i] = NewSearchHit(&tsHits[i])
		}

		assert.Len(t, searchHits, len(tsHits))
		for i, hit := range searchHits {
			assert.NotNil(t, hit)
			assert.Equal(t, fmt.Sprintf("%d", *tsHits[i].TextMatch), hit.Match.Score)
			assert.Equal(t, *tsHits[i].Document, hit.Document)
		}
	})

	t.Run("with nil ts hit", func(t *testing.T) {
		hit := NewSearchHit(nil)
		assert.Nil(t, hit)
	})

	t.Run("with nil document", func(t *testing.T) {
		hit := NewSearchHit(&tsApi.SearchResultHit{
			Document: nil,
		})
		assert.Nil(t, hit)
	})

	t.Run("with empty document", func(t *testing.T) {
		d := make(map[string]interface{})
		hit := NewSearchHit(&tsApi.SearchResultHit{
			Document: &d,
		})
		assert.NotNil(t, hit)
		assert.Empty(t, hit.Document)
	})

	t.Run("with nil text match score", func(t *testing.T) {
		d := make(map[string]interface{})
		hit := NewSearchHit(&tsApi.SearchResultHit{
			Document:  &d,
			TextMatch: nil,
		})
		assert.Equal(t, "", hit.Match.Score)
	})
}

func TestSearchHit_isFieldMissingOrNil(t *testing.T) {
	doc := map[string]interface{}{
		"str_field":         "hello",
		"zero_field":        0,
		"false_field":       false,
		"true_field":        true,
		"nil_field":         nil,
		"empty_slice_field": []string{},
	}
	tsHit := tsApi.SearchResultHit{Document: &doc}
	searchHit := NewSearchHit(&tsHit)

	t.Run("when field is absent", func(t *testing.T) {
		assert.True(t, searchHit.isFieldMissingOrNil("some_field"))
	})

	t.Run("when field value is nil", func(t *testing.T) {
		assert.True(t, searchHit.isFieldMissingOrNil("nil_field"))
	})

	t.Run("when field has non-nil value", func(t *testing.T) {
		fields := []string{"str_field", "zero_field", "false_field", "true_field", "empty_slice_field"}
		for _, f := range fields {
			assert.False(t, searchHit.isFieldMissingOrNil(f))
		}
	})
}

func TestMatchedFields(t *testing.T) {
	cases := []struct {
		resp       []byte
		expMatched []string
	}{
		{
			[]byte(`{
"results": [{
		"hits": [{
            "document": {},
			"highlight": {
				"arr_obj": [{
					"domain": { "matched_tokens": [], "snippet": "regional24-7.com"},
					"arr": [{"matched_tokens": [], "snippet": "Daihatsu"}, {"matched_tokens": [],"snippet": "Chrysler"}]
				}, {
					"domain": { "matched_tokens": [], "snippet": "internalorchestrate.name"},
					"arr": [{"matched_tokens": ["Dino"],"snippet": "<mark>Dino</mark>"}, {"matched_tokens": [],"snippet": "Skoda"}, {"matched_tokens": ["Dino"],"snippet": "<mark>Dino</mark>"}]
				}, {
					"domain": {"matched_tokens": [],"snippet": "nationalincubate.net"},
					"arr": [{"matched_tokens": [],"snippet": "Daewoo"}, {"matched_tokens": [],"snippet": "Cadillac"}]
				}]
			}
		}, {
            "document": {},
			"highlight": {
				"arr_obj": [{
					"domain": {"matched_tokens": [],"snippet": "internalorchestrate.name"},
					"arr": [{"matched_tokens": ["Dino"],"snippet": "<mark>Dino</mark>"}, {"matched_tokens": [],"snippet": "Skoda"}, {"matched_tokens": ["Dino"],"snippet": "<mark>Dino</mark>"}]
				}, {
					"domain": {	"matched_tokens": [],"snippet": "globalstrategic.net"},
					"arr": [{"matched_tokens": [],"snippet": "Skoda"}, {"matched_tokens": [],"snippet": "Volkswagen"}, {"matched_tokens": ["Dino"],"snippet": "<mark>Dino</mark>"}]
				}]
			}
		}]
	}]
}`),
			[]string{"arr_obj.arr", "arr_obj.arr", "arr_obj.arr"},
		},
		{
			[]byte(`{
	"results": [{
		"hits": [{
            "document": {},
 			"highlight": {
 				"nested.address.city": {
 					"matched_tokens": ["Omaha"],
 					"snippet": "<mark>Omaha</mark>"
 				}
 			}
		}]
	}]
}`),
			[]string{"nested.address.city"},
		},
		{
			[]byte(`{
	"results": [{
		"hits": [{
            "document": {},
            "highlight": {
				"arr_obj": [{
					"domain": {"matched_tokens": ["regional24-7.com"], "snippet": "<mark>regional24-7.com</mark>"},
					"arr": [{"matched_tokens": [], "snippet": "Chrysler"}]
				}, {
					"domain": {"matched_tokens": [],"snippet": "internalorchestrate.name"},
					"arr": [{"matched_tokens": [],"snippet": "Dino"}]
				}] 
			}
		}]
	}]
}`),
			[]string{"arr_obj.domain"},
		},
		{
			[]byte(`{
	"results": [{
		"hits": [{
            "document": {},
            "highlight": {
				"arr_obj": [{
					"domain": { "matched_tokens": [], "snippet": "regional24-7.com"},
					"arr": [{"matched_tokens": [], "snippet": "Daihatsu"}]
				}, {
					"domain": { "matched_tokens": [], "snippet": "internalorchestrate.name"},
					"arr": [{"matched_tokens": ["Dino"],"snippet": "<mark>Dino</mark>"}]
				}],
				"commands_obj.name": {
					"matched_tokens": ["dino"],
					"snippet": "<mark>dino</mark>"
				},
				"name": {
					"matched_tokens": ["dino"],
					"snippet": "<mark>dino</mark>"
				}
			}
		}]
	}]
}`),
			[]string{"arr_obj.arr", "commands_obj.name", "name"},
		},
	}
	for _, c := range cases {
		var actualMatched []string
		var dest tsApi.MultiSearchResult
		require.NoError(t, jsoniter.Unmarshal(c.resp, &dest))
		for _, d := range dest.Results {
			for i := range *d.Hits {
				h := NewSearchHit(&(*d.Hits)[i])
				for _, m := range h.Match.Fields {
					actualMatched = append(actualMatched, m.Name)
				}
			}
		}

		sort.Strings(c.expMatched)
		sort.Strings(actualMatched)
		require.Equal(t, c.expMatched, actualMatched)
	}
}

func dateFrom(dateStr string) int64 {
	// swallowing error as the parameters are not expected to cause side effects
	d, _ := date.ToUnixNano(time.RFC3339Nano, dateStr)
	return d
}

type document map[string]interface{}

var documents = map[string]document{
	"complete_document": {
		"id":          0,
		"description": "complete document",
		"balance":     234.56,
		"priority":    4,
		"_text_match": int64(20),
		"created_at":  dateFrom("2022-10-18T00:51:07Z"),
		"has_credit":  false,
	},
	"missing_balance_1": {
		"id":          1,
		"description": "missing balance 1",
		"priority":    2,
		"_text_match": int64(98),
		"created_at":  dateFrom("2022-10-18T00:52:07Z"),
		"has_credit":  true,
	},
	"same_date_as_0": {
		"id":          2,
		"description": "same date as id=0",
		"priority":    7,
		"balance":     40,
		"_text_match": int64(56),
		"created_at":  dateFrom("2022-10-18T00:51:07Z"),
		"has_credit":  false,
	},
	"missing_balance_2": {
		"id":          3,
		"description": "missing balance 2",
		"priority":    1,
		"_text_match": int64(35),
		"created_at":  dateFrom("2022-10-21T03:55:07Z"),
		"has_credit":  true,
	},
	"complete_document_2": {
		"id":          4,
		"description": "complete document 2",
		"balance":     94.19,
		"priority":    6,
		"_text_match": int64(96),
		"created_at":  dateFrom("2022-10-18T00:51:07Z"),
		"has_credit":  false,
	},
	"same_balance_date_as_2": {
		"id":          5,
		"description": "same date, bal as id=2",
		"priority":    3,
		"balance":     40,
		"_text_match": int64(79),
		"created_at":  dateFrom("2022-10-18T00:51:07Z"),
		"has_credit":  true,
	},
	"missing_date": {
		"id":          6,
		"description": "missing date",
		"balance":     234.56,
		"priority":    5,
		"_text_match": int64(37),
		"has_credit":  false,
	},
}

// helper to generate hits.
func generateTsHits(docs ...document) []tsApi.SearchResultHit {
	hits := make([]tsApi.SearchResultHit, 0, len(docs))
	for _, doc := range docs {
		encoded, err := jsoniter.Marshal(doc)
		if err != nil {
			panic(err)
		}
		reader := bytes.NewReader(encoded)
		decoder := jsoniter.NewDecoder(reader)
		decoder.UseNumber()
		var decoded map[string]interface{}
		_ = decoder.Decode(&decoded)
		score := doc["_text_match"].(int64)
		hits = append(hits, tsApi.SearchResultHit{
			Document:  &decoded,
			TextMatch: &score,
		})
	}
	return hits
}
