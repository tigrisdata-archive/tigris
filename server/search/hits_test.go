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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/query/sort"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

func Test_hitsComparator(t *testing.T) {
	t.Run("when both search hits are nil", func(t *testing.T) {
		res := hitsComparator(nil, nil, &sort.Ordering{})
		assert.Equal(t, Equal, res)
	})

	t.Run("when one search hit is nil", func(t *testing.T) {
		hit := NewSearchHit(&generateTsHits(documents["complete_document"])[0])
		emptySortOrder := &sort.Ordering{}

		assert.Equal(t, This, hitsComparator(hit, nil, emptySortOrder))
		assert.Equal(t, That, hitsComparator(nil, hit, emptySortOrder))
	})

	t.Run("use text match when sorting order is nil", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["same_date_as_0"])
		lessScore, moreScore := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])

		assert.Equal(t, That, hitsComparator(lessScore, moreScore, nil))
	})

	t.Run("use text match when no sorting order specified", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["same_date_as_0"])
		lessScore, moreScore := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])

		assert.Equal(t, This, hitsComparator(moreScore, lessScore, nil))
	})

	t.Run("use text match when sorting field is missing in both", func(t *testing.T) {
		tsHits := generateTsHits(documents["missing_balance_1"], documents["missing_balance_2"])
		moreScore, lessScore := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])
		sortOrder := &sort.Ordering{
			{Name: "balance", Ascending: true, MissingValuesFirst: false},
		}

		assert.Equal(t, This, hitsComparator(moreScore, lessScore, sortOrder))
	})

	t.Run("use missing values order when only one document is missing field", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["missing_balance_2"])
		complete, missingField := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])

		// missing values to end
		sortOrder := &sort.Ordering{
			{Name: "balance", Ascending: true, MissingValuesFirst: false},
		}
		assert.Equal(t, This, hitsComparator(complete, missingField, sortOrder))

		// missing values to front
		sortOrder = &sort.Ordering{
			{Name: "balance", Ascending: true, MissingValuesFirst: true},
		}
		assert.Equal(t, That, hitsComparator(complete, missingField, sortOrder))
	})

	t.Run("when comparing on int values", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["missing_balance_2"])
		highPriority, lowPriority := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])

		// ascending
		sortOrder := &sort.Ordering{
			{Name: "priority", Ascending: true},
		}
		assert.Equal(t, This, hitsComparator(lowPriority, highPriority, sortOrder))

		// descending
		sortOrder = &sort.Ordering{
			{Name: "priority", Ascending: false},
		}
		assert.Equal(t, That, hitsComparator(lowPriority, highPriority, sortOrder))
	})

	t.Run("when comparing on int64 values", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["missing_balance_2"])
		lower, higher := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])

		// ascending
		sortOrder := &sort.Ordering{
			{Name: "created_at", Ascending: true},
		}
		assert.Equal(t, This, hitsComparator(lower, higher, sortOrder))

		// descending
		sortOrder = &sort.Ordering{
			{Name: "created_at", Ascending: false},
		}
		assert.Equal(t, That, hitsComparator(lower, higher, sortOrder))
	})

	t.Run("when comparing on float64 values", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["same_date_as_0"])
		higher, lower := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])

		// ascending
		sortOrder := &sort.Ordering{
			{Name: "balance", Ascending: true},
		}
		assert.Equal(t, This, hitsComparator(lower, higher, sortOrder))

		// descending
		sortOrder = &sort.Ordering{
			{Name: "balance", Ascending: false},
		}
		assert.Equal(t, That, hitsComparator(lower, higher, sortOrder))
	})

	t.Run("when comparing on boolean values", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["missing_balance_1"])
		falsy, truthy := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])

		// ascending
		sortOrder := &sort.Ordering{
			{Name: "has_credit", Ascending: true},
		}
		assert.Equal(t, That, hitsComparator(truthy, falsy, sortOrder))

		// descending
		sortOrder = &sort.Ordering{
			{Name: "has_credit", Ascending: false},
		}
		assert.Equal(t, This, hitsComparator(truthy, falsy, sortOrder))
	})

	t.Run("use text match when comparing string field values", func(t *testing.T) {
		tsHits := generateTsHits(documents["missing_balance_1"], documents["missing_balance_2"])
		moreScore, lessScore := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])
		sortOrder := &sort.Ordering{
			{Name: "description", Ascending: true},
		}

		assert.Equal(t, This, hitsComparator(moreScore, lessScore, sortOrder))
	})

	t.Run("use 2 sort orders but no tie breakers are necessary", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["missing_balance_2"])
		highPriority, lowPriority := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])
		sortOrder := &sort.Ordering{
			{Name: "priority", Ascending: true},
			{Name: "created_at", Ascending: false},
		}

		assert.Equal(t, This, hitsComparator(lowPriority, highPriority, sortOrder))
	})

	t.Run("use 2 sort orders and tie breakers are used", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["same_date_as_0"])
		lowPriority, highPriority := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])
		sortOrder := &sort.Ordering{
			{Name: "created_at", Ascending: false},
			{Name: "priority", Ascending: false},
		}

		assert.Equal(t, That, hitsComparator(lowPriority, highPriority, sortOrder))
	})

	t.Run("use text match score when both sort orders are equal", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"], documents["same_date_as_0"])
		lowScore, highScore := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[1])
		sortOrder := &sort.Ordering{
			{Name: "created_at", Ascending: false},
			{Name: "has_credit", Ascending: false},
		}

		assert.Equal(t, That, hitsComparator(lowScore, highScore, sortOrder))
	})

	t.Run("Equality check when documents are exactly equal", func(t *testing.T) {
		tsHits := generateTsHits(documents["complete_document"])
		// same document values
		firstHit, secondHit := NewSearchHit(&tsHits[0]), NewSearchHit(&tsHits[0])
		sortOrder := &sort.Ordering{
			{Name: "balance", Ascending: false},
			{Name: "priority", Ascending: true},
		}

		assert.Equal(t, Equal, hitsComparator(firstHit, secondHit, sortOrder))
	})
}

func TestSortedHits(t *testing.T) {
	searchHits := make([]*Hit, len(documents))
	i := 0
	for _, d := range documents {
		searchHits[i] = NewSearchHit(&generateTsHits(d)[0])
		i++
	}

	testCases := []struct {
		name          string
		inputHits     []*Hit
		sortOrder     *sort.Ordering
		expectedOrder []float64
	}{
		{
			name:          "sort by decreasing text match score when no sort order",
			inputHits:     searchHits,
			expectedOrder: []float64{1, 4, 5, 2, 6, 3, 0},
		},
		{
			name:          "sort by boolean field",
			inputHits:     searchHits,
			sortOrder:     &sort.Ordering{{Name: "has_credit", Ascending: false}},
			expectedOrder: []float64{1, 5, 3, 4, 2, 6, 0},
		},
		{
			name:      "sort using tie breakers",
			inputHits: searchHits,
			sortOrder: &sort.Ordering{
				{Name: "balance", Ascending: false},
				{Name: "created_at", Ascending: true},
			},
			expectedOrder: []float64{0, 6, 4, 5, 2, 1, 3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pq := NewSortedHits(tc.sortOrder)
			for _, h := range tc.inputHits {
				pq.Add(h)
			}

			assert.Equal(t, len(tc.expectedOrder), pq.Len())
			for _, expectedId := range tc.expectedOrder {
				hit, err := pq.Get()
				assert.NoError(t, err)
				assert.Equal(t, expectedId, hit.Document["id"])
			}
		})
	}

	t.Run("getting hit from empty sorted hits", func(t *testing.T) {
		pq := NewSortedHits(nil)
		hit, err := pq.Get()
		assert.Error(t, err)
		assert.Nil(t, hit)
	})

	t.Run("length of sorted hits", func(t *testing.T) {
		pq := NewSortedHits(nil)
		assert.Equal(t, 0, pq.Len())
		assert.False(t, pq.HasMoreHits())

		pq.Add(searchHits[0])
		assert.Equal(t, 1, pq.Len())
		assert.True(t, pq.HasMoreHits())

		_, _ = pq.Get()
		assert.Equal(t, 0, pq.Len())
		assert.False(t, pq.HasMoreHits())
	})
}

func TestNewSearchHit(t *testing.T) {
	var allDocs []document
	for _, d := range documents {
		allDocs = append(allDocs, d)
	}
	tsHits := generateTsHits(allDocs...)

	t.Run("with valid input", func(t *testing.T) {
		searchHits := make([]*Hit, len(tsHits))
		for i, h := range tsHits {
			searchHits[i] = NewSearchHit(&h)
		}

		assert.Len(t, searchHits, len(tsHits))
		for i, hit := range searchHits {
			assert.NotNil(t, hit)
			assert.Equal(t, *tsHits[i].TextMatch, hit.TextMatchScore)
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
		assert.Equal(t, int64(0), hit.TextMatchScore)
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

// helper to generate hits
func generateTsHits(docs ...document) []tsApi.SearchResultHit {
	var hits []tsApi.SearchResultHit
	for _, doc := range docs {
		encoded, _ := json.Marshal(doc)
		var decoded map[string]interface{}
		_ = json.Unmarshal(encoded, &decoded)
		score := doc["_text_match"].(int64)
		hits = append(hits, tsApi.SearchResultHit{
			Document:  &decoded,
			TextMatch: &score,
		})
	}
	return hits
}
