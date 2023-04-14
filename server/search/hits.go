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
	"fmt"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	tsApi "github.com/tigrisdata/typesense-go/typesense/api"
)

const (
	matchedTokensKey = "matched_tokens"
)

type Hits struct {
	hits  []*Hit
	index int
}

func NewHits() *Hits {
	h := &Hits{
		index: 0,
	}

	return h
}

func (h *Hits) add(hit *Hit) {
	h.hits = append(h.hits, hit)
}

func (h *Hits) Next() (*Hit, error) {
	if h.index < len(h.hits) {
		hit := h.hits[h.index]
		h.index++
		return hit, nil
	}

	return nil, errors.Internal("no more hits to iterate")
}

func (h *Hits) Len() int {
	return len(h.hits)
}

func (h *Hits) HasMoreHits() bool {
	return h.index < len(h.hits)
}

type Hit struct {
	Document map[string]interface{}
	Match    *api.Match
}

// True - field absent in document
// True - field values is nil
// False - field has non-nil value.
func (sh *Hit) isFieldMissingOrNil(f string) bool {
	if v, ok := sh.Document[f]; !ok {
		return true
	} else {
		return v == nil
	}
}

func NewSearchHit(tsHit *tsApi.SearchResultHit) *Hit {
	if tsHit == nil || tsHit.Document == nil {
		return nil
	}

	var score string
	if tsHit.TextMatch != nil {
		score = fmt.Sprintf("%d", *tsHit.TextMatch)
	}

	var fields []*api.MatchField
	if tsHit.Highlight != nil {
		// check first in highlight
		fromHighlight(*tsHit.Highlight, &fields)
	} else if tsHit.Highlights != nil {
		fromHighlights(*tsHit.Highlights, &fields)
	}

	return &Hit{
		Document: *tsHit.Document,
		Match: &api.Match{
			Fields:         fields,
			Score:          score,
			VectorDistance: tsHit.VectorDistance,
		},
	}
}

func fromHighlights(highlights []tsApi.SearchHighlight, fields *[]*api.MatchField) {
	for _, f := range highlights {
		name := ""
		if f.Field != nil {
			name = *f.Field
		}
		*fields = append(*fields, &api.MatchField{
			Name: name,
		})
	}
}

func fromHighlight(highlight map[string]interface{}, fields *[]*api.MatchField) {
	// check first in highlight
	for name, value := range highlight {
		if mp, ok := value.(map[string]any); ok {
			// any non array match should just fall in this "if"
			if isMatchedToken(mp) {
				*fields = append(*fields, &api.MatchField{
					Name: name,
				})
			}
			continue
		}

		if mArr, ok := value.([]any); ok {
			for _, each := range mArr {
				if mp, ok := each.(map[string]any); ok {
					matchedToken := isMatchedToken(mp)
					if matchedToken {
						// simple array
						*fields = append(*fields, &api.MatchField{
							Name: name,
						})
						break
					}

					// now this means we can have an array of objects, we iterate one level
					// to build matched fields
					buildMatchedForArrObjects(name, mp, fields)
				}
			}
		}
	}
}
func buildMatchedForArrObjects(parent string, obj map[string]any, fields *[]*api.MatchField) {
	for k, v := range obj {
		if mpNested, ok := v.(map[string]any); ok {
			// nested element in an array of object is not an array
			if isMatchedToken(mpNested) {
				*fields = append(*fields, &api.MatchField{
					Name: parent + "." + k,
				})
			}
		} else if mpNestedArr, ok := v.([]any); ok {
			// nested element in an array of object is an array
			for _, eachNestedArr := range mpNestedArr {
				if eachNested, ok := eachNestedArr.(map[string]any); ok {
					if isMatchedToken(eachNested) {
						*fields = append(*fields, &api.MatchField{
							Name: parent + "." + k,
						})
						break
					}
				}
			}
		}
	}
}

func isMatchedToken(mp map[string]any) bool {
	if matched, found := mp[matchedTokensKey]; found {
		if matchedSlice, ok := matched.([]any); ok {
			return len(matchedSlice) > 0
		}
	}

	return false
}
