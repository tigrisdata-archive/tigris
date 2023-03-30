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
	tsApi "github.com/typesense/typesense-go/typesense/api"
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
	Document       map[string]interface{}
	TextMatchScore int64
	Match          *api.Match
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

	score := int64(0)
	if tsHit.TextMatch != nil {
		score = *tsHit.TextMatch
	}

	var fields []*api.MatchField
	if tsHit.Highlights != nil {
		for _, f := range *tsHit.Highlights {
			name := ""
			if f.Field != nil {
				name = *f.Field
			}
			fields = append(fields, &api.MatchField{
				Name: name,
			})
		}
	}

	return &Hit{
		Document:       *tsHit.Document,
		TextMatchScore: score,
		Match: &api.Match{
			Fields: fields,
			Score:  fmt.Sprintf("%d", score),
		},
	}
}
