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
	"github.com/tigrisdata/tigris/query/search"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

// ResponseFactory is used to convert raw hits response to our Iterable that has final order of hits.
type ResponseFactory struct {
	inputQuery *search.Query
}

func NewResponseFactory(inputQuery *search.Query) *ResponseFactory {
	return &ResponseFactory{
		inputQuery: inputQuery,
	}
}

// GetHitsIterator returns an IHits interface which contains hits results in an order that we need to stream out to the user.
func (r *ResponseFactory) GetHitsIterator(response []tsApi.SearchResult) IHits {
	var hits IHitsMutable
	if r.inputQuery.SortOrder != nil && len(r.inputQuery.WrappedF.SearchFilter()) > 1 {
		hits = NewSortedHits(r.inputQuery.SortOrder)
	} else {
		hits = NewHits()
	}

	for _, r := range response {
		if r.Hits != nil {
			for i := range *r.Hits {
				hits.add(NewSearchHit(&(*r.Hits)[i]))
			}
		}
	}

	return IHits(hits)
}

type IHitsMutable interface {
	iHitsImmutable
	IHits
}

type iHitsImmutable interface {
	add(hit *Hit)
}

type IHits interface {
	Next() (*Hit, error)
	Len() int
	HasMoreHits() bool
}
