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
	tsApi "github.com/tigrisdata/typesense-go/typesense/api"
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

func (r *ResponseFactory) GetResponse(response []tsApi.SearchResult) Response {
	resp := Response{}
	if r.inputQuery.IsGroupByQuery() {
		resp.groups = r.GetGroupedHitsIterator(response)
	} else {
		resp.hits = r.GetHitsIterator(response)
	}

	return resp
}

func (*ResponseFactory) GetGroupedHitsIterator(response []tsApi.SearchResult) *Groups {
	groups := NewGroups()
	for _, r := range response {
		if r.GroupedHits != nil {
			for _, g := range *r.GroupedHits {
				hits := NewHits()
				for i := range g.Hits {
					hits.add(NewSearchHit(&g.Hits[i]))
				}

				groups.add(NewGroup(g.GroupKey, hits.hits))
			}
		}
	}

	return groups
}

// GetHitsIterator returns an IHits interface which contains hits results in an order that we need to stream out to the user.
func (*ResponseFactory) GetHitsIterator(response []tsApi.SearchResult) IHits {
	var hits IHitsMutable = NewHits()

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

type Response struct {
	hits   IHits
	groups *Groups
}

func (r *Response) HasMore() bool {
	if r.groups != nil {
		return r.groups.HasMoreGroups()
	}
	return r.hits.HasMoreHits()
}

func (r *Response) Next() (*ResultRow, error) {
	if r.groups != nil {
		group, err := r.groups.Next()
		if err != nil {
			return nil, err
		}

		return &ResultRow{
			Group: group,
		}, nil
	}

	hit, err := r.hits.Next()
	if err != nil {
		return nil, err
	}

	return &ResultRow{
		Hit: hit,
	}, nil
}

type ResultRow struct {
	Hit   *Hit
	Group *Group
}
