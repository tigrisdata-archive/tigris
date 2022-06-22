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
	"github.com/tigrisdata/tigris/query/filter"
)

const (
	all = "*"
)

type Query struct {
	Q        string
	Fields   []string
	Facets   Facets
	WrappedF *filter.WrappedFilter
	PageSize int
}

func (q *Query) ToSearchFacetSize() int {
	var maxSize = 0
	for _, f := range q.Facets.Fields {
		if maxSize < f.Size {
			maxSize = f.Size
		}
	}

	if len(q.Facets.Fields) > 0 && maxSize == 0 {
		return defaultFacetSize
	}

	return maxSize
}

func (q *Query) ToSearchFacets() string {
	if len(q.Facets.Fields) == 0 {
		return ""
	}

	var facets string
	for i, f := range q.Facets.Fields {
		if i != 0 {
			facets += ","
		}
		facets += f.Name
	}

	return facets
}

func (q *Query) ToSearchFields() string {
	var fields string
	for i, f := range q.Fields {
		if i != 0 {
			fields += ","
		}
		fields += f
	}
	return fields
}

func (q *Query) ToSearchFilter() string {
	return q.WrappedF.Filter.ToSearchFilter()
}

type Builder struct {
	query *Query
}

func NewBuilder() *Builder {
	return &Builder{
		query: &Query{
			Q: all,
		},
	}
}

func (b *Builder) Query(q string) *Builder {
	b.query.Q = q
	return b
}

func (b *Builder) Filter(w *filter.WrappedFilter) *Builder {
	b.query.WrappedF = w
	return b
}

func (b *Builder) Facets(facets Facets) *Builder {
	b.query.Facets = facets
	return b
}

func (b *Builder) SearchFields(f []string) *Builder {
	b.query.Fields = f
	return b
}

func (b *Builder) PageSize(s int) *Builder {
	b.query.PageSize = s
	return b
}

func (b *Builder) Build() *Query {
	return b.query
}
