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
	"fmt"

	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	"github.com/tigrisdata/tigris/query/sort"
)

const (
	all = "*"
)

type Query struct {
	Q            string
	SearchFields []string
	Facets       Facets
	PageSize     int
	WrappedF     *filter.WrappedFilter
	ReadFields   *read.FieldFactory
	SortOrder    *sort.Ordering
}

func (q *Query) ToSearchFacetSize() int {
	maxSize := 0
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
	for i, f := range q.SearchFields {
		if i != 0 {
			fields += ","
		}
		fields += f
	}
	return fields
}

func (q *Query) ToSearchFilter() []string {
	return q.WrappedF.Filter.ToSearchFilter()
}

func (q *Query) ToSortFields() string {
	var sortBy string
	if q.SortOrder == nil {
		return sortBy
	}

	for i, f := range *q.SortOrder {
		if i != 0 {
			sortBy += ","
		}
		missingValue := "last"
		if f.MissingValuesFirst {
			missingValue = "first"
		}
		order := "desc"
		if f.Ascending {
			order = "asc"
		}

		sortBy += fmt.Sprintf("%s(missing_values: %s):%s", f.Name, missingValue, order)
	}
	return sortBy
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
	if len(q) == 0 {
		// don't override the default "*"
		return b
	}

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
	b.query.SearchFields = f
	return b
}

func (b *Builder) ReadFields(f *read.FieldFactory) *Builder {
	b.query.ReadFields = f
	return b
}

func (b *Builder) SortOrder(o *sort.Ordering) *Builder {
	b.query.SortOrder = o
	return b
}

func (b *Builder) PageSize(s int) *Builder {
	b.query.PageSize = s
	return b
}

func (b *Builder) Build() *Query {
	return b.query
}
