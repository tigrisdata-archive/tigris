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

package v1

import (
	"context"

	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/query/filter"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/store/search"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

const (
	defaultPerPage = 250
)

type page struct {
	idx        int
	cap        int
	err        error
	hits       *HitsResponse
	wrappedF   *filter.WrappedFilter
	collection *schema.DefaultCollection
}

func newPage(collection *schema.DefaultCollection, query *qsearch.Query) *page {
	return &page{
		idx:        0,
		hits:       NewHits(),
		cap:        query.PageSize,
		wrappedF:   query.WrappedF,
		collection: collection,
	}
}

func (p *page) append(h tsApi.SearchResultHit) bool {
	if !p.hasCapacity() {
		return false
	}

	p.hits.Append(h)
	return true
}

func (p *page) hasCapacity() bool {
	return p.hits.Count() < p.cap
}

// readRow should be used to read search data because this is the single point where we unpack search fields, apply
// filter and then pack the document into bytes.
func (p *page) readRow(row *Row) bool {
	if p.err != nil {
		return false
	}

	for p.hits.HasMoreHits(p.idx) {
		document, _ := p.hits.GetDocument(p.idx)
		p.idx++
		if document == nil {
			continue
		}

		var searchKey string
		if searchKey, p.err = UnpackSearchFields(document, p.collection); p.err != nil {
			return false
		}

		// now apply the filter
		if !p.wrappedF.Filter.MatchesDoc(document) {
			continue
		}

		data, err := jsoniter.Marshal(*document)
		if err != nil {
			p.err = err
			return false
		}

		row.Key = []byte(searchKey)
		row.Data = &internal.TableData{RawData: data}
		return true
	}

	return false
}

type pageReader struct {
	pages        []*page
	query        *qsearch.Query
	store        search.Store
	collection   *schema.DefaultCollection
	cachedFacets map[string]*api.SearchFacet
}

func newPageReader(store search.Store, coll *schema.DefaultCollection, query *qsearch.Query) *pageReader {
	return &pageReader{
		query:        query,
		store:        store,
		collection:   coll,
		cachedFacets: make(map[string]*api.SearchFacet),
	}
}

func (p *pageReader) read(ctx context.Context, pageNo int) error {
	result, err := p.store.Search(ctx, p.collection.SearchCollectionName(), p.query, pageNo)
	if err != nil {
		return err
	}

	var added = true
	var pg = newPage(p.collection, p.query)
	for _, r := range result {
		if r.Hits == nil {
			continue
		}

		for _, h := range *r.Hits {
			added = false
			if !pg.append(h) {
				p.pages = append(p.pages, pg)
				pg = newPage(p.collection, p.query)
				added = true
			}
		}

		if !added {
			p.pages = append(p.pages, pg)
			pg = newPage(p.collection, p.query)
		}
	}

	// check if we need to build facets
	if p.cachedFacets == nil {
		for _, r := range result {
			p.buildFacets(r.FacetCounts)
		}
	}

	return nil
}

func (p *pageReader) next(ctx context.Context, pageNo int) (bool, *page, error) {
	if len(p.pages) == 0 {
		if err := p.read(ctx, pageNo); err != nil {
			return false, nil, err
		}
	}

	if len(p.pages) == 0 {
		return true, nil, nil
	}

	pg, last := p.pages[0], p.pages[0].hasCapacity()
	p.pages = p.pages[:0]
	return last, pg, nil
}

func (p *pageReader) buildFacets(facets *[]tsApi.FacetCounts) {
	for _, f := range *facets {
		var facet = &api.SearchFacet{
			Stats: p.buildStats(f),
		}

		if f.Counts != nil {
			for _, c := range *f.Counts {
				facet.Counts = append(facet.Counts, &api.FacetCount{
					Count: int64(*c.Count),
					Value: *c.Value,
				})
			}
		}

		p.cachedFacets[*f.FieldName] = facet
	}
}

func (p *pageReader) buildStats(stats tsApi.FacetCounts) *api.FacetStats {
	if stats.Stats == nil {
		return nil
	}

	var stat = &api.FacetStats{}
	if stats.Stats.Avg != nil {
		stat.Avg = *stats.Stats.Avg
	}
	if stats.Stats.Min != nil {
		stat.Min = int64(*stats.Stats.Min)
	}
	if stats.Stats.Max != nil {
		stat.Max = int64(*stats.Stats.Max)
	}
	if stats.Stats.Sum != nil {
		stat.Sum = int64(*stats.Stats.Sum)
	}
	return stat
}

// SearchRowReader is responsible for iterating on the search results. It uses pageReader internally to read page
// and then iterate on documents inside hits.
type SearchRowReader struct {
	pageNo     int
	last       bool
	err        error
	page       *page
	query      *qsearch.Query
	store      search.Store
	pageReader *pageReader
	collection *schema.DefaultCollection
}

func NewSearchReader(ctx context.Context, store search.Store, coll *schema.DefaultCollection, query *qsearch.Query) (*SearchRowReader, error) {
	return &SearchRowReader{
		pageNo:     1,
		query:      query,
		store:      store,
		collection: coll,
		pageReader: newPageReader(store, coll, query),
	}, nil
}

func (s *SearchRowReader) Next(ctx context.Context, row *Row) bool {
	if s.err != nil {
		return false
	}

	for {
		if s.page == nil {
			if s.last, s.page, s.err = s.pageReader.next(ctx, s.pageNo); s.err != nil {
				return false
			}
		}

		if s.page == nil {
			return false
		}

		if s.page.readRow(row) {
			return true
		}

		if s.last {
			return false
		}

		s.page = nil
		s.pageNo++
	}
}

func (s *SearchRowReader) Err() error {
	return s.err
}

func (s *SearchRowReader) getFacets() map[string]*api.SearchFacet {
	return s.pageReader.cachedFacets
}
