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

package database

import (
	"context"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/query/filter"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/schema"
	tsearch "github.com/tigrisdata/tigris/server/search"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
)

const (
	defaultPerPage = 20
	defaultPageNo  = 1
)

type page struct {
	idx  int
	cap  int
	hits []*tsearch.Hit
}

func newPage(c int) *page {
	return &page{
		idx:  0,
		cap:  c,
		hits: []*tsearch.Hit{},
	}
}

func (p *page) append(h *tsearch.Hit) bool {
	if !p.hasCapacity() {
		return false
	}

	p.hits = append(p.hits, h)
	return p.hasCapacity()
}

func (p *page) hasCapacity() bool {
	return len(p.hits) < p.cap
}

// readRow should be used to read search data because this is the single point where we unpack search fields, apply
// filter and then pack the document into bytes.
func (p *page) readRow() map[string]interface{} {
	for p.idx < len(p.hits) {
		document := p.hits[p.idx].Document
		p.idx++
		if document != nil {
			return document
		}
	}

	return nil
}

type pageReader struct {
	ctx          context.Context
	pageNo       int
	found        int64
	pages        []*page
	query        *qsearch.Query
	store        search.Store
	searchIndex  *schema.ImplicitSearchIndex
	cachedFacets map[string]*api.SearchFacet
}

func newPageReader(ctx context.Context, store search.Store, coll *schema.DefaultCollection, query *qsearch.Query, firstPage int32) *pageReader {
	return &pageReader{
		ctx:          ctx,
		found:        -1,
		query:        query,
		store:        store,
		pageNo:       int(firstPage),
		searchIndex:  coll.GetImplicitSearchIndex(),
		cachedFacets: make(map[string]*api.SearchFacet),
	}
}

func (p *pageReader) read() error {
	result, err := p.store.Search(p.ctx, p.searchIndex.StoreIndexName(), p.query, p.pageNo)
	if err != nil {
		return err
	}

	hits := tsearch.NewResponseFactory(p.query).GetHitsIterator(result)
	sortedFacets := tsearch.NewSortedFacets()
	for _, r := range result {
		if r.FacetCounts != nil {
			for i := range *r.FacetCounts {
				if ulog.E(sortedFacets.Add(&(*r.FacetCounts)[i])) {
					continue
				}
			}
		}
	}

	p.pageNo++
	pg := newPage(p.query.PageSize)

	for hits.HasMoreHits() {
		hit, err := hits.Next()
		// log and skip to next hit
		if ulog.E(err) {
			continue
		}
		if !pg.append(hit) {
			p.pages = append(p.pages, pg)
			pg = newPage(p.query.PageSize)
		}
	}

	// include the last page in results if it has hits
	if len(pg.hits) > 0 {
		p.pages = append(p.pages, pg)
	}

	// check if we need to build facets
	if len(p.cachedFacets) == 0 {
		p.buildFacets(sortedFacets)
	}

	if p.found == -1 {
		p.found = 0
		for _, r := range result {
			if r.Found != nil {
				p.found += int64(*r.Found)
			}
		}
	}
	return nil
}

func (p *pageReader) next() (bool, *page, error) {
	if len(p.pages) == 0 {
		if err := p.read(); err != nil {
			return false, nil, err
		}
	}

	if len(p.pages) == 0 {
		return true, nil, nil
	}

	pg, _ := p.pages[0], p.pages[0].hasCapacity()
	p.pages = p.pages[1:]
	return false, pg, nil
}

func (p *pageReader) buildFacets(sf *tsearch.SortedFacets) {
	facetSizeRequested := map[string]int{}
	for _, f := range p.query.Facets.Fields {
		facetSizeRequested[f.Name] = f.Size

		facet := &api.SearchFacet{
			Stats:  sf.GetStats(f.Name),
			Counts: []*api.FacetCount{},
		}

		for i := 0; i < f.Size; i++ {
			if fc, ok := sf.GetFacetCount(f.Name); ok {
				facet.Counts = append(facet.Counts, &api.FacetCount{
					Count: fc.Count,
					Value: fc.Value,
				})
			}
		}

		p.cachedFacets[f.Name] = facet
	}
}

type FilterableSearchIterator struct {
	err        error
	single     bool
	last       bool
	page       *page
	filter     *filter.WrappedFilter
	pageReader *pageReader
	collection *schema.DefaultCollection
	readSize   int
}

func NewFilterableSearchIterator(collection *schema.DefaultCollection, reader *pageReader, filter *filter.WrappedFilter, singlePage bool) *FilterableSearchIterator {
	return &FilterableSearchIterator{
		single:     singlePage,
		pageReader: reader,
		filter:     filter,
		collection: collection,
	}
}

func (it *FilterableSearchIterator) Next(row *Row) bool {
	if it.err != nil {
		return false
	}

	for {
		if it.page == nil {
			if it.last, it.page, it.err = it.pageReader.next(); it.err != nil || it.page == nil {
				return false
			}
		}

		if doc := it.page.readRow(); doc != nil {
			var searchKey string
			if searchKey, row.Data, doc, it.err = UnpackSearchFields(doc, it.collection); it.err != nil {
				return false
			}
			row.Key = []byte(searchKey)

			// now apply the filter
			if !it.filter.MatchesDoc(doc) {
				continue
			}

			var rawData []byte
			// marshal the doc as bytes
			if rawData, it.err = util.MapToJSON(doc); it.err != nil {
				return false
			}
			row.Data.RawData = rawData
			it.readSize += len(rawData)
			return true
		}

		if it.last || it.single {
			return false
		}

		it.page = nil
	}
}

func (it *FilterableSearchIterator) getFacets() map[string]*api.SearchFacet {
	return it.pageReader.cachedFacets
}

func (it *FilterableSearchIterator) Interrupted() error {
	return it.err
}

func (it *FilterableSearchIterator) ReadSize() int {
	return it.readSize
}

func (it *FilterableSearchIterator) getTotalFound() int64 {
	return it.pageReader.found
}

// SearchReader is responsible for iterating on the search results. It uses pageReader internally to read page
// and then iterate on documents inside hits.
type SearchReader struct {
	ctx        context.Context
	query      *qsearch.Query
	store      search.Store
	collection *schema.DefaultCollection
}

func NewSearchReader(ctx context.Context, store search.Store, coll *schema.DefaultCollection, query *qsearch.Query) *SearchReader {
	return &SearchReader{
		ctx:        ctx,
		store:      store,
		query:      query,
		collection: coll,
	}
}

func (reader *SearchReader) SinglePageIterator(collection *schema.DefaultCollection, filter *filter.WrappedFilter, pageNo int32) *FilterableSearchIterator {
	pageReader := newPageReader(reader.ctx, reader.store, reader.collection, reader.query, pageNo)

	return NewFilterableSearchIterator(collection, pageReader, filter, true)
}

func (reader *SearchReader) Iterator(collection *schema.DefaultCollection, filter *filter.WrappedFilter) *FilterableSearchIterator {
	pageReader := newPageReader(reader.ctx, reader.store, reader.collection, reader.query, defaultPageNo)

	return NewFilterableSearchIterator(collection, pageReader, filter, false)
}
