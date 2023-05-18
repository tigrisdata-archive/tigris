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
	"github.com/tigrisdata/tigris/server/metrics"

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
func (p *page) readRow() map[string]any {
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
		if len(result) > 0 {
			builder := tsearch.NewFacetResponse(p.query.Facets)
			for field, built := range builder.Build(result[0].FacetCounts) {
				p.cachedFacets[field] = built
			}
		}
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

	pg, hasCapacity := p.pages[0], p.pages[0].hasCapacity()
	p.pages = p.pages[1:]
	return hasCapacity, pg, nil
}

type FilterableSearchIterator struct {
	err        error
	single     bool
	last       bool
	page       *page
	filter     *filter.WrappedFilter
	pageReader *pageReader
	collection *schema.DefaultCollection
	ctx        context.Context
}

func NewFilterableSearchIterator(ctx context.Context, collection *schema.DefaultCollection, reader *pageReader, filter *filter.WrappedFilter, singlePage bool) *FilterableSearchIterator {
	return &FilterableSearchIterator{
		ctx:        ctx,
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

			var rawData []byte
			// marshal the doc as bytes
			if rawData, it.err = util.MapToJSON(doc); it.err != nil {
				return false
			}
			tsJSON, err := row.Data.TimeStampsToJSON()
			if err != nil {
				return false
			}
			// now apply the filter

			reqStatus, exists := metrics.RequestStatusFromContext(it.ctx)
			if reqStatus != nil && exists && reqStatus.IsCollectionRead() {
				reqStatus.AddReadBytes(int64(len(rawData)))
			}

			if !it.filter.Matches(rawData, tsJSON) {
				continue
			}

			row.Data.RawData = rawData

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

func (reader *SearchReader) SinglePageIterator(ctx context.Context, collection *schema.DefaultCollection, filter *filter.WrappedFilter, pageNo int32) *FilterableSearchIterator {
	pageReader := newPageReader(reader.ctx, reader.store, reader.collection, reader.query, pageNo)

	return NewFilterableSearchIterator(ctx, collection, pageReader, filter, true)
}

func (reader *SearchReader) Iterator(ctx context.Context, collection *schema.DefaultCollection, filter *filter.WrappedFilter) *FilterableSearchIterator {
	pageReader := newPageReader(reader.ctx, reader.store, reader.collection, reader.query, defaultPageNo)

	return NewFilterableSearchIterator(ctx, collection, pageReader, filter, false)
}
