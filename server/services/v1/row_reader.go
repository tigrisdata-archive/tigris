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
	"encoding/json"

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

const (
	perPage = 10
)

type Row struct {
	Key  []byte
	Data *internal.TableData
}

type RowReader interface {
	NextRow(context.Context, *Row) bool
	Err() error
}

type page struct {
	idx        int
	err        error
	resp       *pageResponse
	wrappedF   *filter.WrappedFilter
	collection *schema.DefaultCollection
}

func (p *page) readRow(row *Row) bool {
	if p.err != nil {
		return false
	}

	for p.resp.hits.HasMoreHits(p.idx) {
		document, _ := p.resp.hits.GetDocument(p.idx)
		p.idx++
		if document == nil {
			continue
		}

		if p.err = UnpackSearchFields(document, p.collection); p.err != nil {
			return false
		}

		// now apply the filter
		if !p.wrappedF.Filter.MatchesDoc(document) {
			continue
		}

		data, err := json.Marshal(*document)
		if err != nil {
			p.err = err
			return false
		}

		row.Key = []byte((*document)[searchID].(string))
		row.Data = &internal.TableData{RawData: data}
		return true
	}

	return false
}

type SearchRowReader struct {
	pageNo     int
	page       *page
	err        error
	lastPage   bool
	tsFilter   string
	store      search.Store
	response   *SearchResponse
	wrappedF   *filter.WrappedFilter
	collection *schema.DefaultCollection
}

func MakeSearchRowReader(ctx context.Context, c *schema.DefaultCollection, _ []read.Field, w *filter.WrappedFilter, s search.Store) (*SearchRowReader, error) {
	builder := qsearch.NewBuilder()

	return &SearchRowReader{
		pageNo:     1,
		store:      s,
		wrappedF:   w,
		collection: c,
		tsFilter:   builder.FromFilter(w),
	}, nil
}

func MakeSearchRowReaderUsingFilter(ctx context.Context, c *schema.DefaultCollection, w *filter.WrappedFilter, s search.Store) (*SearchRowReader, error) {
	return MakeSearchRowReader(ctx, c, nil, w, s)
}

func (s *SearchRowReader) readPage(ctx context.Context) (bool, error) {
	result, err := s.store.Search(ctx, s.collection.SearchCollectionName(), s.tsFilter, s.pageNo, perPage)
	if err != nil {
		return false, err
	}

	var hitsResp = NewHitsResponse()
	for _, r := range result {
		hitsResp.Append(r.Hits)
	}

	s.page = &page{
		idx:        0,
		collection: s.collection,
		resp: &pageResponse{
			hits:   hitsResp,
			facets: CreateFacetResponse(result[0].FacetCounts),
		},
		wrappedF: s.wrappedF,
	}

	return hitsResp.Count() < perPage, nil
}

func (s *SearchRowReader) NextRow(ctx context.Context, row *Row) bool {
	if s.err != nil {
		return false
	}

	for {
		if s.page == nil {
			if s.lastPage, s.err = s.readPage(ctx); s.err != nil {
				return false
			}
		}

		if s.page.readRow(row) {
			return true
		}

		if s.lastPage {
			return false
		}

		s.page = nil
		s.pageNo++
	}
}

func (s *SearchRowReader) Err() error {
	return s.err
}

type DatabaseRowReader struct {
	idx        int
	err        error
	keys       []keys.Key
	tx         transaction.Tx
	ctx        context.Context
	kvIterator kv.Iterator
}

func MakeDatabaseRowReader(ctx context.Context, tx transaction.Tx, keys []keys.Key) (*DatabaseRowReader, error) {
	d := &DatabaseRowReader{
		idx:  0,
		tx:   tx,
		ctx:  ctx,
		keys: keys,
	}
	if d.kvIterator, d.err = d.readNextKey(d.ctx, d.keys[d.idx]); d.err != nil {
		return nil, d.err
	}

	return d, nil
}

func (d *DatabaseRowReader) NextRow(_ context.Context, row *Row) bool {
	if d.err != nil {
		return false
	}

	for {
		var keyValue kv.KeyValue
		if d.kvIterator.Next(&keyValue) {
			row.Key = keyValue.FDBKey
			row.Data = keyValue.Data
			return true
		}
		if d.kvIterator.Err() != nil {
			d.err = d.kvIterator.Err()
			return false
		}

		d.idx++
		if d.idx == len(d.keys) {
			return false
		}

		d.kvIterator, d.err = d.readNextKey(d.ctx, d.keys[d.idx])
	}
}

func (d *DatabaseRowReader) readNextKey(ctx context.Context, key keys.Key) (kv.Iterator, error) {
	it, err := d.tx.Read(ctx, key)
	if ulog.E(err) {
		return nil, err
	}
	return it, nil

}

func (d *DatabaseRowReader) Err() error { return d.err }
