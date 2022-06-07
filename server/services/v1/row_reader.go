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
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type Row struct {
	Key  []byte
	Data *internal.TableData
}

type RowReader interface {
	NextRow(row *Row) bool
	Err() error
}

type SearchRowReader struct {
	idx    int
	err    error
	result *SearchResponse
}

func MakeSearchRowReader(ctx context.Context, table string, _ []read.Field, filters []filter.Filter, store search.Store) (*SearchRowReader, error) {
	builder := qsearch.NewBuilder()
	searchFilter := builder.FromFilter(filters)

	result, err := store.Search(ctx, table, searchFilter)
	if err != nil {
		return nil, err
	}

	var hitsResp = NewHitsResponse()
	for _, r := range result {
		hitsResp.Append(r.Hits)
	}

	var s = &SearchResponse{
		Hits:   hitsResp,
		Facets: CreateFacetResponse(result[0].FacetCounts),
	}

	return &SearchRowReader{
		idx:    0,
		result: s,
	}, nil
}

func MakeSearchRowReaderUsingFilter(ctx context.Context, table string, filters []filter.Filter, store search.Store) (*SearchRowReader, error) {
	return MakeSearchRowReader(ctx, table, nil, filters, store)
}

func (s *SearchRowReader) NextRow(row *Row) bool {
	for {
		document, more := s.result.Hits.GetDocument(s.idx)
		if !more {
			break
		}

		if document == nil {
			s.idx++
			continue
		}

		data, err := json.Marshal(*document)
		if err != nil {
			s.err = err
			return false
		}
		row.Key = []byte((*document)[searchID].(string))
		row.Data = &internal.TableData{
			RawData: data,
		}
		break
	}
	hasNext := s.result.Hits.HasMoreHits(s.idx)
	s.idx++

	return hasNext
}

func (s *SearchRowReader) GetFacet() *FacetResponse {
	return s.result.Facets
}

func (s *SearchRowReader) Err() error {
	return s.err
}

type DatabaseRowReader struct {
	idx        int
	tx         transaction.Tx
	ctx        context.Context
	err        error
	keys       []keys.Key
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

func (d *DatabaseRowReader) NextRow(row *Row) bool {
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
