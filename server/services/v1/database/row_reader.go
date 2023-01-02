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

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type Row struct {
	Key  []byte
	Data *internal.TableData
}

// Iterator is to iterate over a single collection.
type Iterator interface {
	// Next fills the next element in the iteration. Returns true if the Iterator has more element.
	Next(*Row) bool
	// Interrupted returns an error if iterator encounters any error.
	Interrupted() error
}

type ScanIterator struct {
	it  kv.Iterator
	err error
}

func NewScanIterator(ctx context.Context, tx transaction.Tx, from keys.Key) (*ScanIterator, error) {
	it, err := tx.ReadRange(ctx, from, nil, false)
	if ulog.E(err) {
		return nil, err
	}

	return &ScanIterator{
		it: it,
	}, nil
}

func (s *ScanIterator) Next(row *Row) bool {
	if s.err != nil {
		return false
	}

	var keyValue kv.KeyValue
	if s.it.Next(&keyValue) {
		row.Key = keyValue.FDBKey
		row.Data = keyValue.Data
		return true
	}
	s.err = s.it.Err()

	return false
}

func (s *ScanIterator) Interrupted() error { return s.err }

type KeyIterator struct {
	it    kv.Iterator
	tx    transaction.Tx
	ctx   context.Context
	keys  []keys.Key
	err   error
	keyId int
}

func NewKeyIterator(ctx context.Context, tx transaction.Tx, keys []keys.Key) (*KeyIterator, error) {
	keyId := 0
	it, err := tx.Read(ctx, keys[keyId])
	if ulog.E(err) {
		return nil, err
	}

	return &KeyIterator{
		tx:    tx,
		it:    it,
		ctx:   ctx,
		keys:  keys,
		keyId: keyId,
	}, nil
}

func (k *KeyIterator) Next(row *Row) bool {
	if k.err != nil {
		return false
	}

	for {
		var keyValue kv.KeyValue
		if k.it.Next(&keyValue) {
			row.Key = keyValue.FDBKey
			row.Data = keyValue.Data
			return true
		}

		if k.err = k.it.Err(); k.err != nil {
			return false
		}

		k.keyId++
		if k.keyId == len(k.keys) {
			return false
		}

		k.it, k.err = k.tx.Read(k.ctx, k.keys[k.keyId])
	}
}

func (k *KeyIterator) Interrupted() error { return k.err }

// FilterIterator only returns elements that match the given predicate.
type FilterIterator struct {
	iterator Iterator
	filter   *filter.WrappedFilter
}

func NewFilterIterator(iterator Iterator, filter *filter.WrappedFilter) *FilterIterator {
	return &FilterIterator{
		iterator: iterator,
		filter:   filter,
	}
}

func (it *FilterIterator) Interrupted() error {
	return it.iterator.Interrupted()
}

// Next advances the iterator till the matching row found and then only fill the row object. In contrast
// to Iterator, filterable allows filtering during iterating of document. Underneath it is just using Iterator
// to iterate over rows to apply filter.
func (it *FilterIterator) Next(row *Row) bool {
	for {
		if !it.iterator.Next(row) {
			return false
		}

		if it.advanceToMatchingRow(row) {
			return true
		}
	}
}

func (it *FilterIterator) advanceToMatchingRow(row *Row) bool {
	return it.filter.Matches(row.Data.RawData)
}

type DatabaseReader struct {
	tx  transaction.Tx
	ctx context.Context
}

func NewDatabaseReader(ctx context.Context, tx transaction.Tx) *DatabaseReader {
	return &DatabaseReader{
		ctx: ctx,
		tx:  tx,
	}
}

// ScanTable returns an iterator for all the rows in this table.
func (reader *DatabaseReader) ScanTable(table []byte) (Iterator, error) {
	return NewKeyIterator(reader.ctx, reader.tx, []keys.Key{keys.NewKey(table)})
}

// ScanIterator only returns an iterator that has elements starting from.
func (reader *DatabaseReader) ScanIterator(from keys.Key) (Iterator, error) {
	return NewScanIterator(reader.ctx, reader.tx, from)
}

// StrictlyKeysFrom is an optimized version that takes input keys and filter out keys that are lower than the "from".
func (reader *DatabaseReader) StrictlyKeysFrom(ikeys []keys.Key, from []byte) (Iterator, error) {
	// this means we have returned data to the user, and now we need to stream again but only from the last offset
	// therefore we need to prune keys that are not needed, i.e. lower by this offset
	var toReadKeys []keys.Key
	for i, k := range ikeys {
		if k.CompareBytes(from) > 0 {
			toReadKeys = append(toReadKeys, ikeys[i])
		}
	}

	return reader.KeyIterator(toReadKeys)
}

// KeyIterator returns an iterator that iterates on a key or a set of keys.
func (reader *DatabaseReader) KeyIterator(ikeys []keys.Key) (Iterator, error) {
	return NewKeyIterator(reader.ctx, reader.tx, ikeys)
}

// FilteredRead returns an iterator that implicitly will be doing filtering on the iterator.
func (reader *DatabaseReader) FilteredRead(iterator Iterator, filter *filter.WrappedFilter) (Iterator, error) {
	return NewFilterIterator(iterator, filter), nil
}
