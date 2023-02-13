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
	"fmt"

	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

type SecondaryIndexReader struct {
	ctx      context.Context
	coll     *schema.DefaultCollection
	filter   *filter.WrappedFilter
	tx       transaction.Tx
	err      error
	keyRange []keys.Key
	kvIter   kv.Iterator
}

func NewSecondaryIndexReader(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, filter *filter.WrappedFilter, keyRange []keys.Key) *SecondaryIndexReader {
	return &SecondaryIndexReader{
		ctx:      ctx,
		tx:       tx,
		coll:     coll,
		filter:   filter,
		err:      nil,
		keyRange: keyRange,
	}
}
func (reader *SecondaryIndexReader) CreateIter() (*SecondaryIndexReader, error) {
	var err error
	switch len(reader.keyRange) {
	case 0:
		return nil, errors.InvalidArgument("Index Key range cannot be zero, we need a minimum of one key")
	case 1:
		reader.kvIter, err = reader.tx.Read(reader.ctx, reader.keyRange[0])
		if err != nil {
			return nil, err
		}
	case 2:
		reader.kvIter, err = reader.tx.ReadRange(reader.ctx, reader.keyRange[0], reader.keyRange[1], false)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.InvalidArgument("Incorrectly created query key range")
	}

	return reader, nil
}

func BuildSecondaryIndexKeys(coll *schema.DefaultCollection, queryFilters []filter.Filter) ([]keys.Key, error) {

	encoder := func(indexParts ...interface{}) (keys.Key, error) {
		return newKeyWithPrimaryKey(indexParts, coll.EncodedName, coll.Indexes.SecondaryIndex.Name, "kvs"), nil
	}

	buildIndexParts := func(fieldName string, datatype schema.FieldType, value interface{}) []interface{} {
		version := getFieldVersion(fieldName, coll)
		return []interface{}{fieldName, version, value}
	}

	eqKeyBuilder := filter.NewKeyBuilder(filter.NewStrictEqKeyComposer(encoder, buildIndexParts))

	for _, field := range coll.QueryableFields {
		eqKeys, err := eqKeyBuilder.Build(queryFilters, []*schema.QueryableField{field})
		if err != nil || len(eqKeys) == 0 {
			continue
		}

		return eqKeys[0].Keys, nil
	}

	rangKeyBuilder := filter.NewKeyBuilder(filter.NewRangeKeyComposer(encoder, buildIndexParts))

	rangeKeys, err := rangKeyBuilder.Build(queryFilters, coll.QueryableFields)
	if err != nil {
		return nil, err
	}

	if len(rangeKeys) == 0 {
		return nil, errors.InvalidArgument("Could not find a query range")
	}

	fmt.Println("KEYS FOR QUERY", rangeKeys[0].Keys[0], rangeKeys[0].Keys[1])

	return rangeKeys[0].Keys, nil
}

func (it SecondaryIndexReader) Next(row *Row) bool {
	if it.kvIter.Err() != nil {
		it.err = it.kvIter.Err()
		return false
	}

	if it.err != nil {
		return false
	}

	var kvRow kv.KeyValue
	if it.kvIter.Next(&kvRow) {
		encodedIdxName := kvRow.Key[len(kvRow.Key)-2]
		primaryKey := kvRow.Key[len(kvRow.Key)-1]
		pkIndexParts := keys.NewKey(it.coll.EncodedName, encodedIdxName, primaryKey)

		getFut, err := it.tx.Get(it.ctx, pkIndexParts.SerializeToBytes(), false)
		if err != nil {
			it.err = err
			return false
		}

		raw, err := getFut.Get()
		if err != nil {
			it.err = err
			return false
		}

		decoded, err := internal.Decode(raw)
		if err != nil {
			it.err = err
			return false
		}

		fmt.Println("Fetched Doc", decoded)
		row.Data = decoded
		row.Key = pkIndexParts.SerializeToBytes()

		return true
	}

	return false

}

func (it SecondaryIndexReader) Interrupted() error { return it.err }

type IndexIterator struct {
	tx   transaction.Tx
	it   kv.Iterator
	ctx  context.Context
	keys []keys.Key
	err  error
}

func (iter *IndexIterator) Next(row *kv.KeyValue) bool {
	if iter.err != nil {
		return false
	}

	if iter.it.Next(row) {
		return true
	}

	return false
}

func (it *IndexIterator) Interrupted() error { return it.err }
