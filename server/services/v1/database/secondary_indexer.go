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
	"strings"
	"sync"

	"github.com/buger/jsonparser"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/value"
)

var (
	StubbFieldName = "._tigris_stub"
	KVSubspace     = "kvs"
	InfoSubspace   = "_info"
	CountSubSpace  = "count"
	SizeSubSpace   = "size"
)

type IndexRow struct {
	value value.Value
	name  string
	pos   int
	stub  bool
}

func newIndexRow(dataType schema.FieldType, collation *value.Collation, name string, rawValue []byte, pos int, stub bool) (*IndexRow, error) {
	value, err := value.NewValueUsingCollation(dataType, rawValue, collation)
	if err != nil {
		return nil, err
	}
	return &IndexRow{
		value,
		name,
		pos,
		stub,
	}, nil
}

func (f IndexRow) Name() string {
	if f.stub {
		return f.name + StubbFieldName
	}
	return f.name
}

func (f IndexRow) IsEqual(b IndexRow) bool {
	compare, err := f.value.CompareTo(b.value)
	if err != nil {
		return false
	}
	return compare == 0 && f.Name() == b.Name() && f.pos == b.pos
}

type SecondaryIndexInfo struct {
	Rows int64
	Size int64
}

type IndexerUpdateSet struct {
	addKeys   []keys.Key
	addSizes  map[string]int64
	addCounts map[string]int64

	removeKeys   []keys.Key
	removeSizes  map[string]int64
	removeCounts map[string]int64
}

func genRowCountKey(coll *schema.DefaultCollection, fieldPath string) keys.Key {
	return keys.NewKey(coll.EncodedName, coll.Indexes.SecondaryIndex.Name, InfoSubspace, CountSubSpace, fieldPath)
}

func genRowSizeKey(coll *schema.DefaultCollection, fieldPath string) keys.Key {
	return keys.NewKey(coll.EncodedName, coll.Indexes.SecondaryIndex.Name, InfoSubspace, SizeSubSpace, fieldPath)
}

type SecondaryIndexer struct {
	collation *value.Collation
	coll      *schema.DefaultCollection
}

func NewSecondaryIndexer(coll *schema.DefaultCollection) *SecondaryIndexer {
	return &SecondaryIndexer{
		collation: value.NewCollationFrom(&api.Collation{Case: "csk"}),
		coll:      coll,
	}
}

// For testing only, it reads the full index.
func (q *SecondaryIndexer) scanIndex(ctx context.Context, tx transaction.Tx) (kv.Iterator, error) {
	start := keys.NewKey(q.coll.EncodedName, q.coll.Indexes.SecondaryIndex.Name, KVSubspace)
	end := keys.NewKey(q.coll.EncodedName, q.coll.Indexes.SecondaryIndex.Name, KVSubspace, 0xFF)
	return tx.ReadRange(ctx, start, end, false)
}

func (q *SecondaryIndexer) IndexInfo(ctx context.Context, tx transaction.Tx) (*SecondaryIndexInfo, error) {
	var wg sync.WaitGroup
	size := int64(0)
	rows := int64(0)
	var errSize error
	var errCount error

	wg.Add(1)
	go func() {
		defer wg.Done()
		lkeySize := keys.NewKey(q.coll.EncodedName, q.coll.Indexes.SecondaryIndex.Name, InfoSubspace, SizeSubSpace, "")
		rkeySize := keys.NewKey(q.coll.EncodedName, q.coll.Indexes.SecondaryIndex.Name, InfoSubspace, SizeSubSpace, 0xFF)
		size, errSize = q.aggregateInfo(ctx, tx, lkeySize, rkeySize)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		lkeyCount := keys.NewKey(q.coll.EncodedName, q.coll.Indexes.SecondaryIndex.Name, InfoSubspace, CountSubSpace, "")
		rkeyCount := keys.NewKey(q.coll.EncodedName, q.coll.Indexes.SecondaryIndex.Name, InfoSubspace, CountSubSpace, 0xFF)
		rows, errCount = q.aggregateInfo(ctx, tx, lkeyCount, rkeyCount)
	}()
	wg.Wait()

	if errCount != nil {
		return nil, errCount
	}
	if errSize != nil {
		return nil, errSize
	}

	return &SecondaryIndexInfo{
		rows,
		size,
	}, nil
}

func (q *SecondaryIndexer) aggregateInfo(ctx context.Context, tx transaction.Tx, lkey keys.Key, rkey keys.Key) (int64, error) {
	counter := int64(0)
	iter, err := tx.AtomicReadRange(ctx, lkey, rkey, false)
	if err != nil {
		return 0, err
	}

	var val kv.FdbBaseKeyValue[int64]
	for iter.Next(&val) {
		counter += val.Data
	}

	if iter.Err() != nil {
		return 0, iter.Err()
	}

	return counter, nil
}

func (q *SecondaryIndexer) Delete(ctx context.Context, tx transaction.Tx, td *internal.TableData, primaryKey []interface{}) error {
	return q.Update(ctx, tx, nil, td, primaryKey)
}

func (q *SecondaryIndexer) Index(ctx context.Context, tx transaction.Tx, td *internal.TableData, primaryKey []interface{}) error {
	return q.Update(ctx, tx, td, nil, primaryKey)
}

func (q *SecondaryIndexer) Update(ctx context.Context, tx transaction.Tx, newTd *internal.TableData, oldTd *internal.TableData, primaryKey []interface{}) error {
	updateSet := q.buildAddAndRemoveKVs(newTd, oldTd, primaryKey)

	// Update Row Count
	err := incAtomicMap(ctx, tx, q.coll, genRowCountKey, updateSet.addCounts)
	if err != nil {
		return err
	}
	err = decAtomicMap(ctx, tx, q.coll, genRowCountKey, updateSet.removeCounts)
	if err != nil {
		return err
	}

	// Update Row Size
	err = incAtomicMap(ctx, tx, q.coll, genRowSizeKey, updateSet.addSizes)
	if err != nil {
		return err
	}
	err = decAtomicMap(ctx, tx, q.coll, genRowSizeKey, updateSet.removeSizes)
	if err != nil {
		return err
	}

	for _, indexKey := range updateSet.removeKeys {
		err := tx.Delete(ctx, indexKey)
		if err != nil {
			return err
		}
	}

	for _, indexKey := range updateSet.addKeys {
		err := tx.Replace(ctx, indexKey, nil, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *SecondaryIndexer) buildAddAndRemoveKVs(newTableData *internal.TableData, oldTableData *internal.TableData, primaryKey []interface{}) *IndexerUpdateSet {
	newRows := q.buildTableRows(newTableData)
	oldRows := q.buildTableRows(oldTableData)

	// Remove any rows in the oldRow set that will be overwritten in the newRows
	// We do not want to issue an unneeded delete when it will be overwritten with the update
	rowsToRemove := removeDuplicateRows(newRows, oldRows)
	rowsToAdd := removeDuplicateRows(oldRows, newRows)

	addKeys, addSizes, addCounts := q.createKeysAndIndexInfo(primaryKey, rowsToAdd)
	removeKeys, removeSizes, removeCounts := q.createKeysAndIndexInfo(primaryKey, rowsToRemove)

	mergeDuplicates(addSizes, removeSizes)
	mergeDuplicates(addCounts, removeCounts)

	return &IndexerUpdateSet{
		addKeys,
		addSizes,
		addCounts,
		removeKeys,
		removeSizes,
		removeCounts,
	}
}

func (q *SecondaryIndexer) buildTableRows(tableData *internal.TableData) []IndexRow {
	if tableData == nil {
		return []IndexRow{}
	}
	rows := q.buildTSRows(tableData)
	for _, field := range q.coll.QueryableFields {
		if schema.IsReservedField(field.Name()) {
			continue
		}

		if field.DataType == schema.ArrayType {
			rows = append(rows, q.indexArray(tableData.RawData, field, field.KeyPath())...)
		} else {
			row, err := q.indexField(tableData.RawData, field.FieldName, field.DataType, 0, field.KeyPath()...)
			if err != nil {
				log.Err(err).Msgf("Failed to index field name: %s", field.FieldName)
				continue
			}
			rows = append(rows, *row)
		}
	}
	return rows
}

func (q *SecondaryIndexer) buildTSRows(tableData *internal.TableData) []IndexRow {
	var rows []IndexRow

	timeStamps := []struct {
		ts    *internal.Timestamp
		field schema.ReservedField
	}{
		{
			tableData.CreatedAt,
			schema.CreatedAt,
		},
		{
			tableData.UpdatedAt,
			schema.UpdatedAt,
		},
	}

	for _, ts := range timeStamps {
		val := []byte{}
		dt := schema.NullType
		if ts.ts != nil {
			val = []byte(ts.ts.ToRFC3339())
			dt = schema.DateTimeType
		}

		row, err := newIndexRow(dt, q.collation, schema.ReservedFields[ts.field], val, 0, false)
		if err == nil {
			rows = append(rows, *row)
		} else {
			log.Err(err).Msgf("Failed to index %s field", schema.ReservedFields[ts.field])
		}
	}
	return rows
}

func (q *SecondaryIndexer) buildIndexKey(row IndexRow, primaryKey []interface{}) keys.Key {
	version := getFieldVersion(row.name, q.coll)
	return newKeyWithPrimaryKey(primaryKey, q.coll.EncodedName, q.coll.Indexes.SecondaryIndex.Name, KVSubspace, row.Name(), version, row.value.AsInterface(), row.pos)
}

func (q *SecondaryIndexer) createKeysAndIndexInfo(primaryKey []interface{}, rows []IndexRow) ([]keys.Key, map[string]int64, map[string]int64) {
	indexKeys := make([]keys.Key, 0, len(rows))
	sizeIncrease := int64(0)
	stubs := map[string]bool{}
	rowCounts := map[string]int64{}
	rowSizes := map[string]int64{}
	for _, row := range rows {
		// Only add one stub per repeated nested field to indicate this document contains the nested field
		if row.stub && stubs[row.Name()] {
			continue
		} else {
			stubs[row.Name()] = true
		}

		indexKey := q.buildIndexKey(row, primaryKey)
		indexKeys = append(indexKeys, indexKey)

		if val, ok := rowCounts[row.Name()]; ok {
			rowCounts[row.Name()] = val + 1
		} else {
			rowCounts[row.Name()] = 1
		}

		sizeIncrease += int64(len(indexKey.SerializeToBytes()))
		if val, ok := rowSizes[row.name]; ok {
			rowSizes[row.Name()] = val + sizeIncrease
		} else {
			rowSizes[row.Name()] = sizeIncrease
		}
	}
	return indexKeys, rowSizes, rowCounts
}

func (q *SecondaryIndexer) indexField(doc []byte, fieldName string, dataType schema.FieldType, pos int, keyPath ...string) (*IndexRow, error) {
	if dataType == schema.ByteType {
		return nil, fmt.Errorf("do not index byte field %s", fieldName)
	}
	val, dt, _, err := jsonparser.Get(doc, keyPath...)
	if dt == jsonparser.NotExist {
		return nil, err
	}

	row, err := newIndexRow(dataType, q.collation, fieldName, val, pos, false)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (q *SecondaryIndexer) indexNestedField(doc []byte, topField string, pos int) []IndexRow {
	var indexedFields []IndexRow
	processor := func(key []byte, value []byte, dt jsonparser.ValueType, offset int) error {
		fieldType := schema.ToFieldType(dt.String(), "", "")
		fieldName := topField + "." + string(key)

		switch fieldType {
		case schema.ArrayType:
			// Create a stub for a nested array
			row, _ := newIndexRow(fieldType, q.collation, fieldName, nil, pos, true)
			indexedFields = append(indexedFields, *row)
		case schema.ObjectType:
			q.indexNestedField(value, fieldName, pos)
		default:
			row, err := newIndexRow(fieldType, q.collation, fieldName, value, pos, false)
			if err != nil {
				log.Err(err).Msgf("Failed to index field name: %s", fieldName)
				return err
			}
			indexedFields = append(indexedFields, *row)
		}

		return nil
	}
	err := jsonparser.ObjectEach(doc, processor)
	if err != nil {
		log.Err(err).Msgf("Failed to index field name: %s", topField)
	}
	return indexedFields
}

func (q *SecondaryIndexer) indexArray(doc []byte, field *schema.QueryableField, keyPath []string) []IndexRow {
	pos := 0
	var rows []IndexRow
	processor := func(value []byte, dt jsonparser.ValueType, offset int, err error) {
		toFieldType := schema.ToFieldType(dt.String(), "", "")
		switch toFieldType {
		case schema.ObjectType:
			indexedFields := q.indexNestedField(value, field.FieldName, pos)
			rows = append(rows, indexedFields...)
		case schema.ArrayType:
			indexedField, _ := newIndexRow(toFieldType, q.collation, field.FieldName, nil, pos, true)
			rows = append(rows, *indexedField)
		default:
			indexedField, err := newIndexRow(field.SubType, q.collation, field.FieldName, value, pos, false)
			if err != nil {
				log.Err(err).Msgf("Failed to index field name: %s", field.FieldName)
				return
			}
			rows = append(rows, *indexedField)
		}
		pos += 1
	}

	_, err := jsonparser.ArrayEach(doc, processor, keyPath...)
	if err != nil {
		log.Err(err).Msgf("Failed to index field name: %s", field.FieldName)
	}
	return rows
}

// This is used to append the Primary key to the end of the key.
func newKeyWithPrimaryKey(id []interface{}, table []byte, indexParts ...interface{}) keys.Key {
	indexParts = append(indexParts, id...)
	return keys.NewKey(table, indexParts...)
}

func getFieldVersion(fieldName string, coll *schema.DefaultCollection) int {
	fieldVersions := coll.LookupFieldVersion(strings.Split(fieldName, (".")))
	if len(fieldVersions) > 0 {
		return fieldVersions[len(fieldVersions)-1].Version
	}
	return 1
}

func containsIndexRow(rows []IndexRow, field IndexRow) bool {
	for _, row := range rows {
		if row.IsEqual(field) {
			return true
		}
	}
	return false
}

func mergeDuplicates(add map[string]int64, remove map[string]int64) {
	for key, val := range add {
		if removeVal, ok := remove[key]; ok {
			add[key] = val - removeVal
			delete(remove, key)
		}
	}
}

// Removes fields in `rows` that is also in `removes`.
func removeDuplicateRows(rows []IndexRow, removes []IndexRow) []IndexRow {
	var final []IndexRow
	for _, removeRow := range removes {
		if !containsIndexRow(rows, removeRow) {
			final = append(final, removeRow)
		}
	}
	return final
}

func incAtomicMap(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, keyGen func(*schema.DefaultCollection, string) keys.Key, fields map[string]int64) error {
	return updateAtomicMap(ctx, tx, coll, keyGen, fields, true)
}

func decAtomicMap(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, keyGen func(*schema.DefaultCollection, string) keys.Key, fields map[string]int64) error {
	return updateAtomicMap(ctx, tx, coll, keyGen, fields, false)
}

func updateAtomicMap(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, keyGen func(*schema.DefaultCollection, string) keys.Key, fields map[string]int64, inc bool) error {
	for fieldName, value := range fields {
		// skip unnecessary updates
		if value == 0 {
			continue
		}
		key := keyGen(coll, fieldName)
		if !inc {
			value = -value
		}
		err := tx.AtomicAdd(ctx, key, value)
		if err != nil {
			return err
		}
	}
	return nil
}