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

	"github.com/buger/jsonparser"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/value"
)

var (
	StubFieldName = "._tigris_array_stub"
	KVSubspace    = "kvs"
	InfoSubspace  = "_info"
	CountSubSpace = "count"
	SizeSubSpace  = "size"
)

type IndexRow struct {
	value    value.Value
	name     string
	pos      int
	stub     bool
	dataType schema.FieldType
	null     bool
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
		dataType,
		false,
	}, nil
}

func newMissingRow(name string) *IndexRow {
	return &IndexRow{
		value:    value.NewNullValue(),
		name:     name,
		pos:      0,
		dataType: schema.NullType,
		stub:     false,
		null:     true,
	}
}

func newNullRow(name string, pos int) *IndexRow {
	return &IndexRow{
		value:    value.NewNullValue(),
		name:     name,
		pos:      pos,
		dataType: schema.NullType,
		stub:     false,
		null:     true,
	}
}

func (f IndexRow) Name() string {
	if f.stub {
		return f.name + StubFieldName
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

type SecondaryIndexer struct {
	collation *value.Collation
	coll      *schema.DefaultCollection
	// Used for unit tests because it can index deeper into a schema than we currently expose to the user
	// This can be removed once indexing into arrays and objects is exposed to the user
	indexAll bool
	// Spare indexes do not index missing fields
	sparse bool
}

func NewSecondaryIndexer(coll *schema.DefaultCollection) *SecondaryIndexer {
	return &SecondaryIndexer{
		collation: value.NewCollationFrom(&api.Collation{Case: "csk"}),
		coll:      coll,
		indexAll:  false,
		sparse:    false, // For now indexes are only non-sparse
	}
}

func (q *SecondaryIndexer) scanIndex(ctx context.Context, tx transaction.Tx) (kv.Iterator, error) {
	start := keys.NewKey(q.coll.EncodedTableIndexName, q.coll.SecondaryIndexKeyword(), KVSubspace)
	end := keys.NewKey(q.coll.EncodedTableIndexName, q.coll.SecondaryIndexKeyword(), KVSubspace, 0xFF)
	return tx.ReadRange(ctx, start, end, false)
}

func (q *SecondaryIndexer) IndexSize(ctx context.Context, tx transaction.Tx) (int64, error) {
	lKey := keys.NewKey(q.coll.EncodedTableIndexName, q.coll.SecondaryIndexKeyword(), KVSubspace)
	rKey := keys.NewKey(q.coll.EncodedTableIndexName, q.coll.SecondaryIndexKeyword(), KVSubspace, 0xFF)
	return tx.RangeSize(ctx, q.coll.EncodedTableIndexName, lKey, rKey)
}

// The count of the number of rows in the index is not efficient
// it will read through the whole index and count the number of rows.
// The size of the index is an estimate and will need at least 100 rows before it will start returning
// a number for the size.
func (q *SecondaryIndexer) IndexInfo(ctx context.Context, tx transaction.Tx) (*SecondaryIndexInfo, error) {
	size := int64(0)
	rows := int64(0)

	size, err := q.IndexSize(ctx, tx)
	if err != nil {
		return nil, err
	}

	iter, err := q.scanIndex(ctx, tx)
	if err != nil {
		return nil, err
	}

	var val kv.KeyValue
	for iter.Next(&val) {
		rows += 1
	}
	if iter.Err() != nil {
		return nil, iter.Err()
	}

	return &SecondaryIndexInfo{
		rows,
		size,
	}, nil
}

func (q *SecondaryIndexer) ReadDocAndDelete(ctx context.Context, tx transaction.Tx, key keys.Key) error {
	iter, err := tx.Read(ctx, key)
	if err != nil {
		return err
	}
	var oldDoc kv.KeyValue
	if iter.Next(&oldDoc) {
		err := q.Delete(ctx, tx, oldDoc.Data, key.IndexParts())
		if err != nil {
			return err
		}
	}

	if iter.Err() != nil {
		return iter.Err()
	}

	return nil
}

func (q *SecondaryIndexer) Delete(ctx context.Context, tx transaction.Tx, td *internal.TableData, primaryKey []interface{}) error {
	return q.Update(ctx, tx, nil, td, primaryKey)
}

func (q *SecondaryIndexer) Index(ctx context.Context, tx transaction.Tx, td *internal.TableData, primaryKey []interface{}) error {
	return q.Update(ctx, tx, td, nil, primaryKey)
}

func (q *SecondaryIndexer) Update(ctx context.Context, tx transaction.Tx, newTd *internal.TableData, oldTd *internal.TableData, primaryKey []interface{}) error {
	if len(q.coll.EncodedTableIndexName) == 0 {
		return fmt.Errorf("could not index collection %s, encoded table not set", q.coll.Name)
	}
	updateSet, err := q.buildAddAndRemoveKVs(newTd, oldTd, primaryKey)
	if err != nil {
		return err
	}

	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)

	for _, indexKey := range updateSet.removeKeys {
		if reqStatus != nil && reqStatusExists {
			if !reqStatus.IsSecondaryIndexFieldIgnored(indexKey.SerializeToBytes()) {
				reqStatus.AddWriteBytes(int64(len(indexKey.SerializeToBytes())))
			}
		}
		if err := tx.Delete(ctx, indexKey); err != nil {
			return err
		}
	}

	for _, indexKey := range updateSet.addKeys {
		if reqStatus != nil && reqStatusExists {
			if !reqStatus.IsSecondaryIndexFieldIgnored(indexKey.SerializeToBytes()) {
				reqStatus.AddWriteBytes(int64(len(indexKey.SerializeToBytes())))
			}
		}
		if err := tx.Replace(ctx, indexKey, internal.EmptyData, false); err != nil {
			return err
		}
	}
	return nil
}

// The process here:
// 1. Build key values for old and new doc
// 2. Remove keys from the old doc that are exactly the same in the new doc
// 3. Remove keys from the new doc that are exactly the same as the old doc
// 4. Create list of keys to remove and size and counts fields to be decremented
// 5. Create list of keys to add to index along with size and count fields to be decremented.
func (q *SecondaryIndexer) buildAddAndRemoveKVs(newTableData *internal.TableData, oldTableData *internal.TableData, primaryKey []interface{}) (*IndexerUpdateSet, error) {
	newRows, err := q.buildTableRows(newTableData)
	if err != nil {
		return nil, err
	}
	oldRows, err := q.buildTableRows(oldTableData)
	if err != nil {
		return nil, err
	}

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
	}, nil
}

func (q *SecondaryIndexer) buildTableRows(tableData *internal.TableData) ([]IndexRow, error) {
	if tableData == nil {
		return []IndexRow{}, nil
	}
	rows, err := q.buildTSRows(tableData)
	if err != nil {
		return nil, err
	}
	for _, field := range q.getIndexedFields() {
		if schema.IsReservedField(field.Name()) {
			continue
		}

		if field.DataType == schema.ArrayType {
			newRows, err := q.indexArray(tableData.RawData, field, field.KeyPath())
			if err != nil {
				if isIgnoreableError(err) {
					continue
				} else {
					log.Err(err).Msgf("Failed to index field name: %s", field.FieldName)
					return nil, err
				}
			}
			rows = append(rows, newRows...)
		} else {
			row, err := q.indexField(tableData.RawData, field.FieldName, field.DataType, 0, field.KeyPath()...)
			if err != nil {
				if isIgnoreableError(err) {
					continue
				} else {
					log.Err(err).Msgf("Failed to index field name: %s", field.FieldName)
					return nil, err
				}
			}
			rows = append(rows, *row)
		}
	}
	return rows, nil
}

func (q *SecondaryIndexer) buildTSRows(tableData *internal.TableData) ([]IndexRow, error) {
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
		if err != nil {
			return nil, err
		}
		if err == nil {
			rows = append(rows, *row)
		} else {
			if isIgnoreableError(err) {
				continue
			} else {
				return nil, err
			}
		}
	}
	return rows, nil
}

func (q *SecondaryIndexer) buildIndexKey(row IndexRow, primaryKey []interface{}) keys.Key {
	if row.null {
		return newKeyWithPrimaryKey(primaryKey, q.coll.EncodedTableIndexName, q.coll.SecondaryIndexKeyword(), KVSubspace, row.Name(), value.SecondaryNullOrder(), row.value.AsInterface(), row.pos)
	}

	dataTypeOrder := value.ToSecondaryOrder(row.dataType, row.value)
	return newKeyWithPrimaryKey(primaryKey, q.coll.EncodedTableIndexName, q.coll.SecondaryIndexKeyword(), KVSubspace, row.Name(), dataTypeOrder, row.value.AsInterface(), row.pos)
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
	if dt == jsonparser.NotExist && !q.sparse {
		return newMissingRow(fieldName), nil
	}

	if err != nil {
		return nil, err
	}

	if dt == jsonparser.Null {
		return newNullRow(fieldName, 0), nil
	}

	row, err := newIndexRow(dataType, q.collation, fieldName, val, pos, false)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (q *SecondaryIndexer) indexNestedField(doc []byte, topField string, pos int) ([]IndexRow, error) {
	var indexedFields []IndexRow
	processor := func(key []byte, value []byte, dt jsonparser.ValueType, offset int) error {
		fieldType := schema.ToFieldType(dt.String(), "", "")
		fieldName := topField + "." + string(key)

		switch fieldType {
		case schema.ArrayType:
			// Create a stub for a nested array
			row, err := newIndexRow(fieldType, q.collation, fieldName, nil, pos, true)
			if err != nil {
				return err
			}
			indexedFields = append(indexedFields, *row)
		case schema.ObjectType:
			// nested objects
			return nil
		case schema.NullType:
			indexedFields = append(indexedFields, *newNullRow(fieldName, pos))
		default:
			row, err := newIndexRow(fieldType, q.collation, fieldName, value, pos, false)
			if err != nil {
				return err
			}
			indexedFields = append(indexedFields, *row)
		}

		return nil
	}
	err := jsonparser.ObjectEach(doc, processor)
	if err != nil {
		if isIgnoreableError(err) {
			log.Err(err).Msgf("Failed to index field name: %s", topField)
		} else {
			return nil, err
		}
	}
	return indexedFields, nil
}

func (q *SecondaryIndexer) indexArray(doc []byte, field *schema.QueryableField, keyPath []string) ([]IndexRow, error) {
	pos := 0
	var rows []IndexRow
	var errProcessor error
	processor := func(value []byte, dt jsonparser.ValueType, offset int, err error) {
		toFieldType := schema.ToFieldType(dt.String(), "", "")
		switch toFieldType {
		case schema.NullType:
			rows = append(rows, *newNullRow(field.FieldName, pos))
		case schema.ObjectType:
			indexedFields, err := q.indexNestedField(value, field.FieldName, pos)
			if err != nil && !isIgnoreableError(err) {
				errProcessor = err
				return
			}
			rows = append(rows, indexedFields...)
		case schema.ArrayType:
			indexedField, err := newIndexRow(toFieldType, q.collation, field.FieldName, nil, pos, true)
			if err != nil && !isIgnoreableError(err) {
				errProcessor = err
				return
			}
			rows = append(rows, *indexedField)
		default:
			indexedField, err := newIndexRow(field.SubType, q.collation, field.FieldName, value, pos, false)
			if err != nil && !isIgnoreableError(err) {
				errProcessor = err
				return
			}
			rows = append(rows, *indexedField)
		}
		pos += 1
	}

	if errProcessor != nil {
		return nil, errProcessor
	}

	_, err := jsonparser.ArrayEach(doc, processor, keyPath...)
	if err != nil {
		if isKeyPathNotFound(err) {
			return []IndexRow{*newMissingRow(field.FieldName)}, nil
		}

		log.Err(err).Msgf("Failed to index field name: %s", field.FieldName)
		return nil, err
	}
	return rows, nil
}

func (q *SecondaryIndexer) getIndexedFields() []*schema.QueryableField {
	if q.indexAll {
		return q.coll.QueryableFields
	}

	return q.coll.GetIndexedFields()
}

// This is used to append the Primary key to the end of the key.
func newKeyWithPrimaryKey(id []interface{}, table []byte, indexParts ...interface{}) keys.Key {
	indexParts = append(indexParts, id...)
	return keys.NewKey(table, indexParts...)
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

func isKeyPathNotFound(err error) bool {
	return err.Error() == "Key path not found"
}

// These are acceptable errors during indexing. A field could be missing from the index or
// the field could be of type binary.
func isIgnoreableError(err error) bool {
	if isKeyPathNotFound(err) || strings.Contains(err.Error(), "do not index byte field") {
		return true
	}
	return false
}
