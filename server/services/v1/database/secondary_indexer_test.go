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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

var kvStore kv.KeyValueStore

func TestIndexingCreateSimpleKVsforDoc(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"double_f": {
				"type": "number",
				"default": 1.5
			},
			"created": {
				"type": "string",
				"format": "date-time",
				"createdAt": true
			},
			"updated": {
				"type": "string",
				"format": "date-time",
				"updatedAt": true
			},
			"binary_val": {
				"type": "string",
				"format": "byte"
			},
			"arr": {
				"type": "array",
				"items": {
					"type": "integer"
				},
				"default": [10,20,30]
			}
		},
		"primary_key": ["id"]
	}`)

	indexStore := setupTest(t, reqSchema)
	td, primaryKey := createDoc(`{"id":1, "double_f":2,"created":"2023-01-16T12:55:17.304154Z","updated": "2023-01-16T12:55:17.304154Z","binary_val": "cGVlay1hLWJvbwo=", "arr":[1,2]}`)
	t.Run("insert", func(t *testing.T) {
		updateSet, err := indexStore.buildAddAndRemoveKVs(td, nil, primaryKey)
		assert.NoError(t, err)
		expected := [][]interface{}{
			{"skey", KVSubspace, "_tigris_created_at", 1, td.CreatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "_tigris_updated_at", 1, td.UpdatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "id", 1, int64(1), 0, 1},
			{"skey", KVSubspace, "double_f", 1, float64(2), 0, 1},
			{"skey", KVSubspace, "created", 1, "2023-01-16T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "updated", 1, "2023-01-16T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "arr", 1, int64(1), 0, 1},
			{"skey", KVSubspace, "arr", 1, int64(2), 1, 1},
		}
		assertKVs(t, expected, updateSet.addKeys, updateSet.addCounts)
	})
	t.Run("update new values", func(t *testing.T) {
		updateTD, _ := createDoc(`{"id":1, "double_f":3,"created":"2023-01-17T12:55:17.304154Z","updated": "2023-01-17T12:55:17.304154Z","binary_val": "cGVlay1hLWJvbwo=", "arr":[1,3]}`)
		updateTD.CreatedAt = td.CreatedAt
		updateSet, err := indexStore.buildAddAndRemoveKVs(updateTD, td, primaryKey)
		assert.NoError(t, err)

		expectedAdd := [][]interface{}{
			{"skey", KVSubspace, "_tigris_updated_at", 1, updateTD.UpdatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "double_f", 1, float64(3), 0, 1},
			{"skey", KVSubspace, "created", 1, "2023-01-17T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "updated", 1, "2023-01-17T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "arr", 1, int64(3), 1, 1},
		}

		expectedRemove := [][]interface{}{
			{"skey", KVSubspace, "_tigris_updated_at", 1, td.UpdatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "double_f", 1, float64(2), 0, 1},
			{"skey", KVSubspace, "created", 1, "2023-01-16T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "updated", 1, "2023-01-16T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "arr", 1, int64(2), 1, 1},
		}
		assertKVs(t, expectedAdd, updateSet.addKeys, nil)
		assertKVs(t, expectedRemove, updateSet.removeKeys, nil)
	})

	t.Run("delete", func(t *testing.T) {
		updateSet, err := indexStore.buildAddAndRemoveKVs(nil, td, primaryKey)
		assert.NoError(t, err)
		expected := [][]interface{}{
			{"skey", KVSubspace, "_tigris_created_at", 1, td.CreatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "_tigris_updated_at", 1, td.UpdatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "id", 1, int64(1), 0, 1},
			{"skey", KVSubspace, "double_f", 1, float64(2), 0, 1},
			{"skey", KVSubspace, "created", 1, "2023-01-16T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "updated", 1, "2023-01-16T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "arr", 1, int64(1), 0, 1},
			{"skey", KVSubspace, "arr", 1, int64(2), 1, 1},
		}
		assert.Len(t, updateSet.addKeys, 0)
		assertKVs(t, expected, updateSet.removeKeys, updateSet.removeCounts)
	})
}

func TestIndexingMissing(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"double_f": {
				"type": "number"
			},
			"a_string": {
				"type": "string"
			},
			"updated": {
				"type": "string",
				"format": "date-time"
			},
			"arr": {
				"type": "array",
				"items": {
					"type": "integer"
				}
			},
			"arr2": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"nested": { "type": "boolean" }
					}
				}
			}
		},
		"primary_key": ["id"]
	}`)

	indexStore := setupTest(t, reqSchema)
	td, primaryKey := createDoc(`{"id":1`)
	updateSet, err := indexStore.buildAddAndRemoveKVs(td, nil, primaryKey)
	assert.NoError(t, err)
	expected := [][]interface{}{
		{"skey", KVSubspace, "_tigris_created_at", 1, td.CreatedAt.ToRFC3339(), 0, 1},
		{"skey", KVSubspace, "_tigris_updated_at", 1, td.UpdatedAt.ToRFC3339(), 0, 1},
		{"skey", KVSubspace, "id", 1, int64(1), 0, 1},
	}
	assertKVs(t, expected, updateSet.addKeys, updateSet.addCounts)
}

func TestIndexingNull(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"double_f": {
				"type": "number"
			},
			"created": {
				"type": "string",
				"format": "date-time"
			},
			"updated": {
				"type": "string",
				"format": "date-time"
			},
			"arr": {
				"type": "array",
				"items": {
					"type": "integer"
				}
			}
		},
		"primary_key": ["id"]
	}`)

	indexStore := setupTest(t, reqSchema)

	td, primaryKey := createDoc(`{"id":1, "double_f":null,"created":null,"updated":null, "arr":[null,null]}`)
	td.CreatedAt = nil
	td.UpdatedAt = nil

	t.Run("create nulls", func(t *testing.T) {
		updateSet, err := indexStore.buildAddAndRemoveKVs(td, nil, primaryKey)
		assert.NoError(t, err)
		expected := [][]interface{}{
			{"skey", KVSubspace, "_tigris_created_at", 1, nil, 0, 1},
			{"skey", KVSubspace, "_tigris_updated_at", 1, nil, 0, 1},
			{"skey", KVSubspace, "id", 1, int64(1), 0, 1},
			{"skey", KVSubspace, "double_f", 1, nil, 0, 1},
			{"skey", KVSubspace, "created", 1, nil, 0, 1},
			{"skey", KVSubspace, "updated", 1, nil, 0, 1},
			{"skey", KVSubspace, "arr", 1, nil, 0, 1},
			{"skey", KVSubspace, "arr", 1, nil, 1, 1},
		}
		assertKVs(t, expected, updateSet.addKeys, updateSet.addCounts)
	})

	t.Run("update nulls", func(t *testing.T) {
		updatedTd, _ := createDoc(`{"id":1, "double_f":5,"created":null,"updated":"2023-01-16T12:55:17.304154Z", "arr":[null,1]}`)
		updateSet, err := indexStore.buildAddAndRemoveKVs(updatedTd, td, primaryKey)
		assert.NoError(t, err)
		expectedAdded := [][]interface{}{
			{"skey", KVSubspace, "_tigris_created_at", 1, updatedTd.CreatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "_tigris_updated_at", 1, updatedTd.UpdatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "double_f", 1, float64(5), 0, 1},
			{"skey", KVSubspace, "updated", 1, "2023-01-16T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "arr", 1, int64(1), 1, 1},
		}
		assertKVs(t, expectedAdded, updateSet.addKeys, nil)
		expectedRemoved := [][]interface{}{
			{"skey", KVSubspace, "_tigris_created_at", 1, nil, 0, 1},
			{"skey", KVSubspace, "_tigris_updated_at", 1, nil, 0, 1},
			{"skey", KVSubspace, "double_f", 1, nil, 0, 1},
			{"skey", KVSubspace, "updated", 1, nil, 0, 1},
			{"skey", KVSubspace, "arr", 1, nil, 1, 1},
		}
		assertKVs(t, expectedRemoved, updateSet.removeKeys, nil)
	})
}

func TestIndexingStringEncoding(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"string_val": {
				"type": "string"
			},
			"created": {
				"type": "string",
				"format": "date-time",
				"createdAt": true
			},
			"arr": {
				"type": "array",
				"items": {
					"type": "string"
				}
			}
		},
		"primary_key": ["id"]
	}`)

	indexStore := setupTest(t, reqSchema)

	t.Run("encodes strings correctly", func(t *testing.T) {
		td, primaryKey := createDoc(`{"id":1, "string_val":"a simple string value","created":"2023-01-16T12:55:17.304154Z","arr":["one", "two"]}`)
		updateSet, err := indexStore.buildAddAndRemoveKVs(td, nil, primaryKey)
		assert.NoError(t, err)
		expected := [][]interface{}{
			{"skey", KVSubspace, "_tigris_created_at", 1, td.CreatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "_tigris_updated_at", 1, td.UpdatedAt.ToRFC3339(), 0, 1},
			{"skey", KVSubspace, "id", 1, int64(1), 0, 1},
			{"skey", KVSubspace, "string_val", 1, stringEncoder("a simple string value"), 0, 1},
			{"skey", KVSubspace, "created", 1, "2023-01-16T12:55:17.304154Z", 0, 1},
			{"skey", KVSubspace, "arr", 1, stringEncoder("one"), 0, 1},
			{"skey", KVSubspace, "arr", 1, stringEncoder("two"), 1, 1},
		}
		assertKVs(t, expected, updateSet.addKeys, updateSet.addCounts)
	})

	t.Run("concaternates longer strings", func(t *testing.T) {
		longStr := stringEncoder("this is a very long string that will be larger than 64 bytes so that we concaternate it correctly")
		td, primaryKey := createDoc(`{"id":1, "string_val":"this is a very long string that will be larger than 64 bytes so that we concaternate it correctly","created":"2023-01-16T12:55:17.304154Z","arr":["one", "two"]}`)
		updateSet, err := indexStore.buildAddAndRemoveKVs(td, nil, primaryKey)
		assert.NoError(t, err)
		assert.Equal(t, []interface{}{"skey", KVSubspace, "string_val", 1, longStr, 0, 1}, updateSet.addKeys[3].IndexParts())
	})
}

func TestIndexingObjectArrayKVGen(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"object1": {
				"type": "object",
				"properties": {
					"val1": { "type": "string" },
					"val2": { "type": "number" },
					"val3": {
						"type": "object",
						"properties": {
							"nested": { "type": "boolean" },
							"another": { "type": "number" },
							"arrayval": {
								"type": "array",
								"items": {
									"type": "object",
									"properties": {
										"val1": {"type": "string"},
										"val2": {"type": "boolean"},
										"val3": {
											"type": "array",
											"items": {
												"type": "number"
											}
										}
									}
								}
							}
						}
					}
				}
			},
			"arr": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"val1": {"type": "integer"},
						"val2": {"type": "number"}
					}
				}
			}
		},
		"primary_key": ["id"]
	}`)

	indexStore := setupTest(t, reqSchema)

	td, primaryKey := createDoc(`{
		"id":1,
		"object1":{
			"val1": "one",
			"val2": 2,
			"val3": {
				"nested": true,
				"another": 100,
				"arrayval": [{"val1": "one","val2":false, "val3": [1, 2]},{"val1": "one","val2":false, "val3": [10, 20]}]
			}
		},
		"arr": [{"val1":1,"val2":2.0},{"val1":1,"val2":5.0}]}
	`)

	updateSet, err := indexStore.buildAddAndRemoveKVs(td, nil, primaryKey)
	assert.NoError(t, err)

	expected := [][]interface{}{
		{"skey", KVSubspace, "_tigris_created_at", 1, td.CreatedAt.ToRFC3339(), 0, 1},
		{"skey", KVSubspace, "_tigris_updated_at", 1, td.UpdatedAt.ToRFC3339(), 0, 1},
		{"skey", KVSubspace, "id", 1, int64(1), 0, 1},
		{"skey", KVSubspace, "object1.val1", 1, stringEncoder("one"), 0, 1},
		{"skey", KVSubspace, "object1.val2", 1, float64(2), 0, 1},
		{"skey", KVSubspace, "object1.val3.nested", 1, true, 0, 1},
		{"skey", KVSubspace, "object1.val3.another", 1, float64(100), 0, 1},
		{"skey", KVSubspace, "object1.val3.arrayval.val1", 1, stringEncoder("one"), 0, 1},
		{"skey", KVSubspace, "object1.val3.arrayval.val2", 1, false, 0, 1},
		{"skey", KVSubspace, "object1.val3.arrayval.val3._tigris_array_stub", 1, interface{}(nil), 0, 1},
		{"skey", KVSubspace, "object1.val3.arrayval.val1", 1, stringEncoder("one"), 1, 1},
		{"skey", KVSubspace, "object1.val3.arrayval.val2", 1, false, 1, 1},
		{"skey", KVSubspace, "arr.val1", 1, float64(1), 0, 1},
		{"skey", KVSubspace, "arr.val2", 1, float64(2.0), 0, 1},
		{"skey", KVSubspace, "arr.val1", 1, float64(1), 1, 1},
		{"skey", KVSubspace, "arr.val2", 1, float64(5.0), 1, 1},
	}
	assertKVs(t, expected, updateSet.addKeys, updateSet.addCounts)
}

// Add stubs for nested arrays.
func TestIndexingArrayWithObjectAndNestedArrayKeyGen(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"arr": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"val1": {"type": "integer"},
						"val2": {
							"type": "array",
							"items": {
								"type": "number"
							}
						},
						"val3": {
							"type": "array",
							"items": {
								"type": "object",
								"properties": {
									"nested1": {"type": "string"},
									"nested2": {"type": "boolean"}
								}
							}
						}
					}
				}
			},
			"arr2": {
				"type": "array",
				"items": {
					"type": "array",
					"items": {
						"type": "number"
					}
				}
			}
		},
		"primary_key": ["id"]
	}`)

	indexStore := setupTest(t, reqSchema)

	td, primaryKey := createDoc(`{
		"id":1,
		"arr": [{
				"val1":1,
				"val2":2.0,
				"val3": [{"nested1": "one", "nested2": true}, {"nested1": "two", "nested2": true}]
			},
			{
				"val1":1,
				"val2":5.0,
				"val3": [{"nested1": "one", "nested2": false}, {"nested1": "three", "nested2": true}]
			}],
		"arr2": [[1,2,3], [6,7,8]]
		}
	`)

	updateSet, err := indexStore.buildAddAndRemoveKVs(td, nil, primaryKey)
	assert.NoError(t, err)

	expected := [][]interface{}{
		{"skey", KVSubspace, "_tigris_created_at", 1, td.CreatedAt.ToRFC3339(), 0, 1},
		{"skey", KVSubspace, "_tigris_updated_at", 1, td.UpdatedAt.ToRFC3339(), 0, 1},
		{"skey", KVSubspace, "id", 1, int64(1), 0, 1},
		{"skey", KVSubspace, "arr.val1", 1, float64(1), 0, 1},
		{"skey", KVSubspace, "arr.val2", 1, float64(2.0), 0, 1},
		{"skey", KVSubspace, "arr.val3._tigris_array_stub", 1, interface{}(nil), 0, 1},

		{"skey", KVSubspace, "arr.val1", 1, float64(1), 1, 1},
		{"skey", KVSubspace, "arr.val2", 1, float64(5.0), 1, 1},
		{"skey", KVSubspace, "arr2._tigris_array_stub", 1, interface{}(nil), 0, 1},
	}

	assertKVs(t, expected, updateSet.addKeys, updateSet.addCounts)
}

func TestIndexingStoreAndGetSimpleKVsforDoc(t *testing.T) {
	reqSchema := []byte(`{
		"title": "t1",
		"properties": {
			"id": {
				"type": "integer"
			},
			"double_f": {
				"type": "number",
				"default": 1.5
			},
			"created": {
				"type": "string",
				"format": "date-time",
				"createdAt": true
			},
			"updated": {
				"type": "string",
				"format": "date-time",
				"updatedAt": true
			},
			"arr": {
				"type": "array",
				"items": {
					"type": "integer"
				},
				"default": [10,20,30]
			}
		},
		"primary_key": ["id"]
	}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assert.NoError(t, kvStore.DropTable(ctx, []byte("t1")))
	assert.NoError(t, kvStore.CreateTable(ctx, []byte("t1")))
	assert.NoError(t, kvStore.DropTable(ctx, []byte("sidx1")))
	assert.NoError(t, kvStore.CreateTable(ctx, []byte("sidx1")))
	indexStore := setupTest(t, reqSchema)

	tm := transaction.NewManager(kvStore)

	t.Run("lots of docs for size test", func(t *testing.T) {
		coll := indexStore.coll
		_ = kvStore.DropTable(ctx, coll.EncodedTableIndexName)

		for z := 0; z < 30; z++ {
			tx, err := tm.StartTx(ctx)
			for i := z * 30; i < 30*z+30; i++ {
				assert.NoError(t, err)
				td, pk := createDoc(`{"id":1, "double_f":2,"created":"2023-01-16T12:55:17.304154Z","updated": "2023-01-16T12:55:17.304154Z", "arr":[1,2]}`, []interface{}{i}...)
				err = indexStore.Index(ctx, tx, td, pk)
				assert.NoError(t, err)
			}
			assert.NoError(t, tx.Commit(ctx))
		}

		tx, err := tm.StartTx(ctx)
		assert.NoError(t, err)
		info, err := indexStore.IndexInfo(ctx, tx)
		assert.NoError(t, err)
		assert.Greater(t, info.Size, int64(100000))
		assert.Equal(t, int64(7200), info.Rows)
		err = tx.Commit(ctx)
		assert.NoError(t, err)
	})

	t.Run("insert", func(t *testing.T) {
		coll := indexStore.coll
		_ = kvStore.DropTable(ctx, coll.EncodedTableIndexName)
		tx, err := tm.StartTx(ctx)
		assert.NoError(t, err)

		info, err := indexStore.IndexInfo(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), info.Rows)
		assert.Equal(t, info.Size, int64(0))

		td, pk := createDoc(`{"id":1, "double_f":2,"created":"2023-01-16T12:55:17.304154Z","updated": "2023-01-16T12:55:17.304154Z", "arr":[1,2]}`)

		err = indexStore.Index(ctx, tx, td, pk)
		assert.NoError(t, err)

		err = tx.Commit(ctx)
		assert.NoError(t, err)

		tx, err = tm.StartTx(ctx)
		assert.NoError(t, err)

		info, err = indexStore.IndexInfo(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, int64(8), info.Rows)

		iter, err := indexStore.scanIndex(ctx, tx)
		assert.NoError(t, err)

		count := 0
		var row kv.KeyValue
		for iter.Next(&row) {
			count += 1
		}
		assert.NoError(t, err)
		assert.Nil(t, iter.Err())
		assert.Equal(t, 8, count)
	})

	t.Run("update", func(t *testing.T) {
		coll := indexStore.coll
		_ = kvStore.DropTable(ctx, coll.EncodedTableIndexName)
		td, pk := createDoc(`{"id":1, "double_f":2,"created":"2023-01-16T12:55:17.304154Z","updated": "2023-01-16T12:55:17.304154Z", "arr":[1,2]}`)

		tx, err := tm.StartTx(ctx)
		assert.NoError(t, err)
		assert.NoError(t, indexStore.Index(ctx, tx, td, pk))
		assert.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		assert.NoError(t, err)

		info, err := indexStore.IndexInfo(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, int64(8), info.Rows)

		iter, err := indexStore.scanIndex(ctx, tx)
		assert.NoError(t, err)

		count := 0
		var row kv.KeyValue
		for iter.Next(&row) {
			count += 1
		}
		assert.NoError(t, err)
		assert.Nil(t, iter.Err())
		assert.Equal(t, 8, count)

		updatedTd, _ := createDoc(`{"id":1, "double_f":2,"created":"2023-01-16T12:55:17.304154Z", "arr":[2, 3]}`)
		tx, err = tm.StartTx(ctx)
		assert.NoError(t, err)
		assert.NoError(t, indexStore.Update(ctx, tx, updatedTd, td, pk))
		assert.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		assert.NoError(t, err)
		info, err = indexStore.IndexInfo(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, int64(7), info.Rows)

		iter, err = indexStore.scanIndex(ctx, tx)
		assert.NoError(t, err)

		count = 0
		for iter.Next(&row) {
			count += 1
		}
		assert.NoError(t, err)
		assert.Nil(t, iter.Err())
		assert.Equal(t, 7, count)
		assert.NoError(t, tx.Commit(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		coll := indexStore.coll
		_ = kvStore.DropTable(ctx, coll.EncodedTableIndexName)
		tx, err := tm.StartTx(ctx)
		assert.NoError(t, err)

		td1, pk1 := createDoc(`{"id":1, "double_f":2,"created":"2023-01-16T12:55:17.304154Z","updated": "2023-01-16T12:55:17.304154Z", "arr":[1,2]}`)

		err = indexStore.Index(ctx, tx, td1, pk1)
		assert.NoError(t, err)

		td2, pk2 := createDoc(`{"id":2, "double_f":4,"created":"2023-01-17T12:00:00.304154Z","updated": "2023-01-17T12:05:10.304154Z", "arr":[1,3]}`, 2)
		err = indexStore.Index(ctx, tx, td2, pk2)
		assert.NoError(t, err)

		assert.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		assert.NoError(t, err)

		info, err := indexStore.IndexInfo(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, int64(16), info.Rows)

		err = indexStore.Delete(ctx, tx, td1, pk1)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTx(ctx)
		assert.NoError(t, err)

		info, err = indexStore.IndexInfo(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, int64(8), info.Rows)

		iter, err := indexStore.scanIndex(ctx, tx)
		assert.NoError(t, err)

		count := 0
		var row kv.KeyValue
		for iter.Next(&row) {
			id := row.Key[len(row.Key)-1]

			assert.Equal(t, int64(2), id)
			count += 1
		}
		assert.NoError(t, err)
		assert.NoError(t, iter.Err())
		assert.Equal(t, 8, count)
		assert.NoError(t, tx.Commit(ctx))
	})
}

func setupTest(t *testing.T, reqSchema []byte) *SecondaryIndexer {
	schFactory, err := schema.NewFactoryBuilder(true).Build("t1", reqSchema)
	assert.NoError(t, err)
	coll, err := schema.NewDefaultCollection(1, 1, schFactory, nil, nil)
	assert.NoError(t, err)
	coll.EncodedName = []byte("t1")
	coll.EncodedTableIndexName = []byte("sidx1")
	indexer := NewSecondaryIndexer(coll)
	indexer.indexAll = true

	return indexer
}

func assertKVs(t *testing.T, expected [][]interface{}, indexKeys []keys.Key, counts map[string]int64) {
	assert.Equal(t, len(expected), len(indexKeys))
	calculatedCounts := map[string]int64{}
	for i, key := range expected {
		assert.Equal(t, key, indexKeys[i].IndexParts())
		fieldName := key[2].(string)

		if val, ok := calculatedCounts[fieldName]; ok {
			calculatedCounts[fieldName] = val + 1
		} else {
			calculatedCounts[fieldName] = 1
		}
	}

	if counts != nil {
		assert.Equal(t, calculatedCounts, counts)
	}
}

func createDoc(doc string, primaryKey ...interface{}) (*internal.TableData, []interface{}) {
	td := createTD([]byte(doc))
	if len(primaryKey) == 0 {
		primaryKey = []interface{}{1}
	}
	return td, primaryKey
}

func createTD(doc []byte) *internal.TableData {
	return internal.NewTableDataWithTS(internal.NewTimestamp(), internal.NewTimestamp(), doc)
}

func stringEncoder(input string) interface{} {
	inputBytes := []byte(input)

	if len(inputBytes) > 64 {
		inputBytes = inputBytes[:64]
	}

	collator := collate.New(language.English)
	var buf collate.Buffer

	return collator.Key(&buf, inputBytes)
}

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled"})

	fdbCfg, err := config.GetTestFDBConfig("../../../../")
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB config: %v", err))
	}

	kvStore, err = kv.NewKeyValueStore(fdbCfg)
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB KV %v", err))
	}

	os.Exit(m.Run())
}
