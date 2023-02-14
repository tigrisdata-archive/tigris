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

package kv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
	ulog "github.com/tigrisdata/tigris/util/log"
)

func readAllUsingIterator(t *testing.T, it Iterator) []KeyValue {
	res := make([]KeyValue, 0)

	var kv KeyValue
	for it.Next(&kv) {
		res = append(res, kv)
	}

	require.NoError(t, it.Err())

	return res
}

func readAll(t *testing.T, it baseIterator) []baseKeyValue {
	res := make([]baseKeyValue, 0)

	var kv baseKeyValue
	for it.Next(&kv) {
		res = append(res, kv)
	}

	require.NoError(t, it.Err())

	return res
}

func testKeyValueStoreBasic(t *testing.T, kv KeyValueStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nRecs := 5

	table := []byte("t1")
	err := kv.DropTable(ctx, table)
	require.NoError(t, err)

	err = kv.CreateTable(ctx, table)
	require.NoError(t, err)

	var tableDataP1 []*internal.TableData
	var tableDataP2 []*internal.TableData
	for i := 0; i < nRecs; i++ {
		tableDataP1 = append(tableDataP1, internal.NewTableData([]byte(fmt.Sprintf("value%d", i+1))))
		tableDataP2 = append(tableDataP2, internal.NewTableData([]byte(fmt.Sprintf("value%d", i+1))))
	}

	// insert records with two prefixes p1 and p2
	for i := 0; i < nRecs; i++ {
		err = kv.Insert(ctx, table, BuildKey("p1", i+1), tableDataP1[i])
		require.NoError(t, err)
		err = kv.Insert(ctx, table, BuildKey("p2", i+1), tableDataP2[i])
		require.NoError(t, err)
	}

	// read individual record
	it, err := kv.Read(ctx, table, BuildKey("p1", 2))
	require.NoError(t, err)

	v := readAllUsingIterator(t, it)
	require.Equal(t, []KeyValue{{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Data: tableDataP1[1]}}, v)

	// replace individual record
	replacedValue2 := internal.NewTableData([]byte("value2+2"))
	err = kv.Replace(ctx, table, BuildKey("p1", 2), replacedValue2, false)
	require.NoError(t, err)

	it, err = kv.Read(ctx, table, BuildKey("p1", 2))
	require.NoError(t, err)

	v = readAllUsingIterator(t, it)
	require.Equal(t, []KeyValue{{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Data: replacedValue2}}, v)

	// read range
	it, err = kv.ReadRange(ctx, table, BuildKey("p1", 2), BuildKey("p1", 4), false)
	require.NoError(t, err)

	v = readAllUsingIterator(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Data: replacedValue2},
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey(table, BuildKey("p1", int64(3))), Data: tableDataP1[2]},
	}, v)

	// update range
	i := 3
	var updatedData []*internal.TableData
	modifiedCount := int32(0)
	modifiedCount, err = kv.UpdateRange(ctx, table, BuildKey("p1", 3), BuildKey("p1", 6), func(orig *internal.TableData) (*internal.TableData, error) {
		require.Equal(t, fmt.Sprintf("value%d", i), string(orig.RawData))
		res := internal.NewTableData([]byte(fmt.Sprintf("value%d+%d", i, i)))
		i++
		updatedData = append(updatedData, res)
		return res, nil
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), modifiedCount)

	it, err = kv.ReadRange(ctx, table, BuildKey("p1", 3), BuildKey("p1", 6), false)
	require.NoError(t, err)

	v = readAllUsingIterator(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey(table, BuildKey("p1", int64(3))), Data: updatedData[0]},
		{Key: BuildKey("p1", int64(4)), FDBKey: getFDBKey(table, BuildKey("p1", int64(4))), Data: updatedData[1]},
		{Key: BuildKey("p1", int64(5)), FDBKey: getFDBKey(table, BuildKey("p1", int64(5))), Data: updatedData[2]},
	}, v)

	// prefix read
	it, err = kv.Read(ctx, table, BuildKey("p1"))
	require.NoError(t, err)

	v = readAllUsingIterator(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(1)), FDBKey: getFDBKey(table, BuildKey("p1", int64(1))), Data: tableDataP1[0]},
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Data: replacedValue2},
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey(table, BuildKey("p1", int64(3))), Data: updatedData[0]},
		{Key: BuildKey("p1", int64(4)), FDBKey: getFDBKey(table, BuildKey("p1", int64(4))), Data: updatedData[1]},
		{Key: BuildKey("p1", int64(5)), FDBKey: getFDBKey(table, BuildKey("p1", int64(5))), Data: updatedData[2]},
	}, v)

	// delete and delete range
	err = kv.Delete(ctx, table, BuildKey("p1", 1))
	require.NoError(t, err)

	err = kv.DeleteRange(ctx, table, BuildKey("p1", 3), BuildKey("p2", 6))
	require.NoError(t, err)

	it, err = kv.ReadRange(ctx, table, BuildKey("p1", 1), BuildKey("p1", 6), false)
	require.NoError(t, err)

	v = readAllUsingIterator(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Data: replacedValue2},
	}, v)

	err = kv.DropTable(ctx, table)
	require.NoError(t, err)
}

func testKeyValueStoreFullScan(t *testing.T, kv KeyValueStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nRecs := 5

	table := []byte("t1")
	err := kv.DropTable(ctx, table)
	require.NoError(t, err)

	err = kv.CreateTable(ctx, table)
	require.NoError(t, err)

	var tableDataP1 []*internal.TableData
	var tableDataP2 []*internal.TableData
	for i := 0; i < nRecs; i++ {
		tableDataP1 = append(tableDataP1, internal.NewTableData([]byte(fmt.Sprintf("value%d", i+1))))
		tableDataP2 = append(tableDataP2, internal.NewTableData([]byte(fmt.Sprintf("value%d", i+1))))
	}

	// insert records with two prefixes p1 and p2
	for i := 0; i < nRecs; i++ {
		err = kv.Insert(ctx, table, BuildKey("p1", i+1), tableDataP1[i])
		require.NoError(t, err)
		err = kv.Insert(ctx, table, BuildKey("p2", i+1), tableDataP2[i])
		require.NoError(t, err)
	}

	// prefix read
	it, err := kv.Read(ctx, table, nil)
	require.NoError(t, err)

	v := readAllUsingIterator(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(1)), FDBKey: getFDBKey(table, BuildKey("p1", int64(1))), Data: tableDataP1[0]},
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Data: tableDataP1[1]},
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey(table, BuildKey("p1", int64(3))), Data: tableDataP1[2]},
		{Key: BuildKey("p1", int64(4)), FDBKey: getFDBKey(table, BuildKey("p1", int64(4))), Data: tableDataP1[3]},
		{Key: BuildKey("p1", int64(5)), FDBKey: getFDBKey(table, BuildKey("p1", int64(5))), Data: tableDataP1[4]},
		{Key: BuildKey("p2", int64(1)), FDBKey: getFDBKey(table, BuildKey("p2", int64(1))), Data: tableDataP2[0]},
		{Key: BuildKey("p2", int64(2)), FDBKey: getFDBKey(table, BuildKey("p2", int64(2))), Data: tableDataP2[1]},
		{Key: BuildKey("p2", int64(3)), FDBKey: getFDBKey(table, BuildKey("p2", int64(3))), Data: tableDataP2[2]},
		{Key: BuildKey("p2", int64(4)), FDBKey: getFDBKey(table, BuildKey("p2", int64(4))), Data: tableDataP2[3]},
		{Key: BuildKey("p2", int64(5)), FDBKey: getFDBKey(table, BuildKey("p2", int64(5))), Data: tableDataP2[4]},
	}, v)

	err = kv.DropTable(ctx, table)
	require.NoError(t, err)
}

type TestCollection struct {
	Key    string `json:"key"`
	Field1 []byte `json:"field1"`
	Field2 []byte `json:"field2"`
	Field3 []byte `json:"field3"`
	Field4 []byte `json:"field4"`
}

func createDocument(t *testing.T) (string, []byte) {
	doc := &TestCollection{
		Key:    uuid.New().String(),
		Field1: []byte(`this is a random string`),
		Field2: []byte(`this is a random string`),
		Field3: []byte(`this is a random string`),
	}

	b, err := jsoniter.Marshal(doc)
	require.NoError(t, err)

	return doc.Key, b
}

func benchKV(t *testing.T, kv baseKVStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	table := []byte("t1")
	err := kv.DropTable(ctx, table)
	require.NoError(t, err)

	err = kv.CreateTable(ctx, table)
	require.NoError(t, err)

	var ops int64
	timer := time.NewTimer(1 * time.Second)
	start := time.Now()
	sigClose := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case _, ok := <-sigClose:
					if !ok {
						return
					}
				default:
					tx, err := kv.BeginTx(ctx)
					require.NoError(t, err)

					key, doc := createDocument(t)
					err = tx.Replace(ctx, table, BuildKey(key), doc, false)
					require.NoError(t, err)
					require.NoError(t, tx.Commit(ctx))
					atomic.AddInt64(&ops, 1)
				}
			}
		}()
	}

	<-timer.C
	close(sigClose)
	wg.Wait()
	require.NoError(t, kv.DropTable(ctx, table))

	t.Logf("total elapsed time for [%v] records [%v]", ops, time.Since(start))
}

func testKVBasic(t *testing.T, kv baseKVStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nRecs := 5

	table := []byte("t1")
	err := kv.DropTable(ctx, table)
	require.NoError(t, err)

	err = kv.CreateTable(ctx, table)
	require.NoError(t, err)

	// insert records with two prefixes p1 and p2
	for i := 0; i < nRecs; i++ {
		err = kv.Insert(ctx, table, BuildKey("p1", i+1), []byte(fmt.Sprintf("value%d", i+1)))
		require.NoError(t, err)
		err = kv.Insert(ctx, table, BuildKey("p2", i+1), []byte(fmt.Sprintf("value%d", i+1)))
		require.NoError(t, err)
	}

	// read individual record
	it, err := kv.Read(ctx, table, BuildKey("p1", 2))
	require.NoError(t, err)

	v := readAll(t, it)
	require.Equal(t, []baseKeyValue{{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Value: []byte("value2")}}, v)

	// replace individual record
	err = kv.Replace(ctx, table, BuildKey("p1", 2), []byte("value2+2"), false)
	require.NoError(t, err)

	it, err = kv.Read(ctx, table, BuildKey("p1", 2))
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []baseKeyValue{{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Value: []byte("value2+2")}}, v)

	// read range
	it, err = kv.ReadRange(ctx, table, BuildKey("p1", 2), BuildKey("p1", 4), false)
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []baseKeyValue{
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Value: []byte("value2+2")},
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey(table, BuildKey("p1", int64(3))), Value: []byte("value3")},
	}, v)

	// update range
	i := 3
	modifiedCount := int32(0)
	modifiedCount, err = kv.UpdateRange(ctx, table, BuildKey("p1", 3), BuildKey("p1", 6), func(orig []byte) ([]byte, error) {
		require.Equal(t, fmt.Sprintf("value%d", i), string(orig))
		res := []byte(fmt.Sprintf("value%d+%d", i, i))
		i++
		return res, nil
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), modifiedCount)

	it, err = kv.ReadRange(ctx, table, BuildKey("p1", 3), BuildKey("p1", 6), false)
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []baseKeyValue{
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey(table, BuildKey("p1", int64(3))), Value: []byte("value3+3")},
		{Key: BuildKey("p1", int64(4)), FDBKey: getFDBKey(table, BuildKey("p1", int64(4))), Value: []byte("value4+4")},
		{Key: BuildKey("p1", int64(5)), FDBKey: getFDBKey(table, BuildKey("p1", int64(5))), Value: []byte("value5+5")},
	}, v)

	// prefix read
	it, err = kv.Read(ctx, table, BuildKey("p1"))
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []baseKeyValue{
		{Key: BuildKey("p1", int64(1)), FDBKey: getFDBKey(table, BuildKey("p1", int64(1))), Value: []byte("value1")},
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Value: []byte("value2+2")},
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey(table, BuildKey("p1", int64(3))), Value: []byte("value3+3")},
		{Key: BuildKey("p1", int64(4)), FDBKey: getFDBKey(table, BuildKey("p1", int64(4))), Value: []byte("value4+4")},
		{Key: BuildKey("p1", int64(5)), FDBKey: getFDBKey(table, BuildKey("p1", int64(5))), Value: []byte("value5+5")},
	}, v)

	// delete and delete range
	err = kv.Delete(ctx, table, BuildKey("p1", 1))
	require.NoError(t, err)

	err = kv.DeleteRange(ctx, table, BuildKey("p1", 3), BuildKey("p2", 6))
	require.NoError(t, err)

	it, err = kv.ReadRange(ctx, table, BuildKey("p1", 1), BuildKey("p1", 6), false)
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []baseKeyValue{
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Value: []byte("value2+2")},
	}, v)

	err = kv.DropTable(ctx, table)
	require.NoError(t, err)
}

func testFullScan(t *testing.T, kv baseKVStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nRecs := 5

	table := []byte("t1")
	err := kv.DropTable(ctx, table)
	require.NoError(t, err)

	err = kv.CreateTable(ctx, table)
	require.NoError(t, err)

	// insert records with two prefixes p1 and p2
	for i := 0; i < nRecs; i++ {
		err = kv.Insert(ctx, table, BuildKey("p1", i+1), []byte(fmt.Sprintf("value%d", i+1)))
		require.NoError(t, err)
		err = kv.Insert(ctx, table, BuildKey("p2", i+1), []byte(fmt.Sprintf("value%d", i+1)))
		require.NoError(t, err)
	}

	// prefix read
	it, err := kv.Read(ctx, table, nil)
	require.NoError(t, err)

	v := readAll(t, it)
	require.Equal(t, []baseKeyValue{
		{Key: BuildKey("p1", int64(1)), FDBKey: getFDBKey(table, BuildKey("p1", int64(1))), Value: []byte("value1")},
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey(table, BuildKey("p1", int64(2))), Value: []byte("value2")},
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey(table, BuildKey("p1", int64(3))), Value: []byte("value3")},
		{Key: BuildKey("p1", int64(4)), FDBKey: getFDBKey(table, BuildKey("p1", int64(4))), Value: []byte("value4")},
		{Key: BuildKey("p1", int64(5)), FDBKey: getFDBKey(table, BuildKey("p1", int64(5))), Value: []byte("value5")},
		{Key: BuildKey("p2", int64(1)), FDBKey: getFDBKey(table, BuildKey("p2", int64(1))), Value: []byte("value1")},
		{Key: BuildKey("p2", int64(2)), FDBKey: getFDBKey(table, BuildKey("p2", int64(2))), Value: []byte("value2")},
		{Key: BuildKey("p2", int64(3)), FDBKey: getFDBKey(table, BuildKey("p2", int64(3))), Value: []byte("value3")},
		{Key: BuildKey("p2", int64(4)), FDBKey: getFDBKey(table, BuildKey("p2", int64(4))), Value: []byte("value4")},
		{Key: BuildKey("p2", int64(5)), FDBKey: getFDBKey(table, BuildKey("p2", int64(5))), Value: []byte("value5")},
	}, v)

	err = kv.DropTable(ctx, table)
	require.NoError(t, err)
}

type kvTestCase struct {
	name   string
	insert []baseKeyValue
	test   []baseKeyValue
	result []baseKeyValue
	err    error
}

func testKVInsert(t *testing.T, kv baseKVStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	table := []byte("t1")
	cases := []kvTestCase{
		{
			name:   "simple",
			test:   []baseKeyValue{{BuildKey("p1"), nil, []byte("value1")}},
			result: []baseKeyValue{{BuildKey("p1"), getFDBKey(table, BuildKey("p1")), []byte("value1")}},
		},
		{
			name:   "conflict",
			insert: []baseKeyValue{{BuildKey("p1"), nil, []byte("value1")}},
			test:   []baseKeyValue{{BuildKey("p1"), nil, []byte("value2")}},
			result: []baseKeyValue{{BuildKey("p1"), getFDBKey(table, BuildKey("p1")), []byte("value1")}},
			err:    ErrDuplicateKey,
		},
	}

	for _, v := range cases {
		err := kv.DropTable(ctx, table)
		require.NoError(t, err)

		err = kv.CreateTable(ctx, table)
		require.NoError(t, err)

		t.Run(v.name, func(t *testing.T) {
			for _, i := range v.insert {
				err := kv.Insert(context.TODO(), table, i.Key, i.Value)
				require.NoError(t, err)
			}
			for _, i := range v.test {
				err := kv.Insert(context.TODO(), table, i.Key, i.Value)
				if v.err != nil {
					require.EqualError(t, err, v.err.Error())
				} else {
					require.NoError(t, err)
				}
			}
			for _, i := range v.result {
				it, err := kv.Read(context.Background(), table, i.Key)
				require.NoError(t, err)
				var res baseKeyValue
				require.True(t, it.Next(&res))
				require.NoError(t, it.Err())
				require.Equal(t, i, res)
				require.True(t, !it.Next(&res))
				require.NoError(t, it.Err())
			}
		})
	}

	t.Run("test_kv_timeout", func(t *testing.T) {
		testKVTimeout(t, kv)
	})

	t.Run("test_kv_retriable", func(t *testing.T) {
		tx, err := kv.BeginTx(ctx)
		require.NoError(t, err)
		var ep fdb.Error
		ep.Code = 1020
		tx.(*ftx).err = ep
		assert.True(t, tx.IsRetriable())
		ep.Code = 2000
		tx.(*ftx).err = ep
		assert.False(t, tx.IsRetriable())
		tx.(*ftx).err = fmt.Errorf("error")
		assert.False(t, tx.IsRetriable())
	})

	err := kv.DropTable(ctx, table)
	require.NoError(t, err)
}

func testKVTimeout(t *testing.T, kv baseKVStore) {
	ctx, cancel1 := context.WithTimeout(context.Background(), 3*time.Millisecond)
	defer cancel1()

	tx, err := kv.BeginTx(ctx)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	err = tx.Commit(context.Background())
	assert.Error(t, err)
	assert.False(t, tx.IsRetriable())

	tx, err = kv.BeginTx(context.Background())
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	err = tx.Commit(context.Background())
	assert.NoError(t, err)

	ctx, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-3*time.Millisecond))
	defer cancel2()
	_, err = kv.BeginTx(ctx)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func testFDBKVIterator(t *testing.T, kv baseKVStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	table := []byte("t1")
	err := kv.DropTable(ctx, table)
	require.NoError(t, err)

	err = kv.CreateTable(ctx, table)
	require.NoError(t, err)

	nRecs := 5

	for i := 0; i < nRecs; i++ {
		err = kv.Insert(ctx, table, BuildKey("p1", i+1), []byte(fmt.Sprintf("value%d", i+1)))
		require.NoError(t, err)
	}

	it, err := kv.Read(ctx, table, nil)
	require.NoError(t, err)

	ic, ok := it.(*fdbIteratorTxCloser)
	require.True(t, ok)
	fi, ok := ic.baseIterator.(*fdbIterator)
	require.True(t, ok)

	var v baseKeyValue
	assert.True(t, it.Next(nil))
	assert.True(t, it.Next(&v))
	assert.NotNil(t, ic.tx)

	fi.subspace = subspace.FromBytes([]byte("invalid"))

	assert.False(t, it.Next(&v))
	assert.Nil(t, ic.tx)
	// Next should not fail after error
	assert.False(t, it.Next(&v))
	assert.Error(t, it.Err())

	err = kv.DropTable(ctx, table)
	require.NoError(t, err)
}

func testSetVersionstampedValue(t *testing.T, kv baseKVStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	tx, err := kv.BeginTx(ctx)
	require.NoError(t, err)

	require.NoError(t, tx.SetVersionstampedValue(ctx, []byte("foo"), []byte("bar")))
	err = tx.Commit(ctx)

	var ep fdb.Error
	require.True(t, errors.As(err, &ep))
	require.Equal(t, 2000, ep.Code)
	assert.False(t, tx.IsRetriable())

	tx, err = kv.BeginTx(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.SetVersionstampedValue(ctx, []byte{0xff, '/', 'm', 'e', 't', 'a', 'd', 'a', 't', 'a', 'V', 'e', 'r', 's', 'i', 'o', 'n'}, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
	require.NoError(t, tx.Commit(ctx))
}

func testKVAddAtomicValue(t *testing.T, kv baseKVStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	key := BuildKey([]byte("foo"))
	key2 := BuildKey([]byte("foo-2"))
	table := []byte("t1")

	err := kv.DropTable(ctx, table)
	require.NoError(t, err)

	err = kv.CreateTable(ctx, table)
	require.NoError(t, err)

	tx, err := kv.BeginTx(ctx)
	require.NoError(t, err)

	require.NoError(t, tx.AtomicAdd(ctx, table, key, 1))
	err = tx.Commit(ctx)
	require.NoError(t, err)

	tx, err = kv.BeginTx(ctx)
	require.NoError(t, err)

	val, err := tx.AtomicRead(ctx, table, key)
	require.NoError(t, err)
	require.Equal(t, int64(1), val)

	require.NoError(t, tx.AtomicAdd(ctx, table, key, -10))
	require.NoError(t, tx.Commit(ctx))

	tx, err = kv.BeginTx(ctx)
	require.NoError(t, err)

	val, err = tx.AtomicRead(ctx, table, key)
	require.NoError(t, err)
	require.Equal(t, int64(-9), val)

	require.NoError(t, tx.AtomicAdd(ctx, table, key, 20))
	require.NoError(t, tx.AtomicAdd(ctx, table, key2, 5))
	err = tx.Commit(ctx)
	require.NoError(t, err)

	tx, err = kv.BeginTx(ctx)
	require.NoError(t, err)

	iter, err := tx.AtomicReadRange(ctx, table, key, nil, false)
	require.NoError(t, err)

	var rangeVal FdbBaseKeyValue[int64]
	count := 0
	expected := []int64{11, 5}
	for iter.Next(&rangeVal) {
		require.Equal(t, expected[count], rangeVal.Data)
		count += 1
	}

	require.Equal(t, 2, count)
	err = tx.Commit(ctx)
	require.NoError(t, err)
}

func TestKVFDB(t *testing.T) {
	cfg, err := config.GetTestFDBConfig("../..")
	require.NoError(t, err)

	kvStore, err := NewKeyValueStore(cfg)
	require.NoError(t, err)

	kv, err := newFoundationDB(cfg)
	require.NoError(t, err)

	t.Run("TestKVFBench", func(t *testing.T) {
		benchKV(t, kv)
	})

	t.Run("TestKVFDBBasic", func(t *testing.T) {
		testKVBasic(t, kv)
	})
	t.Run("TestKeyValueStoreBasic", func(t *testing.T) {
		testKeyValueStoreBasic(t, kvStore)
	})
	t.Run("TestKVFDBInsert", func(t *testing.T) {
		testKVInsert(t, kv)
	})
	t.Run("TestKVFDBFullScan", func(t *testing.T) {
		testFullScan(t, kv)
	})
	t.Run("TestKVFDBFullScan", func(t *testing.T) {
		testKeyValueStoreFullScan(t, kvStore)
	})
	t.Run("TestKVFDBIterator", func(t *testing.T) {
		testFDBKVIterator(t, kv)
	})
	t.Run("TestSetVersionstampedValue", func(t *testing.T) {
		testSetVersionstampedValue(t, kv)
	})

	t.Run("TestAtomicAdd", func(t *testing.T) {
		testKVAddAtomicValue(t, kv)
	})
}

func TestGetCtxTimeout(t *testing.T) {
	// FIXME: time.Now dependent, may be flaky on slow machine
	// positive timeout set in the context
	ctx, cancel1 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel1()
	assert.Greater(t, getCtxTimeout(ctx), int64(0))

	// not timeout set in the context
	assert.Equal(t, int64(0), getCtxTimeout(context.Background()))

	// expired context timeout
	ctx, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-10*time.Millisecond))
	defer cancel2()
	assert.Less(t, getCtxTimeout(ctx), int64(0))
}

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled"})
	os.Exit(m.Run())
}
