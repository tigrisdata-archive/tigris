package kv

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisdb/server/config"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
)

func readAll(t *testing.T, it Iterator) []KeyValue {
	res := make([]KeyValue, 0)

	for it.More() {
		v, err := it.Next()
		require.NoError(t, err)

		res = append(res, *v)
	}

	return res
}

func testKVBasic(t *testing.T, kv KV) {
	ulog.Configure(ulog.LogConfig{Level: "trace"})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nRecs := 5

	err := kv.DropTable(ctx, "t1")
	require.NoError(t, err)

	err = kv.CreateTable(ctx, "t1")
	require.NoError(t, err)

	// insert records with two prefixes p1 and p2
	for i := 0; i < nRecs; i++ {
		err = kv.Insert(ctx, "t1", BuildKey("p1", i+1), []byte(fmt.Sprintf("value%d", i+1)))
		require.NoError(t, err)
		err = kv.Insert(ctx, "t1", BuildKey("p2", i+1), []byte(fmt.Sprintf("value%d", i+1)))
		require.NoError(t, err)
	}

	// read individual record
	it, err := kv.Read(ctx, "t1", BuildKey("p1", 2))
	require.NoError(t, err)

	v := readAll(t, it)
	fmt.Println(string(v[0].FDBKey))
	require.Equal(t, []KeyValue{{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(2))), Value: []byte("value2")}}, v)

	// replace individual record
	err = kv.Replace(ctx, "t1", BuildKey("p1", 2), []byte("value2+2"))
	require.NoError(t, err)

	it, err = kv.Read(ctx, "t1", BuildKey("p1", 2))
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []KeyValue{{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(2))), Value: []byte("value2+2")}}, v)

	// read range
	it, err = kv.ReadRange(ctx, "t1", BuildKey("p1", 2), BuildKey("p1", 4))
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(2))), Value: []byte("value2+2")},
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(3))), Value: []byte("value3")},
	}, v)

	// update range
	i := 3
	err = kv.UpdateRange(ctx, "t1", BuildKey("p1", 3), BuildKey("p1", 6), func(orig []byte) ([]byte, error) {
		require.Equal(t, fmt.Sprintf("value%d", i), string(orig))
		res := []byte(fmt.Sprintf("value%d+%d", i, i))
		i++
		return res, nil
	})
	require.NoError(t, err)

	it, err = kv.ReadRange(ctx, "t1", BuildKey("p1", 3), BuildKey("p1", 6))
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(3))), Value: []byte("value3+3")},
		{Key: BuildKey("p1", int64(4)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(4))), Value: []byte("value4+4")},
		{Key: BuildKey("p1", int64(5)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(5))), Value: []byte("value5+5")},
	}, v)

	// prefix read
	it, err = kv.Read(ctx, "t1", BuildKey("p1"))
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(1)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(1))), Value: []byte("value1")},
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(2))), Value: []byte("value2+2")},
		{Key: BuildKey("p1", int64(3)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(3))), Value: []byte("value3+3")},
		{Key: BuildKey("p1", int64(4)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(4))), Value: []byte("value4+4")},
		{Key: BuildKey("p1", int64(5)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(5))), Value: []byte("value5+5")},
	}, v)

	// delete and delete range
	err = kv.Delete(ctx, "t1", BuildKey("p1", 1))
	require.NoError(t, err)

	err = kv.DeleteRange(ctx, "t1", BuildKey("p1", 3), BuildKey("p2", 6))
	require.NoError(t, err)

	it, err = kv.ReadRange(ctx, "t1", BuildKey("p1", 1), BuildKey("p1", 6))
	require.NoError(t, err)

	v = readAll(t, it)
	require.Equal(t, []KeyValue{
		{Key: BuildKey("p1", int64(2)), FDBKey: getFDBKey("t1", BuildKey("p1", int64(2))), Value: []byte("value2+2")},
	}, v)

	err = kv.DropTable(ctx, "t1")
	require.NoError(t, err)
}

/*
type keyRange struct {
	left  Key
	right Key
}
*/

type kvTestCase struct {
	name   string
	insert []KeyValue
	test   []KeyValue
	//	keyRange keyRange
	result []KeyValue
	err    error
}

func testKVInsert(t *testing.T, kv KV) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cases := []kvTestCase{
		{
			name:   "simple",
			test:   []KeyValue{{BuildKey("p1"), nil, []byte("value1")}},
			result: []KeyValue{{BuildKey("p1"), getFDBKey("t1", BuildKey("p1")), []byte("value1")}},
		},
		{
			name:   "conflict",
			insert: []KeyValue{{BuildKey("p1"), nil, []byte("value1")}},
			test:   []KeyValue{{BuildKey("p1"), nil, []byte("value2")}},
			result: []KeyValue{{BuildKey("p1"), getFDBKey("t1", BuildKey("p1")), []byte("value1")}},
			err:    os.ErrExist,
		},
	}

	for _, v := range cases {
		err := kv.DropTable(ctx, "t1")
		require.NoError(t, err)

		err = kv.CreateTable(ctx, "t1")
		require.NoError(t, err)

		t.Run(v.name, func(t *testing.T) {
			for _, i := range v.insert {
				err := kv.Insert(context.TODO(), "t1", i.Key, i.Value)
				require.NoError(t, err)
			}
			for _, i := range v.test {
				err := kv.Insert(context.TODO(), "t1", i.Key, i.Value)
				if v.err != nil {
					require.EqualError(t, err, v.err.Error())
				} else {
					require.NoError(t, err)
				}
			}
			for _, i := range v.result {
				it, err := kv.Read(context.Background(), "t1", i.Key)
				require.NoError(t, err)
				require.True(t, it.More())
				res, err := it.Next()
				require.NoError(t, err)
				require.Equal(t, i, *res)
				require.True(t, !it.More())
			}
		})
	}

	t.Run("test_kv_timeout", func(t *testing.T) {
		testKVTimeout(t, kv)
	})

	err := kv.DropTable(ctx, "t1")
	require.NoError(t, err)
}

func testKVTimeout(t *testing.T, kv KV) {
	ctx, cancel1 := context.WithTimeout(context.Background(), 3*time.Millisecond)
	defer cancel1()

	tx, err := kv.Tx(ctx)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	err = tx.Commit(context.Background())
	assert.Error(t, err)

	tx, err = kv.Tx(context.Background())
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	err = tx.Commit(context.Background())
	assert.NoError(t, err)

	ctx, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-3*time.Millisecond))
	defer cancel2()
	_, err = kv.Tx(ctx)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestKVFDB(t *testing.T) {
	cfg, err := config.GetTestFDBConfig()
	require.NoError(t, err)

	kv, err := NewFoundationDB(cfg)
	require.NoError(t, err)

	t.Run("TestKVFDBBasic", func(t *testing.T) {
		testKVBasic(t, kv)
	})
	t.Run("TestKVFDBInsert", func(t *testing.T) {
		testKVInsert(t, kv)
	})
}

func TestGetCtxTimeout(t *testing.T) {
	//FIXME: time.Now dependent, may be flaky on slow machine
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
