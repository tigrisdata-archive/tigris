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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

func TestChunkStore(t *testing.T) {
	cfg, err := config.GetTestFDBConfig("../..")
	require.NoError(t, err)

	kv, err := newFoundationDB(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	table := []byte("t1")
	require.NoError(t, kv.DropTable(ctx, table))
	require.NoError(t, kv.CreateTable(ctx, table))

	store, err := NewTxStore(kv)
	require.NoError(t, err)

	chunkStore := NewChunkStore(store)

	doc := []byte(`{
    "a": 1,
    "b": 10.2,
    "c": "foo",
    "d": "bar",
    "e": "The fun begins starting here",
    "f": "This is again a new line that has data",
    "g": "what about a new which has all the necessary information that we need to form something",
    "record": {
        "browser": "Microsoft Edge",
        "geo_coordinates": {
            "IPv4": "56.235.92.239",
            "city": "San Diego",
            "countryCode": "PN",
            "state": "Virginia",
            "countryName": "Algeria"
        },
        "user_id": "8b880277-7b2c-4258-8f00-bd6b61549f2a",
        "labels": [
            "noOIOiI",
            "NcWMBI"
        ],
        "timestamp": 492962963,
        "vendor": "Housefax",
        "platform": "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/5340 (KHTML, like Gecko) Chrome/36.0.815.0 Mobile Safari/5340",
        "hostname": "directout-of-the-box.io",
        "capturedSessionState": "LRZVl",
        "device": "Opera/8.21 (X11; Linux x86_64; en-US) Presto/2.9.334 Version/10.00",
        "entry_url": "https://www.centralunleash.biz/mission-critical/efficient",
        "language": "Tahitian"
    },
    "another_record": {
        "device": "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4 rv:5.0; en-US) AppleWebKit/532.37.2 (KHTML, like Gecko) Version/4.2 Safari/532.37.2",
        "geo_coordinates": {
            "city": "San Diego",
            "countryName": "Northern Mariana Islands",
            "state": "South Carolina",
            "IPv4": "10.206.89.119",
            "countryCode": "LI"
        },
        "hostname": "dynamicorchestrate.io",
        "user_id": "95322585-765a-44c8-afae-ffc33d0942e4",
        "entry_url": "https://www.districtredefine.name/reintermediate/b2b/facilitate",
        "platform": "Mozilla/5.0 (Windows 98; Win 9x 4.90) AppleWebKit/5341 (KHTML, like Gecko) Chrome/37.0.820.0 Mobile Safari/5341",
        "labels": [
            "QNUvCwIEg",
            "bHVLddOx"
        ],
        "capturedSessionState": "MCzGd",
        "language": "Malayalam",
        "vendor": "PeerJ",
        "timestamp": 582254242,
        "browser": "Brave"
    }
}`)
	data := internal.NewTableData(doc)

	cases := []struct {
		chunkSize   int
		totalChunks int32
	}{
		{
			// chunkSize = exact size of the data + 1
			2125,
			1,
		},

		{
			// chunkSize = exact size of the data
			2124,
			1,
		},
		{
			// chunkSize = exact size of the data - 1
			2123,
			2,
		},
		{
			KB,
			3,
		},
		{
			KB,
			3,
		},
		{
			KB / 2,
			5,
		},
		{
			KB / 4,
			9,
		},
		{
			KB / 10,
			22,
		},
	}
	for _, c := range cases {
		chunkSize = c.chunkSize

		tx, err := chunkStore.BeginTx(ctx)
		require.NoError(t, err)

		err = tx.Replace(ctx, table, BuildKey("p1_", 1), data, false)
		require.NoError(t, err)
		_ = tx.Commit(ctx)
		if c.totalChunks == 1 {
			require.Nil(t, data.TotalChunks)
		} else {
			require.True(t, data.IsChunkedData())
			require.Equal(t, c.totalChunks, *data.TotalChunks)
		}

		tx, err = chunkStore.BeginTx(ctx)
		require.NoError(t, err)
		it, err := tx.Read(ctx, table, BuildKey("p1_", 1))
		require.NoError(t, err)

		found := 0
		totalExp := 1
		var keyValue KeyValue
		for it.Next(&keyValue) {
			require.Equal(t, doc, keyValue.Data.RawData)
			found++
		}
		require.Equal(t, totalExp, found)
		_ = tx.Commit(ctx)
	}
}

func TestChunkStoreIterator(t *testing.T) {
	ptr1, ptr2 := int32(1), int32(2)
	cases := []struct {
		expValues []*KeyValue
		expError  error
		expCall   int
	}{
		{
			[]*KeyValue{{Key: BuildKey("k1"), Data: &internal.TableData{RawData: []byte(`{}`)}}},
			nil,
			1,
		}, {
			[]*KeyValue{{Key: BuildKey("k2"), Data: &internal.TableData{RawData: []byte(`{}`), TotalChunks: &ptr1}}},
			nil,
			1,
		}, {
			[]*KeyValue{{Key: BuildKey("k3"), Data: &internal.TableData{RawData: []byte(`{}`), TotalChunks: &ptr2}}},
			fmt.Errorf("mismatch in total chunk read '1' versus total chunks expected '2'"),
			0,
		}, {
			[]*KeyValue{
				{Key: BuildKey("k4"), Data: &internal.TableData{RawData: []byte(`{}`), TotalChunks: &ptr2}},
				{Key: BuildKey("k4"), Data: &internal.TableData{RawData: []byte(`{}`)}},
			},
			fmt.Errorf("key shorter than expected chunked key '[k4]'"),
			0,
		}, {
			[]*KeyValue{
				{Key: BuildKey("k5"), Data: &internal.TableData{RawData: []byte(`{}`), TotalChunks: &ptr2}},
				{Key: BuildKey("k5", "something", "random"), Data: &internal.TableData{RawData: []byte(`{}`)}},
			},
			fmt.Errorf("chunk identifier not found in the key '[k5 something random]'"),
			0,
		}, {
			[]*KeyValue{
				{Key: BuildKey("k6"), Data: &internal.TableData{RawData: []byte(`{}`), TotalChunks: &ptr2}},
				{Key: BuildKey("k6", "_C_", "random"), Data: &internal.TableData{RawData: []byte(`{}`)}},
			},
			fmt.Errorf("chunk number mismatch found: 'random' exp: '1'"),
			0,
		},
	}
	for _, c := range cases {
		it := &ChunkIterator{
			Iterator: &mockedIterator{
				values: c.expValues,
			},
		}

		times := 0
		var keyValue KeyValue
		for it.Next(&keyValue) {
			times++
		}
		require.Equal(t, c.expError, it.err)
		require.Equal(t, c.expCall, times)
	}
}

type mockedIterator struct {
	idx    int
	values []*KeyValue
}

func (it *mockedIterator) Next(value *KeyValue) bool {
	if it.idx >= len(it.values) {
		return false
	}

	hasNext := it.idx < len(it.values)
	value.Key = it.values[it.idx].Key
	value.Data = it.values[it.idx].Data
	it.idx++

	return hasNext
}

func (it *mockedIterator) Err() error { return nil }
