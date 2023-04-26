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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

func TestCompression(t *testing.T) {
	cfg, err := config.GetTestFDBConfig("../..")
	require.NoError(t, err)

	ctx := context.Background()
	table := []byte("t1")

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
	chunkSize = KB
	data := internal.NewTableData(doc)
	cases := []struct {
		compression bool
		chunking    bool
	}{
		{
			true,
			true,
		}, {
			true,
			false,
		}, {
			false,
			false,
		},
	}
	for _, c := range cases {
		builder := NewBuilder()
		if c.chunking {
			builder.WithChunking()
		}
		if c.compression {
			builder.WithCompression()
		}

		store, err := builder.Build(cfg)
		require.NoError(t, err)
		require.NoError(t, store.DropTable(ctx, table))
		require.NoError(t, store.CreateTable(ctx, table))
		tx, err := store.BeginTx(ctx)
		require.NoError(t, err)

		require.NoError(t, tx.Insert(ctx, table, BuildKey("p1_", 1), data))
		require.NoError(t, tx.Commit(ctx))

		tx, err = store.BeginTx(ctx)
		require.NoError(t, err)
		it, err := tx.Read(ctx, table, BuildKey("p1_", 1), false)
		require.NoError(t, err)

		found := 0
		totalExp := 1
		var keyValue KeyValue
		for it.Next(&keyValue) {
			require.Equal(t, doc, keyValue.Data.RawData)
			found++
		}
		if c.compression {
			require.True(t, keyValue.Data.Compression != nil)
		} else {
			require.True(t, keyValue.Data.Compression == nil)
		}
		require.Equal(t, totalExp, found)
		require.NoError(t, tx.Commit(ctx))
	}
}
