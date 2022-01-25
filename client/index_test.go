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

package main

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	indexHTTP "github.com/tigrisdata/tigrisdb/api/client/v1/index"
	userHTTP "github.com/tigrisdata/tigrisdb/api/client/v1/user"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
)

func TestAPIGRPC(t *testing.T) {
	ctx := context.TODO()

	//c, _ := NewGRPCClient(ctx, "localhost", 8082)
	c, _ := newGRPCClient(ctx, "server", 8082)

	_, _ = c.DropTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t1"})

	_, err := c.CreateTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t1"})
	require.NoError(t, err)

	_, err = c.Insert(ctx, &api.TigrisDBCRUDRequest{
		Db:    "db1",
		Table: "t1",
		Docs: []*api.TigrisDBDoc{
			{
				PrimaryKey:   []byte("aaa"),
				PartitionKey: []byte("a"),
				Value:        []byte(`{"bbb": "ccc"}`),
			},
		},
	})
	require.NoError(t, err)

	_, err = c.Update(ctx, &api.TigrisDBCRUDRequest{Db: "db1", Table: "t1", Docs: []*api.TigrisDBDoc{{PrimaryKey: []byte("aaa"), PartitionKey: []byte("a"), Value: []byte("bbb11111")}}})
	require.NoError(t, err)
	_, err = c.Upsert(ctx, &api.TigrisDBCRUDRequest{Db: "db1", Table: "t1", Docs: []*api.TigrisDBDoc{{PrimaryKey: []byte("bbb"), PartitionKey: []byte("b"), Value: []byte("bbb11111")}}})
	require.NoError(t, err)
	_, err = c.Replace(ctx, &api.TigrisDBCRUDRequest{Db: "db1", Table: "t1", Docs: []*api.TigrisDBDoc{{PrimaryKey: []byte("aaa"), PartitionKey: []byte("a"), Value: []byte(`{"bbb222222" : "uuuuu111"}`)}}})
	require.NoError(t, err)

	rc, err := c.Read(ctx, &api.TigrisDBCRUDRequest{Db: "db1", Table: "t1", Docs: []*api.TigrisDBDoc{{PrimaryKey: []byte("aaa"), PartitionKey: []byte("a")}}})
	require.NoError(t, err)

	for {
		d, err := rc.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		log.Debug().Str("value", string(d.Value)).Msg("Read")
	}

	_, err = c.Delete(ctx, &api.TigrisDBCRUDRequest{Db: "db1", Table: "t1", Docs: []*api.TigrisDBDoc{{PrimaryKey: []byte("aaa"), PartitionKey: []byte("a")}}})
	require.NoError(t, err)

	_, err = c.DropTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t1"})
	require.NoError(t, err)

	err = c.Close()
	require.NoError(t, err)
}

func TestAPIGRPCUpdatePrimaryIndex(t *testing.T) {
	ctx := context.TODO()

	//c, _ := NewGRPCClient(ctx, "localhost", 8082)
	c, _ := newGRPCClient(ctx, "server", 8082)
	ac, _ := NewAdminGRPCClient(ctx, "server", 8082)

	_, _ = c.DropTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t4"})

	_, err := c.CreateTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t4"})
	require.NoError(t, err)

	_, err = ac.PatchPrimaryIndex(ctx, &api.PatchPrimaryIndexRequest{
		Db:    "db1",
		Table: "t4",
		Entries: []*api.PatchIndexEntry{
			{
				PrimaryKey:   []byte("mmmm"),
				PartitionKey: []byte("mm"),
				Value: &api.MicroShardKey{
					FileId:    "fid1",
					Timestamp: time.Now().UnixNano(),
					Offset:    1111111,
					Length:    1111111,
				},
			},
			{
				PrimaryKey:   []byte("nnnnnn"),
				PartitionKey: []byte("nn"),
				Value: &api.MicroShardKey{
					FileId:    "fid2",
					Timestamp: time.Now().UnixNano(),
					Offset:    1111111,
					Length:    1111111,
				},
			},
		},
	})
	require.NoError(t, err)

	rc, err := c.Read(ctx, &api.TigrisDBCRUDRequest{Db: "db1", Table: "t4", Docs: []*api.TigrisDBDoc{{PrimaryKey: []byte("mmmm"), PartitionKey: []byte("mm")}}})
	require.NoError(t, err)

	for {
		d, err := rc.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		log.Debug().Str("value", string(d.Value)).Msg("ReadPrimaryIndex")
	}

	_, err = c.DropTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t4"})
	require.NoError(t, err)

	err = c.Close()
	require.NoError(t, err)
}

func TestAPIGRPCUpdateIndex(t *testing.T) {
	ctx := context.TODO()

	//c, _ := NewGRPCClient(ctx, "localhost", 8082)
	c, _ := newGRPCClient(ctx, "server", 8082)
	ac, _ := NewAdminGRPCClient(ctx, "server", 8082)

	_, _ = c.DropTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t3"})

	_, err := c.CreateTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t3"})
	require.NoError(t, err)

	_, err = ac.UpdateIndex(ctx, &api.UpdateIndexRequest{
		Db:    "db1",
		Table: "t3",
		Index: "clustering",
		New: []*api.ShardFile{
			{
				FileId:    "fid1",
				Timestamp: time.Now().UnixNano(),
				Shards: []*api.MicroShardKey{
					{
						MinKey: []byte("aaa"),
						MaxKey: []byte("aaa"),
						Offset: 1111111,
						Length: 1111111,
					},
					{
						MinKey: []byte("aaa"),
						MaxKey: []byte("aax"),
						Offset: 2222223,
					},
					{
						MinKey: []byte("aba"),
						MaxKey: []byte("abx"),
						Offset: 333333333,
					},
					{
						MinKey: []byte("aca"),
						MaxKey: []byte("aca"),
						Offset: 333333333,
					},
				},
			},
			{
				FileId:    "fid2",
				Timestamp: time.Now().UnixNano(),
				Shards: []*api.MicroShardKey{
					{
						MinKey: []byte("aca"),
						MaxKey: []byte("aca"),
						Offset: 1111111,
					},
					{
						MinKey: []byte("aca"),
						MaxKey: []byte("acx"),
						Offset: 2222223,
					},
					{
						MinKey: []byte("ada"),
						MaxKey: []byte("adx"),
						Offset: 333333333,
					},
					{
						MinKey: []byte("aea"),
						MaxKey: []byte("aex"),
						Offset: 333333333,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	rresp, err := ac.ReadIndex(ctx, &api.ReadIndexRequest{
		Db:     "db1",
		Table:  "t3",
		Index:  "clustering",
		MinKey: []byte("aaa"),
		MaxKey: []byte("adx"),
	})
	require.NoError(t, err)

	for _, v := range rresp.Shards {
		log.Error().Str("file_id", v.FileId).Int64("ts", v.Timestamp).Str("min_key", string(v.MinKey)).Str("max_key", string(v.MaxKey)).Uint64("offset", v.Offset).Msg("micro shard key")
	}

	_, err = c.DropTable(ctx, &api.TigrisDBRequest{Db: "db1", Table: "t3"})
	require.NoError(t, err)

	err = c.Close()
	require.NoError(t, err)
}

func byteSlice(ins string) *[]byte {
	in := []byte(ins)
	return &in
}

func TestAPIHTTPUpdateIndex(t *testing.T) {
	ctx := context.TODO()

	c, err := userHTTP.NewClientWithResponses("http://server:8081")
	require.NoError(t, err)

	_, _ = c.TigrisDBDropTableWithResponse(ctx, "db1", "t2", userHTTP.TigrisDBDropTableJSONRequestBody{})

	cresp, err := c.TigrisDBCreateTableWithResponse(ctx, "db1", "t2", userHTTP.TigrisDBCreateTableJSONRequestBody{})
	require.NoError(t, err)
	require.NotNil(t, cresp)
	require.Equal(t, cresp.StatusCode(), http.StatusOK)

	ac, err := indexHTTP.NewClientWithResponses("http://server:8081")
	require.NoError(t, err)

	resp2, err := ac.IndexAPIUpdateIndexWithResponse(ctx, "db1", "t2", "clustering", indexHTTP.IndexAPIUpdateIndexJSONRequestBody{
		New: &[]indexHTTP.ShardFile{
			{
				FileId:    aws.String("fid1"),
				Timestamp: aws.Int64(time.Now().UnixNano()),
				Shards: &[]indexHTTP.MicroShardKey{
					{
						MinKey: byteSlice("aaa"),
						MaxKey: byteSlice("aaa"),
						Offset: aws.Uint64(1111111),
						Length: aws.Uint64(55555),
					},
					{
						MinKey: byteSlice("aaa"),
						MaxKey: byteSlice("aax"),
						Offset: aws.Uint64(2222223),
					},
					{
						MinKey: byteSlice("aba"),
						MaxKey: byteSlice("abx"),
						Offset: aws.Uint64(333333333),
					},
					{
						MinKey: byteSlice("aca"),
						MaxKey: byteSlice("aca"),
						Offset: aws.Uint64(333333333),
					},
				},
			},
			{
				FileId:    aws.String("fid2"),
				Timestamp: aws.Int64(time.Now().UnixNano()),
				Shards: &[]indexHTTP.MicroShardKey{
					{
						MinKey: byteSlice("aca"),
						MaxKey: byteSlice("aca"),
						Offset: aws.Uint64(1111111),
					},
					{
						MinKey: byteSlice("aca"),
						MaxKey: byteSlice("acx"),
						Offset: aws.Uint64(2222223),
					},
					{
						MinKey: byteSlice("ada"),
						MaxKey: byteSlice("adx"),
						Offset: aws.Uint64(333333333),
					},
					{
						MinKey: byteSlice("aea"),
						MaxKey: byteSlice("aex"),
						Offset: aws.Uint64(333333333),
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp2)
	require.Equal(t, http.StatusOK, resp2.StatusCode())

	resp3, err := ac.IndexAPIReadIndexWithResponse(ctx, "db1", "t2", "clustering",
		indexHTTP.IndexAPIReadIndexJSONRequestBody{
			MinKey: byteSlice("aaa"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, resp3)
	require.Equal(t, http.StatusOK, resp3.StatusCode())
	require.NotNil(t, resp3.JSON200)

	for _, v := range *resp3.JSON200.Shards {
		log.Error().Str("file_id", aws.StringValue(v.FileId)).Int64("ts", aws.Int64Value(v.Timestamp)).Str("min_key", string(*v.MinKey)).Str("max_key", string(*v.MinKey)).Msg("micro shard key")
	}

	resp4, err := c.TigrisDBDropTableWithResponse(ctx, "db1", "t2", userHTTP.TigrisDBDropTableJSONRequestBody{})
	require.NoError(t, err)
	require.NotNil(t, resp4)
	require.Equal(t, http.StatusOK, resp4.StatusCode())
}
