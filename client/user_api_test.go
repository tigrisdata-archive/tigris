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
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAPIGRPC(t *testing.T) {
	ctx := context.TODO()

	c, err := newGRPCClient(ctx, "localhost", 8081)
	require.NoError(t, err)

	_, _ = c.DropCollection(ctx, &api.DropCollectionRequest{Db: "db1", Collection: "t1"})

	values, err := structpb.NewValue([]interface{}{"pkey_int"})
	require.NoError(t, err)
	_, err = c.CreateCollection(ctx, &api.CreateCollectionRequest{
		Db:         "db1",
		Collection: "t1",
		Schema: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"primary_key": values,
			},
		},
	})
	require.NoError(t, err)

	inputDocuments := []*api.UserDocument{
		{
			Doc: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"pkey_int":  structpb.NewNumberValue(1),
					"int_value": structpb.NewNumberValue(2),
					"str_value": structpb.NewStringValue("foo"),
				},
			},
		},
	}
	_, err = c.Insert(ctx, &api.InsertRequest{
		Db:         "db1",
		Collection: "t1",
		Documents:  inputDocuments,
	})
	require.NoError(t, err)

	rc, err := c.Read(ctx, &api.ReadRequest{
		Db:         "db1",
		Collection: "t1",
		Keys: []*api.UserDocument{
			{
				Doc: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"pkey_int": structpb.NewNumberValue(1),
					},
				},
			},
		}})
	require.NoError(t, err)

	totalReceivedDocuments := 0
	for {
		d, err := rc.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		log.Debug().Str("value", d.Doc.String()).Msg("Read")
		require.EqualValues(t, inputDocuments[0].Doc.AsMap(), d.Doc.AsMap())
		totalReceivedDocuments++
	}
	require.Equal(t, totalReceivedDocuments, len(inputDocuments))

	_, err = c.DropCollection(ctx, &api.DropCollectionRequest{Db: "db1", Collection: "t1"})
	require.NoError(t, err)

	err = c.Close()
	require.NoError(t, err)
}
