package main

import (
	"context"
	"github.com/tigrisdata/tigrisdb/util"
	"io"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func getTestServerHost() string {
	util.LoadEnvironment() // Move this to test.Main

	if util.GetEnvironment() == util.EnvTest {
		return "tigris_server"
	}
	return "localhost"
}

func TestAPIGRPC(t *testing.T) {
	ctx := context.TODO()

	c, err := newGRPCClient(ctx, getTestServerHost(), 8081)
	require.NoError(t, err)

	_, _ = c.DropCollection(ctx, &api.DropCollectionRequest{Db: "db1", Collection: "t1"})

	values, err := structpb.NewValue([]interface{}{"pkey_int"})
	require.NoError(t, err)
	_, err = c.CreateCollection(ctx, &api.CreateCollectionRequest{Db: "db1", Collection: "t1", CreateBody: &api.CreateCollectionRequestBody{
		Schema: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"primary_key": values,
			},
		}},
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
		InsertBody: &api.InsertRequestBody{
			Documents: inputDocuments,
		},
	})
	require.NoError(t, err)

	rc, err := c.Read(ctx, &api.ReadRequest{Db: "db1", Collection: "t1", ReadBody: &api.ReadRequestBody{
		Keys: []*api.UserDocument{
			{
				Doc: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"pkey_int": structpb.NewNumberValue(1),
					},
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
