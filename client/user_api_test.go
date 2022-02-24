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
	"testing"

	"github.com/tigrisdata/tigrisdb/server/config"
)

func getTestServerHostPort() (string, int16) {
	config.LoadEnvironment() // Move this to test.Main

	if config.GetEnvironment() == config.EnvTest {
		return "tigris_server", 8081
	}
	return "localhost", 8081
}

func TestAPIGRPC(t *testing.T) {
	/*
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		h, p := getTestServerHostPort()
		c, err := newGRPCClient(ctx, h, p)
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

		inputDocuments := []*api.Document{
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
			Keys: []*api.Document{
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
		require.Equal(t, len(inputDocuments), totalReceivedDocuments)

		_, err = c.DropCollection(ctx, &api.DropCollectionRequest{Db: "db1", Collection: "t1"})
		require.NoError(t, err)

		err = c.Close()
		require.NoError(t, err)

	*/
}

/*
type readResponse struct {
	Result *userHTTP.ReadResponse
}
*/

func TestAPIHTTP(t *testing.T) {
	/*
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		h, p := getTestServerHostPort()
		c, err := newHTTPClient(ctx, h, p)
		require.NoError(t, err)

		_, _ = c.TigrisDBDropCollectionWithResponse(ctx, "db1", "t1")

		values, err := structpb.NewValue([]interface{}{"pkey_int"})
		require.NoError(t, err)
		_, err = c.TigrisDBCreateCollectionWithResponse(ctx, "db1", "t1", userHTTP.TigrisDBCreateCollectionJSONRequestBody{
			Schema: &map[string]interface{}{
				"primary_key": values,
			},
		})
		require.NoError(t, err)

		inputDocuments := []userHTTP.Document{
			{
				Doc: &map[string]interface{}{
					"pkey_int":  1,
					"int_value": 2,
					"str_value": "foo",
				},
			},
		}
		_, err = c.TigrisDBInsertWithResponse(ctx, "db1", "t1", userHTTP.TigrisDBInsertJSONRequestBody{
			Documents: &inputDocuments,
		})
		require.NoError(t, err)

		resp, err := c.TigrisDBRead(ctx, "db1", "t1", userHTTP.TigrisDBReadJSONRequestBody{
			Keys: &[]userHTTP.Document{
				{
					Doc: &map[string]interface{}{
						"pkey_int": 1,
					},
				},
			}})
		require.NoError(t, err)

		require.False(t, resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated)

		i := 0
		dec := json.NewDecoder(resp.Body)

		for dec.More() {
			td := readResponse{}
			err := dec.Decode(&td)
			require.NoError(t, err)
			log.Debug().Interface("value", td.Result).Msg("Read response")
			res, err := json.Marshal(td.Result.Doc)
			require.NoError(t, err)
			exp, err := json.Marshal(inputDocuments[i].Doc)
			require.NoError(t, err)
			require.JSONEq(t, string(exp), string(res))
			i++
		}

		require.Equal(t, len(inputDocuments), i)

		_, err = c.TigrisDBDropCollectionWithResponse(ctx, "db1", "t1")
		require.NoError(t, err)

		err = c.Close()
		require.NoError(t, err)
	*/
}
