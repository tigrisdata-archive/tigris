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

//go:build integration

package server

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

func TestInfo(t *testing.T) {
	resp := info(t)

	resp.Status(http.StatusOK).
		JSON().
		Object().
		Value("server_version").NotNull()
}

func info(t *testing.T) *httpexpect.Response {
	e := httpexpect.New(t, config.GetBaseURL())
	return e.GET(config.GetBaseURL() + "/v1/observability/info").
		Expect()
}

func TestTxForwarder(t *testing.T) {
	t.Skip("tigris_server2 requires authenticated requests")
	token, found := os.LookupEnv("TEST_AUTH_TOKEN")
	require.True(t, found)

	e1 := expectLow(t, config.GetBaseURL())
	e2 := expectLow(t, config.GetBaseURL2())

	r, err := http.DefaultClient.Get(config.GetBaseURL2() + "/v1/health")
	if err != nil || r.StatusCode != http.StatusOK {
		t.Skipf("server at %s is not available", config.GetBaseURL2())
	}
	assert.NoError(t, r.Body.Close())

	dbName := "db_" + t.Name()
	collName := "test_collection"
	createTestCollection(t, dbName, collName, testCreateSchema)

	r1 := e1.POST(fmt.Sprintf("/v1/projects/%s/database/transactions/begin", dbName)).
		Expect().Status(http.StatusOK).
		Body().Raw()

	res1 := struct {
		TxCtx api.TransactionCtx `json:"tx_ctx"`
	}{}

	err = jsoniter.Unmarshal([]byte(r1), &res1)
	require.NoError(t, err)

	e2.POST(getDocumentURL(dbName, collName, "insert")).
		WithJSON(Map{"documents": []Doc{{"pkey_int": 1}, {"pkey_int": 3}, {"pkey_int": 5}, {"pkey_int": 7}}}).
		WithHeader("Tigris-Tx-Id", res1.TxCtx.Id).
		WithHeader("Tigris-Tx-Origin", res1.TxCtx.Origin).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	// direct request to the original server
	e1.POST(getDocumentURL(dbName, collName, "insert")).
		WithJSON(Map{"documents": []Doc{{"pkey_int": 2}}}).
		WithHeader("Tigris-Tx-Id", res1.TxCtx.Id).
		WithHeader("Tigris-Tx-Origin", res1.TxCtx.Origin).
		WithHeader(Authorization, Bearer+token).
		Expect().Status(http.StatusOK).JSON().Object().ValueEqual("status", "inserted")

	e2.PUT(getDocumentURL(dbName, collName, "replace")).
		WithJSON(Map{"documents": []Doc{{"pkey_int": 4}}}).
		WithHeader("Tigris-Tx-Id", res1.TxCtx.Id).
		WithHeader("Tigris-Tx-Origin", res1.TxCtx.Origin).
		WithHeader(Authorization, Bearer+token).
		Expect().Status(http.StatusOK).JSON().Object().ValueEqual("status", "replaced")

	e2.DELETE(getDocumentURL(dbName, collName, "delete")).
		WithJSON(Map{"filter": Map{"pkey_int": 5}}).
		WithHeader("Tigris-Tx-Id", res1.TxCtx.Id).
		WithHeader("Tigris-Tx-Origin", res1.TxCtx.Origin).
		WithHeader(Authorization, Bearer+token).
		Expect().Status(http.StatusOK).
		JSON().Object().ValueEqual("status", "deleted")

	e2.PUT(getDocumentURL(dbName, collName, "update")).
		WithJSON(Map{"filter": Map{"pkey_int": 7}, "fields": Map{"$set": Map{"int_value": 70}}}).
		WithHeader("Tigris-Tx-Id", res1.TxCtx.Id).
		WithHeader("Tigris-Tx-Origin", res1.TxCtx.Origin).
		WithHeader(Authorization, Bearer+token).
		Expect().Status(http.StatusOK).
		JSON().Object().ValueEqual("status", "updated")

	str := e2.POST(getDocumentURL(dbName, collName, "read")).
		WithJSON(Map{
			"filter": Map{},
		}).
		WithHeader("Tigris-Tx-Id", res1.TxCtx.Id).
		WithHeader("Tigris-Tx-Origin", res1.TxCtx.Origin).
		WithHeader(Authorization, Bearer+token).
		Expect().Status(http.StatusOK).Body().Raw()

	dec := jsoniter.NewDecoder(bytes.NewReader([]byte(str)))
	res := []int32{1, 2, 3, 4, 7}
	for i := 0; dec.More(); i++ {
		var mp map[string]map[string]jsoniter.RawMessage
		require.NoError(t, dec.Decode(&mp))
		if i != 4 {
			assert.Equal(t, fmt.Sprintf(`{"pkey_int":%d}`, res[i]), string(mp["result"]["data"]))
		} else {
			assert.Equal(t, fmt.Sprintf(`{"pkey_int":%d,"int_value":70}`, res[i]), string(mp["result"]["data"]))
		}
	}

	e2.POST(fmt.Sprintf("/v1/projects/%s/database/transactions/commit", dbName)).
		WithHeader("Tigris-Tx-Id", res1.TxCtx.Id).
		WithHeader("Tigris-Tx-Origin", res1.TxCtx.Origin).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK)
}
