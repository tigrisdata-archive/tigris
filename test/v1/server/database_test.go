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

//go:build integration

package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"gopkg.in/gavv/httpexpect.v1"
)

func getDatabaseURL(databaseName string, methodName string) string {
	return fmt.Sprintf("/api/v1/databases/%s/%s", databaseName, methodName)
}

func beginTransactionURL(databaseName string) string {
	return fmt.Sprintf("/api/v1/databases/%s/transactions/begin", databaseName)
}

func TestCreateDatabase(t *testing.T) {
	resp := createDatabase(t, "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "database created successfully")
}

func TestBeginTransaction(t *testing.T) {
	resp := beginTransaction(t, "test_db")
	cookieVal := resp.Cookie("Tigris-Tx-Id").Value()
	require.NotNil(t, t, cookieVal)
}

func TestDescribeDatabase(t *testing.T) {
	createCollection(t, "test_db", "test_collection", testCreateSchema).Status(http.StatusOK)
	resp := describeDatabase(t, "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("db", "test_db").
		ValueEqual("size", 0)
}

func TestDropDatabase_NotFound(t *testing.T) {
	resp := dropDatabase(t, "test_drop_db_not_found")
	testError(resp, http.StatusNotFound, api.Code_NOT_FOUND, "database doesn't exist 'test_drop_db_not_found'")
}

func TestDropDatabase(t *testing.T) {
	resp := dropDatabase(t, "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "database dropped successfully")
}

func createDatabase(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(getDatabaseURL(databaseName, "create")).
		Expect()
}

func beginTransaction(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(beginTransactionURL(databaseName)).
		Expect()
}

func dropDatabase(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.DELETE(getDatabaseURL(databaseName, "drop")).
		Expect()
}

func describeDatabase(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(getDatabaseURL(databaseName, "describe")).Expect()
}
