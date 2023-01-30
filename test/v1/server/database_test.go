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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"gopkg.in/gavv/httpexpect.v1"
)

func getDatabaseURL(databaseName string, methodName string) string {
	return fmt.Sprintf("/v1/projects/%s/database/%s", databaseName, methodName)
}

func getProjectURL(projectName string, methodName string) string {
	return fmt.Sprintf("/v1/projects/%s/%s", projectName, methodName)
}

func beginTransactionURL(databaseName string) string {
	return fmt.Sprintf("/v1/projects/%s/database/transactions/begin", databaseName)
}

func TestCreateDatabase(t *testing.T) {
	deleteProject(t, "test_db")
	resp := createProject(t, "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "project created successfully")
}

func TestCreateDatabaseInvalidName(t *testing.T) {
	invalidDbNames := []string{"", "$testdb", "testdb$", "test$db"}
	for _, name := range invalidDbNames {
		resp := createProject(t, name)
		resp.Status(http.StatusBadRequest).
			JSON().
			Path("$.error").
			Object().
			ValueEqual("message", "invalid database name")
	}
}

func TestCreateDatabaseValidName(t *testing.T) {
	validDbNames := []string{"test-coll", "test_coll"}
	for _, name := range validDbNames {
		deleteProject(t, name)
		resp := createProject(t, name)
		resp.Status(http.StatusOK)
	}
}

func TestBeginTransaction(t *testing.T) {
	resp := beginTransaction(t, "test_db")
	cookieVal := resp.Cookie("Tigris-Tx-Id").Value()
	require.NotNil(t, t, cookieVal)
}

func TestDescribeDatabase(t *testing.T) {
	createCollection(t, "test_db", "test_collection", testCreateSchema).Status(http.StatusOK)
	resp := describeDatabase(t, "test_db", Map{})
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("size", 0)
}

func TestDropDatabase_NotFound(t *testing.T) {
	resp := deleteProject(t, "test_project_not_found")
	testError(resp, http.StatusNotFound, api.Code_NOT_FOUND, "project doesn't exist 'test_project_not_found'")
}

func TestDropDatabase(t *testing.T) {
	resp := deleteProject(t, "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "project deleted successfully")
}

func createProject(t *testing.T, projectName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(getProjectURL(projectName, "create")).
		Expect()
}

func beginTransaction(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(beginTransactionURL(databaseName)).
		Expect()
}

func deleteProject(t *testing.T, projectName string) *httpexpect.Response {
	e := expect(t)
	return e.DELETE(getProjectURL(projectName, "delete")).
		Expect()
}

func describeDatabase(t *testing.T, databaseName string, req Map) *httpexpect.Response {
	e := expect(t)
	return e.POST(getDatabaseURL(databaseName, "describe")).WithJSON(req).Expect()
}
