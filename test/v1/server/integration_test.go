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
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

var (
	testDB         string
	testCollection = "test_collection"
)

type (
	Map map[string]interface{}
	Doc Map
)

var testCreateSchema = Map{
	"schema": Map{
		"title":       testCollection,
		"description": "this schema is for integration tests",
		"properties": Map{
			"pkey_int": Map{
				"description": "primary key field",
				"type":        "integer",
			},
			"int_value": Map{
				"description": "simple int field",
				"type":        "integer",
			},
			"string_value": Map{
				"description": "simple string field",
				"type":        "string",
				"maxLength":   128,
			},
			"added_string_value": Map{
				"description": "simple string field",
				"type":        "string",
			},
			"bool_value": Map{
				"description": "simple boolean field",
				"type":        "boolean",
			},
			"double_value": Map{
				"description": "simple double field",
				"type":        "number",
			},
			"added_value_double": Map{
				"description": "simple double field",
				"type":        "number",
			},
			"bytes_value": Map{
				"description": "simple bytes field",
				"type":        "string",
				"format":      "byte",
			},
			"uuid_value": Map{
				"description": "uuid field",
				"type":        "string",
				"format":      "uuid",
			},
			"date_time_value": Map{
				"description": "date time field",
				"type":        "string",
				"format":      "date-time",
			},
			"simple_array_value": Map{
				"description": "array field",
				"type":        "array",
				"items": Map{
					"type": "string",
				},
			},
			"array_value": Map{
				"description": "array field",
				"type":        "array",
				"items": Map{
					"type": "object",
					"properties": Map{
						"id": Map{
							"type": "integer",
						},
						"product": Map{
							"type": "string",
						},
					},
				},
			},
			"object_value": Map{
				"description": "object field",
				"type":        "object",
				"properties": Map{
					"name": Map{
						"type": "string",
					},
					"bignumber": Map{
						"type": "integer",
					},
				},
			},
		},
		"primary_key": []interface{}{"pkey_int"},
	},
}

func setupTestsOnlyProject(t *testing.T) string {
	proj := fmt.Sprintf("integration_%s", t.Name())
	deleteProject(t, proj)
	createProject(t, proj).Status(http.StatusOK)

	return proj
}

func setupTestsProjectAndSearchIndex(t *testing.T) (string, string) {
	proj := fmt.Sprintf("integration_%s", t.Name())
	deleteProject(t, proj)
	createProject(t, proj).Status(http.StatusOK)
	createSearchIndex(t, proj, testIndex, testSearchIndexSchema).Status(http.StatusOK)
	return proj, testIndex
}

func setupTests(t *testing.T) (string, string) {
	db := fmt.Sprintf("integration_%s", t.Name())
	deleteProject(t, db)
	createProject(t, db).Status(http.StatusOK)
	createCollection(t, db, testCollection, testCreateSchema).Status(http.StatusOK)

	return db, testCollection
}

func cleanupTests(t *testing.T, db string) {
	deleteProject(t, db).Status(http.StatusOK)
}

func expectLow(s httpexpect.LoggerReporter, url string) *httpexpect.Expect {
	return httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  url,
		Reporter: httpexpect.NewAssertReporter(s),
	})
}

func expect(s httpexpect.LoggerReporter) *httpexpect.Expect {
	return expectLow(s, config.GetBaseURL())
}

func expectRealtime(s httpexpect.LoggerReporter) *httpexpect.Expect {
	return expectLow(s, config.GetBaseRealtimeURL())
}

func getDocumentURL(databaseName, collectionName string, methodName string) string {
	return fmt.Sprintf("/v1/projects/%s/database/collections/%s/documents/%s", databaseName, collectionName, methodName)
}

func getCollectionURL(databaseName, collectionName string, methodName string) string {
	return fmt.Sprintf("/v1/projects/%s/database/collections/%s/%s", databaseName, collectionName, methodName)
}

func createCollection(t *testing.T, database string, collection string, schema map[string]interface{}) *httpexpect.Response {
	e := expect(t)
	return e.POST(getCollectionURL(database, collection, "createOrUpdate")).
		WithJSON(schema).
		Expect()
}

func createTestCollection(t *testing.T, database string, collection string, schema map[string]interface{}) {
	deleteProject(t, database)
	createProject(t, database)
	dropCollection(t, database, collection)
	createCollection(t, database, collection, schema).Status(http.StatusOK)
}

func describeCollection(t *testing.T, database string, collection string, schema map[string]interface{}) *httpexpect.Response {
	e := expect(t)
	return e.POST(getCollectionURL(database, collection, "describe")).
		WithJSON(schema).
		Expect()
}

func dropCollection(t *testing.T, database string, collection string) *httpexpect.Response {
	e := expect(t)
	return e.DELETE(getCollectionURL(database, collection, "drop")).
		Expect()
}

func testError(resp *httpexpect.Response, status int, code api.Code, message string) {
	resp.Status(status).
		JSON().Path("$.error").Object().
		ValueEqual("message", message).ValueEqual("code", api.CodeToString(code))
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	os.Exit(m.Run())
}
