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

	"github.com/stretchr/testify/suite"
	"github.com/tigrisdata/tigrisdb/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

/**
{
	"name": "test_collection",
	"description": "this schema is for integration tests",
	"properties": {
		"pkey_int": {
			"description": "primary key field",
			"type": "int"
		},
		"int_value": {
			"description": "simple int field",
			"type": "int"
		},
		"string_value": {
			"description": "simple string field",
			"type": "string",
			"max_length": 128,
		},
		"bool_value": {
			"description": "simple bool field",
			"type": "bool"
		},
		"double_value": {
			"description": "simple double field",
			"type": "double"
		},
		"bytes_value": {
			"description": "simple bytes field",
			"type": "bytes"
		}
	},
	"primary_key": [
		"cust_id",
		"order_id"
	]
}
*/
var testCreateSchema = map[string]interface{}{
	"schema": map[string]interface{}{
		"name":        "test_collection",
		"description": "this schema is for integration tests",
		"properties": map[string]interface{}{
			"pkey_int": map[string]interface{}{
				"description": "primary key field",
				"type":        "int",
			},
			"int_value": map[string]interface{}{
				"description": "simple int field",
				"type":        "int",
			},
			"string_value": map[string]interface{}{
				"description": "simple string field",
				"type":        "string",
				"max_length":  128,
			},
			"bool_value": map[string]interface{}{
				"description": "simple bool field",
				"type":        "bool",
			},
			"double_value": map[string]interface{}{
				"description": "simple double field",
				"type":        "double",
			},
			"bytes_value": map[string]interface{}{
				"description": "simple bytes field",
				"type":        "bytes",
			},
		},
		"primary_key": []interface{}{"pkey_int"},
	},
}

type CollectionSuite struct {
	suite.Suite

	database string
}

func getCollectionURL(databaseName, collectionName string, methodName string) string {
	return fmt.Sprintf("/api/v1/databases/%s/collections/%s/%s", databaseName, collectionName, methodName)
}

func (s *CollectionSuite) SetupSuite() {
	// create the database for the collection test suite
	createDatabase(s.T(), s.database)
}

func (s *CollectionSuite) TearDownSuite() {
	// drop the database for the collection test suite
	dropDatabase(s.T(), s.database)

}

func (s *CollectionSuite) TestCreateCollection() {
	s.Run("status_400_empty_name", func() {
		dropCollection(s.T(), s.database, "test_collection")

		resp := createCollection(s.T(), s.database, "", nil)
		resp.Status(http.StatusBadRequest).
			JSON().
			Object().
			ValueEqual("message", "invalid collection name")
	})
	s.Run("status_400_schema_nil", func() {
		dropCollection(s.T(), s.database, "test_collection")

		resp := createCollection(s.T(), s.database, "test_collection", nil)
		resp.Status(http.StatusBadRequest).
			JSON().
			Object().
			ValueEqual("message", "schema is a required during collection creation")
	})
	s.Run("status_success", func() {
		dropCollection(s.T(), s.database, "test_collection")

		resp := createCollection(s.T(), s.database, "test_collection", testCreateSchema)
		resp.Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("msg", "collection created successfully")
	})
	s.Run("status_conflict", func() {
		dropCollection(s.T(), s.database, "test_collection")

		resp := createCollection(s.T(), s.database, "test_collection", testCreateSchema)
		resp.Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("msg", "collection created successfully")

		resp = createCollection(s.T(), s.database, "test_collection", testCreateSchema)
		resp.Status(http.StatusConflict).
			JSON().
			Object().
			ValueEqual("message", "collection already exists")
	})
}

func (s *CollectionSuite) TestAlterCollection() {}

func (s *CollectionSuite) TestDropCollection() {
	createCollection(s.T(), s.database, "test_collection", testCreateSchema)

	resp := dropCollection(s.T(), s.database, "test_collection")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("msg", "collection dropped successfully")
}

func createCollection(t *testing.T, database string, collection string, schema map[string]interface{}) *httpexpect.Response {
	e := httpexpect.New(t, config.GetBaseURL())
	return e.POST(getCollectionURL(database, collection, "create")).
		WithJSON(schema).
		Expect()
}

func dropCollection(t *testing.T, database string, collection string) *httpexpect.Response {
	e := httpexpect.New(t, config.GetBaseURL())
	return e.DELETE(getCollectionURL(database, collection, "drop")).
		Expect()
}
