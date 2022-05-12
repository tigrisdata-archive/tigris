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
	"gopkg.in/gavv/httpexpect.v1"
)

/**
{
	"title": "test_collection",
	"description": "this schema is for integration tests",
	"properties": {
		"pkey_int": {
			"description": "primary key field",
			"type": "integer"
		},
		"int_value": {
			"description": "simple integer field",
			"type": "integer"
		},
		"string_value": {
			"description": "simple string field",
			"type": "string",
			"maxLength": 128,
		},
		"bool_value": {
			"description": "simple boolean field",
			"type": "boolean"
		},
		"double_value": {
			"description": "simple double field",
			"type": "number"
		},
		"bytes_value": {
			"description": "simple bytes field",
			"type": "string",
			"format": "byte"
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
		"title":       "test_collection",
		"description": "this schema is for integration tests",
		"properties": map[string]interface{}{
			"pkey_int": map[string]interface{}{
				"description": "primary key field",
				"type":        "integer",
			},
			"int_value": map[string]interface{}{
				"description": "simple int field",
				"type":        "integer",
			},
			"string_value": map[string]interface{}{
				"description": "simple string field",
				"type":        "string",
				"maxLength":   128,
			},
			"added_string_value": map[string]interface{}{
				"description": "simple string field",
				"type":        "string",
			},
			"bool_value": map[string]interface{}{
				"description": "simple boolean field",
				"type":        "boolean",
			},
			"double_value": map[string]interface{}{
				"description": "simple double field",
				"type":        "number",
			},
			"added_value_double": map[string]interface{}{
				"description": "simple double field",
				"type":        "number",
			},
			"bytes_value": map[string]interface{}{
				"description": "simple bytes field",
				"type":        "string",
				"format":      "byte",
			},
			"uuid_value": map[string]interface{}{
				"description": "uuid field",
				"type":        "string",
				"format":      "uuid",
			},
			"date_time_value": map[string]interface{}{
				"description": "date time field",
				"type":        "string",
				"format":      "date-time",
			},
			"array_value": map[string]interface{}{
				"description": "array field",
				"type":        "array",
				"items": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"id": map[string]interface{}{
							"type": "integer",
						},
						"product": map[string]interface{}{
							"type": "string",
						},
					},
				},
			},
			"object_value": map[string]interface{}{
				"description": "object field",
				"type":        "object",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type": "string",
					},
				},
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
	dropDatabase(s.T(), s.database)
	createDatabase(s.T(), s.database).Status(http.StatusOK)
}

func (s *CollectionSuite) TearDownSuite() {
	// drop the database for the collection test suite
	dropDatabase(s.T(), s.database).Status(http.StatusOK)
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
			ValueEqual("message", "collection created successfully")
	})
	s.Run("status_conflict", func() {
		dropCollection(s.T(), s.database, "test_collection")

		var createOrUpdateOptions = map[string]interface{}{
			"only_create": true,
		}
		for key, value := range testCreateSchema {
			createOrUpdateOptions[key] = value
		}

		e := expect(s.T())
		e.POST(getCollectionURL(s.database, "test_collection", "createOrUpdate")).
			WithJSON(createOrUpdateOptions).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("message", "collection created successfully")

		e.POST(getCollectionURL(s.database, "test_collection", "createOrUpdate")).
			WithJSON(createOrUpdateOptions).
			Expect().
			Status(http.StatusConflict).
			JSON().
			Object().
			ValueEqual("message", "collection already exist")
	})
}

func (s *CollectionSuite) TestDropCollection() {
	createCollection(s.T(), s.database, "test_collection", testCreateSchema).Status(http.StatusOK)

	resp := dropCollection(s.T(), s.database, "test_collection")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "collection dropped successfully")

	// dropping again should return in a NOT FOUND error
	resp = dropCollection(s.T(), s.database, "test_collection")
	resp.Status(http.StatusNotFound).
		JSON().
		Object().
		ValueEqual("message", "collection doesn't exists 'test_collection'")
}

func (s *CollectionSuite) TestDescribeCollection() {
	createCollection(s.T(), s.database, "test_collection", testCreateSchema).Status(http.StatusOK)
	resp := describeCollection(s.T(), s.database, "test_collection", testCreateSchema)

	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("collection", "test_collection")

	// cleanup
	dropCollection(s.T(), s.database, "test_collection")
}

func createCollection(t *testing.T, database string, collection string, schema map[string]interface{}) *httpexpect.Response {
	e := expect(t)
	return e.POST(getCollectionURL(database, collection, "createOrUpdate")).
		WithJSON(schema).
		Expect()
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
