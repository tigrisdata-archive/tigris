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
	"net/http"
	"testing"

	api "github.com/tigrisdata/tigris/api/server/v1"
)

func TestCreateCollection(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	t.Run("status_400_empty_name", func(t *testing.T) {
		dropCollection(t, db, coll)

		resp := createCollection(t, db, "", nil)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "invalid collection name")
	})
	t.Run("status_400_schema_nil", func(t *testing.T) {
		dropCollection(t, db, coll)

		resp := createCollection(t, db, coll, nil)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "schema is a required during collection creation")
	})
	t.Run("status_success", func(t *testing.T) {
		dropCollection(t, db, coll)

		resp := createCollection(t, db, coll, testCreateSchema)
		resp.Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("message", "collection of type 'documents' created successfully")
	})
	t.Run("status_conflict", func(t *testing.T) {
		dropCollection(t, db, coll)

		createOrUpdateOptions := map[string]interface{}{
			"only_create": true,
		}
		for key, value := range testCreateSchema {
			createOrUpdateOptions[key] = value
		}

		e := expect(t)
		e.POST(getCollectionURL(db, coll, "createOrUpdate")).
			WithJSON(createOrUpdateOptions).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("message", "collection of type 'documents' created successfully")

		resp := e.POST(getCollectionURL(db, coll, "createOrUpdate")).
			WithJSON(createOrUpdateOptions).
			Expect()
		testError(resp, http.StatusConflict, api.Code_ALREADY_EXISTS, "collection already exist")
	})
}

func TestCreateCollectionInvalidName(t *testing.T) {
	invalidCollectionName := []string{"", "$testcoll", "testcoll$", "test$coll"}
	for _, name := range invalidCollectionName {
		resp := createCollection(t, "valid_db_name", name, testCreateSchema)
		resp.Status(http.StatusBadRequest).
			JSON().
			Path("$.error").
			Object().
			ValueEqual("message", "invalid collection name")
	}
}

func TestDropCollection(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	createCollection(t, db, coll, testCreateSchema).Status(http.StatusOK)

	resp := dropCollection(t, db, coll)
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "collection dropped successfully")

	// dropping again should return in a NOT FOUND error
	resp = dropCollection(t, db, coll)
	testError(resp, http.StatusNotFound, api.Code_NOT_FOUND, "collection doesn't exist 'test_collection'")
}

func TestDescribeCollection(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	createCollection(t, db, coll, testCreateSchema).Status(http.StatusOK)
	resp := describeCollection(t, db, coll, Map{})

	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("collection", coll).
		ValueEqual("size", 0)

	// cleanup
	dropCollection(t, db, coll)
}

func TestCollection_Update(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	dropCollection(t, db, coll)
	resp := createCollection(t, db, coll,
		Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"int_field": Map{
						"type": "integer",
					},
					"string_field": Map{
						"type": "string",
					},
				},
				"primary_key": []any{"int_field"},
			},
		})

	resp.Status(http.StatusOK)

	cases := []struct {
		name    string
		schema  Map
		expCode int
	}{
		{
			"primary key missing",
			Map{"schema": Map{"title": coll, "properties": Map{"int_field": Map{"type": "integer"}, "string_field": Map{"type": "string"}}}},
			http.StatusBadRequest,
		},
		/*
			Type change and field deletion allowed when config.DefaultConfig.Schema.AllowIncompatible is set
			{
				"type change",
				Map{"schema": Map{"title": coll, "properties": Map{"int_field": Map{"type": "string"}, "string_field": Map{"type": "string"}}, "primary_key": []any{"int_field"}}},
				http.StatusBadRequest,
			},
			{
				"field removed",
				Map{"schema": Map{"title": coll, "properties": Map{"int_field": Map{"type": "integer"}}, "primary_key": []any{"int_field"}}},
				http.StatusBadRequest,
			},
		*/
		{
			"success adding a field",
			Map{"schema": Map{"title": coll, "properties": Map{"int_field": Map{"type": "integer"}, "string_field": Map{"type": "string"}, "extra_field": Map{"type": "string"}}, "primary_key": []any{"int_field"}}},
			http.StatusOK,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			createCollection(t, db, coll, c.schema).Status(c.expCode)
		})
	}
}
