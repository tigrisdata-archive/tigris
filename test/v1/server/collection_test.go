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
	t.Run("status_400_required_search_index_attribute", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"addr": Map{"type": "string", "sort": true},
				},
			},
		}

		resp := createCollection(t, db, coll, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Enable search index first to use faceting or sorting on field 'addr' of type 'string'")
	})
	t.Run("status_400_required_search_index_attribute_1", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"addr": Map{"type": "string", "facet": true},
				},
			},
		}

		resp := createCollection(t, db, coll, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Enable search index first to use faceting or sorting on field 'addr' of type 'string'")
	})
	t.Run("status_400_unsupported_index_obj_level", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"obj": Map{"type": "object", "index": true},
				},
			},
		}

		resp := createCollection(t, db, coll, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot enable index on object 'obj' or object fields")
	})
	t.Run("status_400_unsupported_sort_obj_level", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"obj": Map{"type": "object", "searchIndex": true, "sort": true},
				},
			},
		}

		resp := createCollection(t, db, coll, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot have sort or facet attribute on an object 'obj'")
	})
	t.Run("status_400_unsupported_facet_obj_level", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"obj": Map{"type": "object", "searchIndex": true, "facet": true},
				},
			},
		}

		resp := createCollection(t, db, coll, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot have sort or facet attribute on an object 'obj'")
	})
	t.Run("status_400_unsupported_index_arr", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"arr": Map{"type": "array", "items": Map{"type": "string"}, "index": true},
				},
			},
		}

		resp := createCollection(t, db, coll, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot enable index on field 'arr' of type 'array'. Only top level non-byte fields can be indexed.")
	})
	t.Run("status_400_unsupported_sort_arr", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"arr": Map{"type": "array", "items": Map{"type": "string"}, "searchIndex": true, "sort": true},
				},
			},
		}

		resp := createCollection(t, db, coll, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot enable sorting on field 'arr' of type 'array'")
	})
	t.Run("status_400_unsupported_facet_object_arr", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"arr": Map{"type": "array", "items": Map{"type": "object", "properties": Map{"name": Map{"type": "string", "searchIndex": true, "facet": true}}}},
				},
			},
		}

		resp := createCollection(t, db, coll, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot enable index or search on an array of objects 'name'")
	})
	t.Run("status_200_sort_facet_index", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"addr":    Map{"type": "string"},
					"name":    Map{"type": "string", "index": true, "searchIndex": true, "sort": true, "facet": true},
					"numeric": Map{"type": "integer", "index": true, "searchIndex": true, "sort": true, "facet": true},
					"number":  Map{"type": "number", "index": true, "searchIndex": true, "sort": true, "facet": true},
					"arr":     Map{"type": "array", "items": Map{"type": "string"}, "searchIndex": true, "facet": true},
					"arr_num": Map{"type": "array", "items": Map{"type": "number"}, "searchIndex": true, "facet": true},
					"record":  Map{"type": "object", "properties": Map{"record_name": Map{"type": "string", "searchIndex": true, "sort": true, "facet": true}, "record_arr": Map{"type": "array", "items": Map{"type": "string"}, "searchIndex": true, "facet": true}, "record_obj": Map{"type": "object", "properties": Map{"name": Map{"type": "string", "searchIndex": true, "sort": true, "facet": true}}}}},
				},
			},
		}

		createCollection(t, db, coll, schema).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
	})
	t.Run("status_200_sort_facet_nested_obj", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"obj_1level":     Map{"type": "object", "properties": Map{"name": Map{"type": "string", "searchIndex": true, "sort": true}}},
					"obj_2level":     Map{"type": "object", "properties": Map{"level2": Map{"type": "object", "properties": Map{"name": Map{"type": "string", "searchIndex": true, "sort": true, "facet": true}}}}},
					"obj_2level_arr": Map{"type": "object", "properties": Map{"level2": Map{"type": "object", "properties": Map{"arr": Map{"type": "array", "items": Map{"type": "string"}, "searchIndex": true, "facet": true}}}}},
				},
			},
		}

		createCollection(t, db, coll, schema).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
	})
	t.Run("status_200_facet_nested_obj", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"arr": Map{"type": "object", "properties": Map{"name": Map{"type": "string", "searchIndex": true, "facet": true}}},
				},
			},
		}

		createCollection(t, db, coll, schema).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
	})
	t.Run("status_200_facet_arr", func(t *testing.T) {
		dropCollection(t, db, coll)

		schema := Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"arr": Map{"type": "array", "items": Map{"type": "string"}, "searchIndex": true, "facet": true},
				},
			},
		}

		createCollection(t, db, coll, schema).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
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

	indexes := []Map{
		{
			"name":  "_tigris_created_at",
			"state": "INDEX ACTIVE",
		},
		{
			"name":  "_tigris_updated_at",
			"state": "INDEX ACTIVE",
		},
		{
			"fields": []Map{
				{
					"name": "bool_value",
				},
			},
			"name":  "bool_value",
			"state": "INDEX ACTIVE",
		},
		{
			"fields": []Map{
				{
					"name": "date_time_value",
				},
			},
			"name":  "date_time_value",
			"state": "INDEX ACTIVE",
		},
		{
			"fields": []Map{
				{
					"name": "double_value",
				},
			},
			"name":  "double_value",
			"state": "INDEX ACTIVE",
		},
		{
			"fields": []Map{
				{
					"name": "int_value",
				},
			},
			"name":  "int_value",
			"state": "INDEX ACTIVE",
		},
		{
			"fields": []Map{
				{
					"name": "string_value",
				},
			},
			"name":  "string_value",
			"state": "INDEX ACTIVE",
		},
		{
			"fields": []Map{
				{
					"name": "uuid_value",
				},
			},
			"name":  "uuid_value",
			"state": "INDEX ACTIVE",
		},
	}

	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("collection", coll).
		ValueEqual("size", 0).
		ValueEqual("indexes", indexes)

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
