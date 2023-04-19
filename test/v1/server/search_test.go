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
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/util"
	"gopkg.in/gavv/httpexpect.v1"
)

var (
	testIndex                  = "test_index"
	testIndexExplicitIdSchemaA = "test_index_explicit_id_a"
	testIndexExplicitIdSchemaB = "test_index_explicit_id_b"
)

var testSearchIndexSchema = Map{
	"schema": Map{
		"title":       testIndex,
		"description": "this schema is for integration tests",
		"properties": Map{
			"int_value": Map{
				"description": "simple int field",
				"type":        "integer",
			},
			"string_value": Map{
				"description": "simple string field",
				"type":        "string",
				"sort":        true,
				"facet":       true,
			},
			"bool_value": Map{
				"description": "simple boolean field",
				"type":        "boolean",
			},
			"double_value": Map{
				"description": "simple double field",
				"type":        "number",
			},
			"uuid_value": Map{
				"description": "uuid field",
				"type":        "string",
				"format":      "uuid",
			},
			"created_at": Map{
				"description": "date time field",
				"type":        "string",
				"format":      "date-time",
				"sort":        true,
			},
			"array_simple_value": Map{
				"description": "array field",
				"type":        "array",
				"items": Map{
					"type": "string",
				},
			},
			"array_integer_value": Map{
				"description": "array field",
				"type":        "array",
				"items": Map{
					"type": "integer",
				},
			},
			"vector": Map{
				"type":       "array",
				"format":     "vector",
				"dimensions": 4,
			},
			"object_value": Map{
				"description": "object field",
				"type":        "object",
				"properties": Map{
					"string_value": Map{
						"type":  "string",
						"sort":  true,
						"facet": true,
					},
					"integer_value": Map{
						"type":  "integer",
						"sort":  true,
						"facet": true,
					},
				},
			},
			"array_obj_value": Map{
				"description": "array field",
				"type":        "array",
				"items": Map{
					"type": "object",
					"properties": Map{
						"integer_value": Map{
							"type": "integer",
						},
						"string_value": Map{
							"type": "string",
						},
					},
				},
			},
		},
	},
}

var testSearchIndexExplicitIdSchemaA = Map{
	"schema": Map{
		"title": testIndexExplicitIdSchemaA,
		"properties": Map{
			"id": Map{
				// not an id
				"type":  "string",
				"sort":  true,
				"facet": true,
			},
			"stringV": Map{
				"id":    true,
				"type":  "string",
				"sort":  true,
				"facet": true,
			},
			"doubleV": Map{
				"description": "simple double field",
				"type":        "number",
			},
			"arrayV": Map{
				"description": "array field",
				"type":        "array",
				"items": Map{
					"type": "string",
				},
			},
			"objectV": Map{
				"type": "object",
				"properties": Map{
					"id": Map{
						"type":  "string",
						"sort":  true,
						"facet": true,
					},
					"stringV": Map{
						"type":  "string",
						"sort":  true,
						"facet": true,
					},
				},
			},
		},
	},
}

var testSearchIndexExplicitIdSchemaB = Map{
	"schema": Map{
		"title": testIndexExplicitIdSchemaA,
		"properties": Map{
			"id": Map{
				// not an id
				"type":  "string",
				"sort":  true,
				"facet": true,
			},
			"stringV": Map{
				"type":  "string",
				"sort":  true,
				"facet": true,
			},
			"doubleV": Map{
				"description": "simple double field",
				"type":        "number",
			},
			"arrayV": Map{
				"description": "array field",
				"type":        "array",
				"items": Map{
					"type": "string",
				},
			},
			"objectV": Map{
				"type": "object",
				"properties": Map{
					"id": Map{
						"id":    true,
						"type":  "string",
						"sort":  true,
						"facet": true,
					},
					"stringV": Map{
						"type":  "string",
						"sort":  true,
						"facet": true,
					},
				},
			},
		},
	},
}

func getIndexDocumentURL(projectName, indexName string, anyIdentifier string) string {
	if len(anyIdentifier) > 0 {
		return fmt.Sprintf("/v1/projects/%s/search/indexes/%s/documents/%s", projectName, indexName, anyIdentifier)
	}

	return fmt.Sprintf("/v1/projects/%s/search/indexes/%s/documents", projectName, indexName)
}

func getIndexURL(projectName, indexName string) string {
	return fmt.Sprintf("/v1/projects/%s/search/indexes/%s", projectName, indexName)
}

func createSearchIndex(t *testing.T, project string, index string, schema map[string]interface{}) *httpexpect.Response {
	e := expect(t)
	return e.PUT(getIndexURL(project, index)).
		WithJSON(schema).
		Expect()
}

func deleteSearchIndex(t *testing.T, project string, index string) *httpexpect.Response {
	e := expect(t)
	return e.DELETE(getIndexURL(project, index)).
		Expect()
}

func TestIndex_Management(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	t.Run("status_400_empty_name", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		resp := createSearchIndex(t, project, "", nil)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "empty search index name is not allowed")
	})
	t.Run("status_400_schema_nil", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		resp := createSearchIndex(t, project, testIndex, nil)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "schema is a required during index creation")
	})
	t.Run("status_400_unsupported_sort_obj_level", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		schema := Map{
			"schema": Map{
				"title": testIndex,
				"properties": Map{
					"obj": Map{"type": "object", "sort": true},
				},
			},
		}

		resp := createSearchIndex(t, project, testIndex, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot have sort or facet attribute on an object 'obj'")
	})
	t.Run("status_400_unsupported_facet_obj_level", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		schema := Map{
			"schema": Map{
				"title": testIndex,
				"properties": Map{
					"obj": Map{"type": "object", "facet": true},
				},
			},
		}

		resp := createSearchIndex(t, project, testIndex, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot have sort or facet attribute on an object 'obj'")
	})
	t.Run("status_400_unsupported_sort_arr", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		schema := Map{
			"schema": Map{
				"title": testIndex,
				"properties": Map{
					"arr": Map{"type": "array", "items": Map{"type": "string"}, "sort": true},
				},
			},
		}

		resp := createSearchIndex(t, project, testIndex, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot enable sorting on field 'arr' of type 'array'")
	})
	t.Run("status_400_unsupported_facet_object_arr", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		schema := Map{
			"schema": Map{
				"title": testIndex,
				"properties": Map{
					"arr": Map{"type": "array", "items": Map{"type": "object", "properties": Map{"name": Map{"type": "string", "facet": true}}}},
				},
			},
		}

		resp := createSearchIndex(t, project, testIndex, schema)
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "Cannot enable index or search on an array of objects 'name'")
	})
	t.Run("status_200_sort_facet_index", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		schema := Map{
			"schema": Map{
				"title": testIndex,
				"properties": Map{
					"addr":    Map{"type": "string"},
					"name":    Map{"type": "string", "sort": true, "facet": true},
					"numeric": Map{"type": "integer", "sort": true, "facet": true},
					"number":  Map{"type": "number", "sort": true, "facet": true},
					"arr":     Map{"type": "array", "items": Map{"type": "string"}, "facet": true},
					"arr_num": Map{"type": "array", "items": Map{"type": "number"}, "facet": true},
					"record":  Map{"type": "object", "properties": Map{"record_name": Map{"type": "string", "sort": true, "facet": true}, "record_arr": Map{"type": "array", "items": Map{"type": "string"}, "facet": true}, "record_obj": Map{"type": "object", "properties": Map{"name": Map{"type": "string", "sort": true, "facet": true}}}}},
				},
			},
		}

		createSearchIndex(t, project, testIndex, schema).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
	})
	t.Run("status_200_sort_facet_nested_obj", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		schema := Map{
			"schema": Map{
				"title": testIndex,
				"properties": Map{
					"obj_1level":     Map{"type": "object", "properties": Map{"name": Map{"type": "string", "sort": true}}},
					"obj_2level":     Map{"type": "object", "properties": Map{"level2": Map{"type": "object", "properties": Map{"name": Map{"type": "string", "sort": true, "facet": true}}}}},
					"obj_2level_arr": Map{"type": "object", "properties": Map{"level2": Map{"type": "object", "properties": Map{"arr": Map{"type": "array", "items": Map{"type": "string"}, "facet": true}}}}},
				},
			},
		}

		createSearchIndex(t, project, testIndex, schema).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
	})
	t.Run("status_200_facet_nested_obj", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		schema := Map{
			"schema": Map{
				"title": testIndex,
				"properties": Map{
					"arr": Map{"type": "object", "properties": Map{"name": Map{"type": "string", "facet": true}}},
				},
			},
		}

		createSearchIndex(t, project, testIndex, schema).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
	})
	t.Run("status_200_facet_arr", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		schema := Map{
			"schema": Map{
				"title": testIndex,
				"properties": Map{
					"arr": Map{"type": "array", "items": Map{"type": "string"}, "facet": true},
				},
			},
		}

		createSearchIndex(t, project, testIndex, schema).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
	})
	t.Run("status_success", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		resp := createSearchIndex(t, project, testIndex, testSearchIndexSchema)
		resp.Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")
	})
	t.Run("create_delete_list", func(t *testing.T) {
		deleteSearchIndex(t, project, testIndex)

		resp := createSearchIndex(t, project, testIndex, testSearchIndexSchema)
		resp.Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "created")

		respList := expect(t).GET(fmt.Sprintf("/v1/projects/%s/search/indexes", project)).
			Expect().
			JSON().
			Object()

		found := false
		items := respList.Value("indexes").Array()
		for _, item := range items.Iter() {
			if item.Raw().(map[string]interface{})["name"] == testIndex {
				found = true
			}
		}
		require.True(t, found)

		deleteResp := deleteSearchIndex(t, project, testIndex)
		deleteResp.Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "deleted")

		respList = expect(t).GET(fmt.Sprintf("/v1/projects/%s/search/indexes", project)).
			Expect().
			JSON().
			Object()
		if len(respList.Raw()) > 0 {
			found := false
			items := respList.Value("indexes").Array()
			for _, item := range items.Iter() {
				if item.Raw().(map[string]interface{})["name"] == testIndex {
					found = true
				}
			}
			require.False(t, found)
		}
	})
}

func TestCreate_ById(t *testing.T) {
	project, index := setupTestsProjectAndSearchIndex(t)
	defer cleanupTests(t, project)
	t.Run("success", func(t *testing.T) {
		doc := Doc{
			"id":                 "1",
			"int_value":          1,
			"string_value":       "simple_insert",
			"bool_value":         true,
			"double_value":       10.01,
			"array_simple_value": []string{"a", "b"},
			"array_obj_value":    []any{map[string]any{"integer_value": 10}},
			"object_value":       map[string]any{"string_value": "a"},
		}

		expect(t).POST(getIndexDocumentURL(project, index, "1")).
			WithJSON(Map{
				"document": doc,
			}).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("id", "1")

		for _, source := range []bool{false, true} {
			docs := getDocuments(t, project, index, source, "1")
			encResp, err := util.MapToJSON(docs[0]["data"].(map[string]any))
			require.NoError(t, err)

			encInp, err := util.MapToJSON(doc)
			require.NoError(t, err)
			require.JSONEq(t, string(encInp), string(encResp))
		}

		res := getSearchResults(t, project, index, Map{"q": "a", "search_fields": []string{"object_value.string_value"}}, false)
		require.Equal(t, 1, len(res.Result.Hits))
		compareDocs(t, doc, res.Result.Hits[0]["data"])
	})
}

func TestCreate(t *testing.T) {
	project, index := setupTestsProjectAndSearchIndex(t)
	defer cleanupTests(t, project)

	docs := []Doc{
		{
			"id":           "1",
			"int_value":    1,
			"string_value": "simple_insert_1",
			"double_value": 10.01,
		}, {
			"id":           "2",
			"int_value":    2,
			"string_value": "simple_insert_2",
			"double_value": 20.01,
		},
	}

	expect(t).POST(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docs,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "2", "error": nil},
			},
		)

	for _, source := range []bool{false, true} {
		output := getDocuments(t, project, index, source, "1", "2")
		require.Equal(t, 2, len(output))
		for i, out := range output {
			encResp, err := util.MapToJSON(out["data"].(map[string]any))
			require.NoError(t, err)

			encInp, err := util.MapToJSON(docs[i])
			require.NoError(t, err)
			require.JSONEq(t, string(encInp), string(encResp))
		}
	}

	docsNext := []Doc{
		{
			"id":           "1",
			"int_value":    1,
			"string_value": "simple_insert_1",
			"double_value": 10.01,
		}, {
			"id":           "3",
			"int_value":    3,
			"string_value": "simple_insert_3",
			"double_value": 30.01,
		},
	}

	expect(t).POST(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docsNext,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": map[string]any{"code": 409, "message": "A document with id '1' already exists."}},
				{"id": "3", "error": nil},
			},
		)

	for _, source := range []bool{false, true} {
		output := getDocuments(t, project, index, source, "3")
		encResp, err := util.MapToJSON(output[0]["data"].(map[string]any))
		require.NoError(t, err)

		encInp, err := util.MapToJSON(docsNext[1])
		require.NoError(t, err)
		require.JSONEq(t, string(encInp), string(encResp))
	}

	// Invalid id type. Expected string.
	docs[0]["id"] = 4
	docs[1]["id"] = 5
	expect(t).POST(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docs,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "", "error": map[string]any{"code": 400, "message": "wrong type of 'id' field"}},
				{"id": "", "error": map[string]any{"code": 400, "message": "wrong type of 'id' field"}},
			},
		)
}

func TestCreateOrReplace(t *testing.T) {
	project, index := setupTestsProjectAndSearchIndex(t)
	defer cleanupTests(t, project)

	docs := []Doc{
		{
			"id":           "1",
			"int_value":    1,
			"string_value": "simple_insert_1",
			"double_value": 10.01,
		}, {
			"id":           "2",
			"int_value":    2,
			"string_value": "simple_insert_2",
			"double_value": 20.01,
		},
	}

	expect(t).PUT(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docs,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "2", "error": nil},
			},
		)

	for _, source := range []bool{false, true} {
		output := getDocuments(t, project, index, source, "1", "2")
		require.Equal(t, 2, len(output))
		for i, out := range output {
			encResp, err := util.MapToJSON(out["data"].(map[string]any))
			require.NoError(t, err)

			encInp, err := util.MapToJSON(docs[i])
			require.NoError(t, err)
			require.JSONEq(t, string(encInp), string(encResp))
		}
	}

	docsNext := []Doc{
		{
			"id":           "1",
			"int_value":    10,
			"string_value": "simple_insert_1_replaced",
			"double_value": 100.01,
		}, {
			"id":           "3",
			"int_value":    3,
			"string_value": "simple_insert_3",
			"double_value": 30.01,
		},
	}

	expect(t).PUT(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docsNext,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "3", "error": nil},
			},
		)

	for _, source := range []bool{false, true} {
		output := getDocuments(t, project, index, source, "1", "3")
		require.Equal(t, 2, len(output))
		for i, out := range output {
			encResp, err := util.MapToJSON(out["data"].(map[string]any))
			require.NoError(t, err)

			encInp, err := util.MapToJSON(docsNext[i])
			require.NoError(t, err)
			require.JSONEq(t, string(encInp), string(encResp))
		}
	}
}

func TestUpdate(t *testing.T) {
	project, index := setupTestsProjectAndSearchIndex(t)
	defer cleanupTests(t, project)

	docs := []Doc{
		{
			"id":           "1",
			"int_value":    1,
			"string_value": "simple_insert_1",
			"double_value": 10.01,
		}, {
			"id":           "2",
			"int_value":    2,
			"string_value": "simple_insert_2",
			"double_value": 20.01,
		},
	}

	expect(t).POST(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docs,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "2", "error": nil},
			},
		)

	docsUpdated := []Doc{
		{
			"id":           "1",
			"int_value":    10,
			"string_value": "simple_insert1_updated",
			"double_value": 100.01,
		}, {
			"id":           "2",
			"int_value":    20,
			"string_value": "simple_insert2_updated",
			"double_value": 200.01,
		}, {
			// this id doesn't exist should receive an error.
			"id":           "3",
			"int_value":    30,
			"string_value": "simple_insert3_updated",
			"double_value": 300.01,
		},
	}

	expect(t).PATCH(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docsUpdated,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "2", "error": nil},
				{"id": "3", "error": map[string]any{"code": 404, "message": "Could not find a document with id: 3"}},
			},
		)

	for _, source := range []bool{false, true} {
		output := getDocuments(t, project, index, source, "1", "2", "3")
		require.Equal(t, 3, len(output))
		for i, out := range output {
			if i == 2 {
				// last is nil
				require.Nil(t, out)
				continue
			}
			encResp, err := util.MapToJSON(out["data"].(map[string]any))
			require.NoError(t, err)

			encInp, err := util.MapToJSON(docsUpdated[i])
			require.NoError(t, err)
			require.JSONEq(t, string(encInp), string(encResp))
		}
	}
}

func TestDelete(t *testing.T) {
	project, index := setupTestsProjectAndSearchIndex(t)
	defer cleanupTests(t, project)

	docs := []Doc{
		{
			"id":           "1",
			"int_value":    1,
			"string_value": "simple_insert_1",
			"double_value": 10.01,
		}, {
			"id":           "2",
			"int_value":    2,
			"string_value": "simple_insert_2",
			"double_value": 20.01,
		}, {
			"id":           "3",
			"int_value":    3,
			"string_value": "simple_insert_3",
			"double_value": 30.01,
		},
	}

	expect(t).POST(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docs,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "2", "error": nil},
				{"id": "3", "error": nil},
			},
		)

	expect(t).DELETE(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"ids": []string{"1", "3"},
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "3", "error": nil},
			},
		)

	for _, source := range []bool{false, true} {
		output := getDocuments(t, project, index, source, "1", "2", "3")
		require.Equal(t, 3, len(output))
		for i, out := range output {
			if i == 0 || i == 2 {
				// 1 and 3 are deleted
				require.Nil(t, out)
				continue
			}

			encResp, err := util.MapToJSON(out["data"].(map[string]any))
			require.NoError(t, err)

			encInp, err := util.MapToJSON(docs[i])
			require.NoError(t, err)
			require.JSONEq(t, string(encInp), string(encResp))
		}
	}
}

func TestSearch(t *testing.T) {
	project, index := setupTestsProjectAndSearchIndex(t)
	defer cleanupTests(t, project)

	docs := []Doc{
		{
			"id":                 "1",
			"int_value":          1,
			"string_value":       "data platform",
			"double_value":       10.01,
			"array_simple_value": []string{"abc", "def"},
			"object_value": Doc{
				"string_value":  "san francisco",
				"integer_value": 1,
			},
			"created_at": "2023-02-02T05:50:19+00:00",
		}, {
			"id":                 "2",
			"int_value":          2,
			"string_value":       "big data",
			"double_value":       20.01,
			"array_simple_value": []string{"foo", "bar"},
			"object_value": Doc{
				"string_value":  "san diego",
				"integer_value": 2,
			},
			"created_at": "2023-02-01T05:50:19+00:00",
		}, {
			"id":                 "3",
			"int_value":          3,
			"string_value":       "basedata",
			"double_value":       30.01,
			"array_simple_value": []string{"foo", "bar"},
			"object_value": Doc{
				"string_value":  "san francisco",
				"integer_value": 3,
			},
			"created_at": "2023-01-02T05:50:19+00:00",
		},
	}

	expect(t).PUT(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docs,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "2", "error": nil},
				{"id": "3", "error": nil},
			},
		)

	res := getSearchResults(t, project, index, Map{"q": "data", "sort": []Doc{{"created_at": "$asc"}}}, false)
	require.Equal(t, 2, len(res.Result.Hits))
	compareDocs(t, docs[1], res.Result.Hits[0]["data"])
	compareDocs(t, docs[0], res.Result.Hits[1]["data"])

	res = getSearchResults(t, project, index, Map{"q": "*", "group_by": Doc{"fields": []string{"object_value.string_value"}}, "sort": []Doc{{"created_at": "$asc"}}}, false)
	require.Equal(t, 2, len(res.Result.Groups))
	require.Equal(t, []interface{}{"san francisco"}, res.Result.Groups[0]["group_keys"])
	require.Equal(t, []interface{}{"san diego"}, res.Result.Groups[1]["group_keys"])

	compareDocs(t, docs[2], res.Result.Groups[0]["hits"].([]any)[0].(map[string]any)["data"].(map[string]any))
	compareDocs(t, docs[0], res.Result.Groups[0]["hits"].([]any)[1].(map[string]any)["data"].(map[string]any))
	compareDocs(t, docs[1], res.Result.Groups[1]["hits"].([]any)[0].(map[string]any)["data"].(map[string]any))
}

func TestVectorSearch(t *testing.T) {
	project, index := setupTestsProjectAndSearchIndex(t)
	defer cleanupTests(t, project)

	// vector length greater the defined dimensions
	expect(t).PUT(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": []Doc{
				{
					"id":           "1",
					"string_value": "data platform",
					"vector":       []float64{0.1, 1.1, 0.02, 93, 0.75},
				},
			},
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{
					"id": "1",
					"error": map[string]any{
						"code":    400,
						"message": "field 'vector' of vector type should not have dimensions greater than the defined in schema, defined: '4' found: '5'"},
				},
			},
		)

	docs := []Doc{
		{
			"id":           "1",
			"string_value": "data platform",
			"vector":       []float64{0.1, 1.1, 0.93, 0.75},
		}, {
			"id":           "2",
			"string_value": "big data platform",
			"vector":       []float64{-0.23, -0.57, 1.12, 0.98},
		}, {
			"id":           "3",
			"string_value": "basedata",
			"vector":       []float64{1.1, 2.22, 0.0875, 0.975},
		}, {
			"id":           "4",
			"string_value": "random",
			"vector":       []float64{2.1, 1.22, 0.7875, 0.175},
		},
	}

	expect(t).PUT(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docs,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
				{"id": "2", "error": nil},
				{"id": "3", "error": nil},
				{"id": "4", "error": nil},
			},
		)

	res := getSearchResults(t, project, index, Map{"vector": Map{"vector": []float64{1.1, 2.22, 0.0875, 0.975}}}, false)
	require.Equal(t, 4, len(res.Result.Hits))

	// closer to the query vector
	compareDocs(t, docs[2], res.Result.Hits[0]["data"])
	compareDocs(t, docs[0], res.Result.Hits[1]["data"])
	compareDocs(t, docs[3], res.Result.Hits[2]["data"])
	compareDocs(t, docs[1], res.Result.Hits[3]["data"])

	require.Equal(t, float64(0), res.Result.Hits[0]["metadata"]["match"].(map[string]any)["vector_distance"])
	require.Equal(t, 0.22375428676605225, res.Result.Hits[1]["metadata"]["match"].(map[string]any)["vector_distance"])

	res = getSearchResults(t, project, index, Map{"vector": Map{"vector": []float64{1.1, 2.22, 0.0875, 0.975}}, "filter": Map{"string_value": Map{"$contains": "platform"}}}, false)
	require.Equal(t, 2, len(res.Result.Hits))

	// contains will filter out other rows
	compareDocs(t, docs[0], res.Result.Hits[0]["data"])
	compareDocs(t, docs[1], res.Result.Hits[1]["data"])

	res = getSearchResults(t, project, index, Map{"vector": Map{"vector": []float64{1.1, 2.22, 0.0875, 0.975}}, "filter": Map{"string_value": Map{"$not": "platform"}}}, false)
	require.Equal(t, 2, len(res.Result.Hits))

	// contains will filter out other rows
	compareDocs(t, docs[2], res.Result.Hits[0]["data"])
	compareDocs(t, docs[3], res.Result.Hits[1]["data"])

}

func TestComplexObjects(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	indexName := "t1"
	schema := []byte(`{
  "schema": {
    "title": "t1",
    "properties": {
      "a": {
        "type": "integer"
      },
      "b": {
        "type": "string"
      },
      "c": {
        "type": "object",
        "properties": {
          "a": {
            "type": "integer"
          },
          "b": {
            "type": "string"
          },
          "c": {
            "type": "object",
            "properties": {
              "a": {
                "type": "string"
              },
              "b": {
                "searchIndex": false,
                "type": "object",
                "properties": {}
              },
              "c": {
                "type": "array",
                "searchIndex": false,
                "items": {
                  "type": "object",
                  "properties": {
                    "a": {
                      "type": "string"
                    }
                  }
                }
              },
              "d": {
                "type": "array",
                "items": {
                  "type": "string"
                }
              }
            }
          },
          "d": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "e": {
            "searchIndex": false,
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "a": {
                  "type": "string"
                }
              }
            }
          }
        }
      },
      "d": {
        "type": "object",
        "properties": {}
      },
      "e": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {}
        }
      },
      "f": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "a": {
              "type": "string"
            }
          }
        }
      },
      "g": {
        "type": "array",
        "items": {
          "type": "string"
        }
      }
    }
  }
}`)
	var schemaObj map[string]any
	require.NoError(t, jsoniter.Unmarshal(schema, &schemaObj))
	createSearchIndex(t, project, indexName, schemaObj).Status(http.StatusOK)

	docA := Doc{
		"id": "1",
		"a":  1,
		"b":  "first document",
		"c": Map{
			"a": 10,
			"b": "nested object under c",
			"c": Map{
				"a": "foo",
				"b": Map{"name": "this is free flow object but not indexed"},
				"c": []Map{Map{"a": "car"}, Map{"a": "bike"}},
				"d": []string{"PARIS", "LONDON", "ENGLAND"},
			},
			"d": []string{"SANTA CLARA", "SAN JOSE"},
			"e": []Map{{"a": "football"}, {"a": "basketball"}},
		},
		"d": Map{"agent": "free flow object top level"},
		"e": []Map{{"random": "array of free flow object"}},
		"f": []Map{{"a": "array of object with a field"}},
		"g": []string{"NEW YORK", "MIAMI"},
	}

	expect(t).PUT(getIndexDocumentURL(project, indexName, "")).
		WithJSON(Map{
			"documents": []Doc{docA},
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
			},
		)

	cases := []struct {
		query    Map
		expError string
	}{
		{query: Map{"q": "nested object under c", "search_fields": []string{"c.b"}}, expError: ""},
		{query: Map{"q": "foo", "search_fields": []string{"c.c.a"}}, expError: ""},
		{query: Map{"q": "foo", "search_fields": []string{"c.c.a"}}, expError: ""},
		{query: Map{"q": "foo", "search_fields": []string{"c.c.b"}}, expError: "`c.c.b` is not a searchable field. Only indexed fields can be queried"},
		{query: Map{"q": "paris", "search_fields": []string{"c.c.d"}}, expError: ""},
		{query: Map{"q": "santa", "search_fields": []string{"c.d"}}, expError: ""},
		{query: Map{"q": "santa", "search_fields": []string{"c.e"}}, expError: "`c.e` is not a searchable field. Only indexed fields can be queried"},
		{query: Map{"q": "free flow object top level", "search_fields": []string{"d"}}, expError: ""},
		{query: Map{"q": "array of free flow object", "search_fields": []string{"e"}}, expError: ""},
		{query: Map{"q": "array of object with a field", "search_fields": []string{"f"}}, expError: ""},
		{query: Map{"q": "NEW YORK", "search_fields": []string{"g"}}, expError: ""},
	}
	for _, c := range cases {
		if len(c.expError) > 0 {
			expect(t).POST(fmt.Sprintf("/v1/projects/%s/search/indexes/%s/documents/search", project, indexName)).
				WithJSON(c.expError).
				Expect().
				Status(http.StatusBadRequest)

			continue
		}
		res := getSearchResults(t, project, indexName, c.query, false)
		require.Equal(t, 1, len(res.Result.Hits))
	}
}

func TestChunking(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	indexName := "fake_index"
	var schemaObj map[string]any
	require.NoError(t, jsoniter.Unmarshal(FakeDocumentSchema, &schemaObj))

	t.Run("create_read_search", func(t *testing.T) {
		createSearchIndex(t, project, indexName, schemaObj).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, indexName)

		fakes, serialized := GenerateFakes(t, []string{"1", "2", "3"}, []string{"first_create", "second_create", "third_create"})
		writeDocuments(t, project, indexName, Map{"documents": fakes}, expResponseForFakes(fakes), "create")

		validateReadOut(t, project, indexName, []string{"1", "2", "3"}, serialized)

		validateSearchOut(t, project, indexName, Map{"q": "second_create", "search_fields": []string{"placeholder"}}, serialized[1:2])
	})
	t.Run("replace_read_search", func(t *testing.T) {
		createSearchIndex(t, project, indexName, schemaObj).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, indexName)

		fakes, serialized := GenerateFakes(t, []string{"1", "2", "3"}, []string{"first_create", "second_create", "third_create"})
		writeDocuments(t, project, indexName, Map{"documents": fakes}, expResponseForFakes(fakes), "create")

		replaceFakes, replaceSerialized := GenerateFakes(t, []string{"1", "random", "3"}, []string{"first_replaced", "random_replaced", "third_replaced"})
		writeDocuments(t, project, indexName, Map{"documents": replaceFakes}, expResponseForFakes(replaceFakes), "replace")

		var finalSerialized [][]byte
		finalSerialized = append(finalSerialized, replaceSerialized[0])
		finalSerialized = append(finalSerialized, serialized[1])
		finalSerialized = append(finalSerialized, replaceSerialized[2])

		validateReadOut(t, project, indexName, []string{"1", "2", "3"}, finalSerialized)

		validateSearchOut(t, project, indexName, Map{"q": "second_create", "search_fields": []string{"placeholder"}}, serialized[1:2])
		validateSearchOut(t, project, indexName, Map{"q": "third_replaced", "search_fields": []string{"placeholder"}}, replaceSerialized[2:3])
	})
	t.Run("update_read_search", func(t *testing.T) {
		createSearchIndex(t, project, indexName, schemaObj).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, indexName)

		fakes, _ := GenerateFakes(t, []string{"1", "2", "3"}, []string{"first_create", "second_create", "third_create"})
		writeDocuments(t, project, indexName, Map{"documents": fakes}, expResponseForFakes(fakes), "create")

		updatedFakes, updatedSerialized := GenerateFakes(t, []string{"1"}, []string{"updated_first_document"})
		writeDocuments(t, project, indexName, Map{"documents": updatedFakes}, expResponseForFakes(updatedFakes), "update")
		validateReadOut(t, project, indexName, []string{"1"}, updatedSerialized[:1])

		validateSearchOut(t, project, indexName, Map{"q": "updated_first_document", "search_fields": []string{"placeholder"}}, updatedSerialized[:1])
	})
	t.Run("delete", func(t *testing.T) {
		createSearchIndex(t, project, indexName, schemaObj).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, indexName)

		fakes, serialized := GenerateFakes(t, []string{"1", "2", "3"}, []string{"first_create", "second_create", "third_create"})
		writeDocuments(t, project, indexName, Map{"documents": fakes}, expResponseForFakes(fakes), "create")

		expect(t).DELETE(getIndexDocumentURL(project, indexName, "")).
			WithJSON(Map{
				"ids": []string{"1", "3"},
			}).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status",
				[]map[string]any{
					{"id": "1", "error": nil},
					{"id": "3", "error": nil},
				},
			)

		for _, source := range []bool{false, true} {
			output := getDocuments(t, project, indexName, source, "1", "2", "3")
			require.Equal(t, 3, len(output))
			require.Nil(t, output[0])
			require.Nil(t, output[2])

			encResp, err := util.MapToJSON(output[1]["data"].(map[string]any))
			require.NoError(t, err)
			require.JSONEq(t, string(serialized[1]), string(encResp))
		}
	})
}

func TestSearchIndexExplicitIdA(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	t.Run("create_read_search", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaA, testSearchIndexExplicitIdSchemaA).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaA)

		docs := []Doc{
			{"id": "1", "stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"id": "nested1", "stringV": "foo"}},
			{"id": "2", "stringV": "bar", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}
		serialized := getSerializedData(t, docs)

		writeDocuments(t, project, testIndexExplicitIdSchemaA, Map{"documents": docs}, expResponseFromIds([]string{"foo", "bar"}, []error{nil, nil}), "create")

		validateReadOut(t, project, testIndexExplicitIdSchemaA, []string{"foo", "bar"}, serialized)

		validateSearchOut(t, project, testIndexExplicitIdSchemaA, Map{"q": "foo", "search_fields": []string{"stringV"}}, serialized[:1])
	})
	t.Run("replace_read_search", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaA, testSearchIndexExplicitIdSchemaA).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaA)

		docs := []Doc{
			{"id": "1", "stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"id": "nested1", "stringV": "foo"}},
			{"id": "2", "stringV": "bar", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}
		serialized := getSerializedData(t, docs)
		writeDocuments(t, project, testIndexExplicitIdSchemaA, Map{"documents": docs}, expResponseFromIds([]string{"foo", "bar"}, []error{nil, nil}), "create")

		replaceDocs := []Doc{
			{"stringV": "foo", "arrayV": []string{"foo_bar", "bar_foo"}, "doubleV": 30.01, "objectV": Map{"id": "nested3", "stringV": "foo_bar"}},
			{"stringV": "foo_bar", "id": "random", "arrayV": []string{"a", "b"}, "doubleV": 40.01, "objectV": Map{"id": "nested4", "stringV": "c"}},
		}
		replaceSerialized := getSerializedData(t, replaceDocs)
		writeDocuments(t, project, testIndexExplicitIdSchemaA, Map{"documents": replaceDocs}, expResponseFromIds([]string{"foo", "foo_bar"}, []error{nil, nil}), "replace")

		var finalSerialized [][]byte
		finalSerialized = append(finalSerialized, replaceSerialized[0])
		finalSerialized = append(finalSerialized, serialized[1])
		finalSerialized = append(finalSerialized, replaceSerialized[1])

		validateReadOut(t, project, testIndexExplicitIdSchemaA, []string{"foo", "bar", "foo_bar"}, finalSerialized)

		validateSearchOut(t, project, testIndexExplicitIdSchemaA, Map{"q": "random", "search_fields": []string{"id"}}, finalSerialized[2:3])
	})
	t.Run("missing_id_create", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaA, testSearchIndexExplicitIdSchemaA).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaA)

		docs := []Doc{
			{"stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"id": "nested1", "stringV": "foo"}},
			{"id": "2", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}
		writeDocuments(t,
			project,
			testIndexExplicitIdSchemaA,
			Map{"documents": docs},
			expResponseFromIds(
				[]string{"foo", ""},
				[]error{
					nil,
					&api.TigrisError{Code: 400, Message: "index has explicitly marked 'stringV' field as 'id' but document is missing that field"},
				}),
			"create")
	})
	t.Run("missing_id_replace", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaA, testSearchIndexExplicitIdSchemaA).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaA)

		docs := []Doc{
			{"stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"id": "nested1", "stringV": "foo"}},
			{"id": "2", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}
		writeDocuments(t,
			project,
			testIndexExplicitIdSchemaA,
			Map{"documents": docs},
			expResponseFromIds(
				[]string{"foo", ""},
				[]error{
					nil,
					&api.TigrisError{Code: 400, Message: "index has explicitly marked 'stringV' field as 'id' but document is missing that field"},
				}),
			"replace")
	})
	t.Run("missing_id_update", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaA, testSearchIndexExplicitIdSchemaA).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaA)

		docs := []Doc{
			{"stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"id": "nested1", "stringV": "foo"}},
			{"id": "2", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}
		writeDocuments(t,
			project,
			testIndexExplicitIdSchemaA,
			Map{"documents": docs},
			expResponseFromIds(
				[]string{"foo", ""},
				[]error{
					&api.TigrisError{Code: 404, Message: "Could not find a document with id: foo"},
					&api.TigrisError{Code: 400, Message: "index has explicitly marked 'stringV' field as 'id' but document is missing that field"},
				}),
			"update")
	})
}

// this test is testing explicit setting "id" a nested field
func TestSearchIndexExplicitIdB(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	t.Run("create_read_search", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaB, testSearchIndexExplicitIdSchemaB).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaB)

		docs := []Doc{
			{"id": "1", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"id": "nested1", "stringV": "foo"}},
			{"stringV": "bar", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}
		serialized := getSerializedData(t, docs)

		writeDocuments(t, project, testIndexExplicitIdSchemaB, Map{"documents": docs}, expResponseFromIds([]string{"nested1", "nested2"}, []error{nil, nil}), "create")

		validateReadOut(t, project, testIndexExplicitIdSchemaB, []string{"nested1", "nested2"}, serialized)

		validateSearchOut(t, project, testIndexExplicitIdSchemaB, Map{"q": "bar", "search_fields": []string{"stringV"}}, serialized[1:2])
	})
	t.Run("replace_read_search", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaB, testSearchIndexExplicitIdSchemaB).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaB)

		docs := []Doc{
			{"stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"id": "nested1", "stringV": "foo"}},
			{"id": "2", "stringV": "bar", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}
		serialized := getSerializedData(t, docs)
		writeDocuments(t, project, testIndexExplicitIdSchemaB, Map{"documents": docs}, expResponseFromIds([]string{"nested1", "nested2"}, []error{nil, nil}), "create")

		replaceDocs := []Doc{
			{"arrayV": []string{"foo_bar", "bar_foo"}, "doubleV": 30.01, "objectV": Map{"id": "nested2", "stringV": "foo_bar"}},
			{"stringV": "foo_bar", "id": "random", "arrayV": []string{"a", "b"}, "doubleV": 40.01, "objectV": Map{"id": "nested3", "stringV": "c"}},
		}
		replaceSerialized := getSerializedData(t, replaceDocs)
		writeDocuments(t, project, testIndexExplicitIdSchemaB, Map{"documents": replaceDocs}, expResponseFromIds([]string{"nested2", "nested3"}, []error{nil, nil}), "replace")

		var finalSerialized [][]byte
		finalSerialized = append(finalSerialized, serialized[0])
		finalSerialized = append(finalSerialized, replaceSerialized[0])
		finalSerialized = append(finalSerialized, replaceSerialized[1])

		validateReadOut(t, project, testIndexExplicitIdSchemaB, []string{"nested1", "nested2", "nested3"}, finalSerialized)

		validateSearchOut(t, project, testIndexExplicitIdSchemaB, Map{"q": "random", "search_fields": []string{"id"}}, finalSerialized[2:3])
	})
	t.Run("missing_id_create", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaB, testSearchIndexExplicitIdSchemaB).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaB)

		docs := []Doc{
			{"stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"stringV": "foo"}},
			{"id": "2", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}

		writeDocuments(t,
			project,
			testIndexExplicitIdSchemaB,
			Map{"documents": docs},
			expResponseFromIds(
				[]string{"", "nested2"},
				[]error{
					&api.TigrisError{Code: 400, Message: "index has explicitly marked 'objectV.id' field as 'id' but document is missing that field"},
					nil,
				}),
			"create")
	})
	t.Run("missing_id_replace", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaB, testSearchIndexExplicitIdSchemaB).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaB)

		docs := []Doc{
			{"stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"stringV": "foo"}},
			{"id": "2", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"id": "nested2", "stringV": "bar"}},
		}
		writeDocuments(t,
			project,
			testIndexExplicitIdSchemaB,
			Map{"documents": docs},
			expResponseFromIds(
				[]string{"", "nested2"},
				[]error{
					&api.TigrisError{Code: 400, Message: "index has explicitly marked 'objectV.id' field as 'id' but document is missing that field"},
					nil,
				}),
			"replace")
	})
	t.Run("missing_id_update", func(t *testing.T) {
		createSearchIndex(t, project, testIndexExplicitIdSchemaB, testSearchIndexExplicitIdSchemaB).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, testIndexExplicitIdSchemaB)

		docs := []Doc{
			{"stringV": "foo", "arrayV": []string{"foo", "bar"}, "doubleV": 10.01, "objectV": Map{"id": "nested1", "stringV": "foo"}},
			{"id": "2", "arrayV": []string{"bar", "foo"}, "doubleV": 20.01, "objectV": Map{"stringV": "bar"}},
		}
		writeDocuments(t,
			project,
			testIndexExplicitIdSchemaB,
			Map{"documents": docs},
			expResponseFromIds(
				[]string{"nested1", ""},
				[]error{
					&api.TigrisError{Code: 404, Message: "Could not find a document with id: nested1"},
					&api.TigrisError{Code: 400, Message: "index has explicitly marked 'objectV.id' field as 'id' but document is missing that field"},
				}),
			"update")
	})
}

func TestSearch_StringInt64(t *testing.T) {
	project, index := setupTestsProjectAndSearchIndex(t)
	defer cleanupTests(t, project)

	docs := []Doc{
		{
			"id":                  "1",
			"int_value":           "9223372036854775799",
			"string_value":        "data platform",
			"array_integer_value": []string{"9223372036854775807", "9223372036854775806"},
			"array_simple_value":  []string{"abc", "def"},
			"object_value": Doc{
				"string_value":  "san francisco",
				"integer_value": "9223372036854775804",
			},
			"created_at": "2023-02-02T05:50:19+00:00",
		},
	}

	expect(t).PUT(getIndexDocumentURL(project, index, "")).
		WithJSON(Map{
			"documents": docs,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status",
			[]map[string]any{
				{"id": "1", "error": nil},
			},
		)

	res := getSearchResults(t, project, index, Map{"q": "data", "sort": []Doc{{"created_at": "$asc"}}}, false)
	require.Equal(t, 1, len(res.Result.Hits))

	docs[0]["int_value"] = 9223372036854775799
	docs[0]["array_integer_value"] = []int64{9223372036854775807, 9223372036854775806}
	docs[0]["object_value"] = Doc{
		"string_value":  "san francisco",
		"integer_value": 9223372036854775804,
	}
	compareDocs(t, docs[0], res.Result.Hits[0]["data"])
}

func validateReadOut(t *testing.T, project string, index string, ids []string, expReadOut [][]byte) {
	for _, source := range []bool{false, true} {
		output := getDocuments(t, project, index, source, ids...)
		require.Equal(t, len(expReadOut), len(output))
		for i, out := range output {
			encResp, err := util.MapToJSON(out["data"].(map[string]any))
			require.NoError(t, err)
			require.JSONEq(t, string(expReadOut[i]), string(encResp))
		}
	}
}

func validateSearchOut(t *testing.T, project string, index string, inputSearchQuery Map, expSearchOut [][]byte) {
	res := getSearchResults(t, project, index, inputSearchQuery, false)
	require.Equal(t, len(expSearchOut), len(res.Result.Hits))
	for _, expSearchOut := range expSearchOut {
		jsonA, err := jsoniter.Marshal(res.Result.Hits[0]["data"])
		require.NoError(t, err)
		require.JSONEq(t, string(expSearchOut), string(jsonA))
	}
}

func getSerializedData(t *testing.T, docs []Doc) [][]byte {
	var serialized [][]byte
	for _, d := range docs {
		js, err := util.MapToJSON(d)
		require.NoError(t, err)
		serialized = append(serialized, js)
	}

	return serialized
}

func expResponseFromIds(ids []string, expError []error) []map[string]any {
	var expResponse []map[string]any
	for i, id := range ids {
		expResponse = append(expResponse, map[string]any{
			"id":    id,
			"error": expError[i],
		})
	}

	return expResponse
}

func expResponseForFakes(fakes []FakeDocument) []map[string]any {
	var expResponse []map[string]any
	for _, f := range fakes {
		expResponse = append(expResponse, map[string]any{
			"id":    f.Id,
			"error": nil,
		})
	}

	return expResponse
}

func writeDocuments(t *testing.T, project string, indexName string, docs Map, expResponse []map[string]any, opType string) {
	var req *httpexpect.Request
	if opType == "create" {
		req = expect(t).POST(getIndexDocumentURL(project, indexName, ""))
	} else if opType == "replace" {
		req = expect(t).PUT(getIndexDocumentURL(project, indexName, ""))
	} else {
		req = expect(t).PATCH(getIndexDocumentURL(project, indexName, ""))
	}

	req.
		WithJSON(docs).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", expResponse)
}

func compareDocs(t *testing.T, docA Doc, docB Doc) {
	jsonA, err := jsoniter.Marshal(docA)
	require.NoError(t, err)
	jsonB, err := jsoniter.Marshal(docB)
	require.NoError(t, err)

	require.JSONEq(t, string(jsonA), string(jsonB))
}

type res struct {
	Result struct {
		Hits   []map[string]Doc `json:"hits"`
		Groups []Doc            `json:"group"`
		Meta   Doc              `json:"meta"`
	} `json:"result"`
}

func getSearchResults(t *testing.T, project string, index string, query Map, isCollectionSearch bool) *res {
	url := fmt.Sprintf("/v1/projects/%s/search/indexes/%s/documents/search", project, index)
	if isCollectionSearch {
		url = fmt.Sprintf("/v1/projects/%s/database/collections/%s/documents/search", project, index)
	}

	var req = expect(t).POST(url).
		WithJSON(query).
		Expect().
		Status(http.StatusOK).
		Body().
		Raw()
	dec := jsoniter.NewDecoder(bytes.NewReader([]byte(req)))

	var res *res
	require.NoError(t, dec.Decode(&res))
	return res
}

func getDocuments(t *testing.T, project string, index string, backendStorage bool, ids ...string) []Doc {
	req := expect(t).GET(fmt.Sprintf("/v1/projects/%s/search/indexes/%s/documents", project, index))
	for _, id := range ids {
		req.WithQuery("ids", id)
	}
	if backendStorage {
		req.WithHeader("Tigris-Search-Read-From-Storage", "true")
	}

	str := req.
		Expect().
		Status(http.StatusOK).
		Body().
		Raw()

	dec := jsoniter.NewDecoder(bytes.NewReader([]byte(str)))
	var mp map[string][]Doc
	require.NoError(t, dec.Decode(&mp))
	return mp["documents"]
}
