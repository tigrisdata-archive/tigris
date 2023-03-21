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

var testIndex = "test_index"

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
		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, "invalid index name")
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
		Status(http.StatusBadRequest)
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
		writeDocuments(t, project, indexName, fakes, "create")

		for _, source := range []bool{false, true} {
			output := getDocuments(t, project, indexName, source, "1", "2", "3")
			require.Equal(t, 3, len(output))
			for i, out := range output {
				encResp, err := util.MapToJSON(out["data"].(map[string]any))
				require.NoError(t, err)
				require.JSONEq(t, string(serialized[i]), string(encResp))
			}
		}

		res := getSearchResults(t, project, indexName, Map{"q": "second_create", "search_fields": []string{"placeholder"}}, false)
		require.Equal(t, 1, len(res.Result.Hits))
		jsonA, err := jsoniter.Marshal(res.Result.Hits[0]["data"])
		require.NoError(t, err)
		require.JSONEq(t, string(serialized[1]), string(jsonA))
	})
	t.Run("replace_read_search", func(t *testing.T) {
		createSearchIndex(t, project, indexName, schemaObj).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, indexName)

		fakes, serialized := GenerateFakes(t, []string{"1", "2", "3"}, []string{"first_create", "second_create", "third_create"})
		writeDocuments(t, project, indexName, fakes, "create")

		replaceFakes, replaceSerialized := GenerateFakes(t, []string{"1", "random", "3"}, []string{"first_replaced", "random_replaced", "third_replaced"})
		writeDocuments(t, project, indexName, replaceFakes, "replace")

		for _, source := range []bool{false, true} {
			output := getDocuments(t, project, indexName, source, "1", "2", "3")
			require.Equal(t, 3, len(output))
			for i, out := range output {
				encResp, err := util.MapToJSON(out["data"].(map[string]any))
				require.NoError(t, err)

				if i == 1 {
					require.JSONEq(t, string(serialized[i]), string(encResp))
				} else {
					require.JSONEq(t, string(replaceSerialized[i]), string(encResp))
				}
			}
		}

		res := getSearchResults(t, project, indexName, Map{"q": "second_create", "search_fields": []string{"placeholder"}}, false)
		require.Equal(t, 1, len(res.Result.Hits))
		jsonA, err := jsoniter.Marshal(res.Result.Hits[0]["data"])
		require.NoError(t, err)
		require.JSONEq(t, string(serialized[1]), string(jsonA))

		res = getSearchResults(t, project, indexName, Map{"q": "third_replaced", "search_fields": []string{"placeholder"}}, false)
		require.Equal(t, 1, len(res.Result.Hits))
		jsonA, err = jsoniter.Marshal(res.Result.Hits[0]["data"])
		require.NoError(t, err)
		require.JSONEq(t, string(replaceSerialized[2]), string(jsonA))
	})
	t.Run("update_read_search", func(t *testing.T) {
		createSearchIndex(t, project, indexName, schemaObj).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, indexName)

		fakes, _ := GenerateFakes(t, []string{"1", "2", "3"}, []string{"first_create", "second_create", "third_create"})
		writeDocuments(t, project, indexName, fakes, "create")

		updatedFakes, updatedSerialized := GenerateFakes(t, []string{"1"}, []string{"updated_first_document"})
		writeDocuments(t, project, indexName, updatedFakes, "update")

		for _, source := range []bool{false, true} {
			output := getDocuments(t, project, indexName, source, "1")
			require.Equal(t, 1, len(output))
			encResp, err := util.MapToJSON(output[0]["data"].(map[string]any))
			require.NoError(t, err)
			require.JSONEq(t, string(updatedSerialized[0]), string(encResp))
		}

		res := getSearchResults(t, project, indexName, Map{"q": "updated_first_document", "search_fields": []string{"placeholder"}}, false)
		require.Equal(t, 1, len(res.Result.Hits))
		jsonA, err := jsoniter.Marshal(res.Result.Hits[0]["data"])
		require.NoError(t, err)
		require.JSONEq(t, string(updatedSerialized[0]), string(jsonA))
	})
	t.Run("delete", func(t *testing.T) {
		createSearchIndex(t, project, indexName, schemaObj).Status(http.StatusOK)
		defer deleteSearchIndex(t, project, indexName)

		fakes, serialized := GenerateFakes(t, []string{"1", "2", "3"}, []string{"first_create", "second_create", "third_create"})
		writeDocuments(t, project, indexName, fakes, "create")

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

func writeDocuments(t *testing.T, project string, indexName string, fakes []FakeDocument, opType string) {
	var expResponse []map[string]any
	for _, f := range fakes {
		expResponse = append(expResponse, map[string]any{
			"id":    f.Id,
			"error": nil,
		})
	}

	var req *httpexpect.Request
	if opType == "create" {
		req = expect(t).POST(getIndexDocumentURL(project, indexName, ""))
	} else if opType == "replace" {
		req = expect(t).PUT(getIndexDocumentURL(project, indexName, ""))
	} else {
		req = expect(t).PATCH(getIndexDocumentURL(project, indexName, ""))
	}

	req.
		WithJSON(Map{
			"documents": fakes,
		}).
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
