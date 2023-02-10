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
				"maxLength":   128,
			},
			"bool_value": Map{
				"description": "simple boolean field",
				"type":        "boolean",
			},
			"double_value": Map{
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
						"type": "string",
					},
					"integer_value": Map{
						"type": "integer",
					},
				},
			},
		},
		"source": Map{
			"type": "user",
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

/*
*

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

Array of objects need fixing
*/
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
			"bytes_value":        []byte(`"simple_insert"`),
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

		docs := getDocuments(t, project, index, "1")
		encResp, err := util.MapToJSON(docs[0]["doc"].(map[string]any))
		require.NoError(t, err)

		encInp, err := util.MapToJSON(doc)
		require.NoError(t, err)
		require.JSONEq(t, string(encInp), string(encResp))
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

	output := getDocuments(t, project, index, "1", "2")
	require.Equal(t, 2, len(output))
	for i, out := range output {
		encResp, err := util.MapToJSON(out["doc"].(map[string]any))
		require.NoError(t, err)

		encInp, err := util.MapToJSON(docs[i])
		require.NoError(t, err)
		require.JSONEq(t, string(encInp), string(encResp))
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

	output = getDocuments(t, project, index, "3")
	encResp, err := util.MapToJSON(output[0]["doc"].(map[string]any))
	require.NoError(t, err)

	encInp, err := util.MapToJSON(docsNext[1])
	require.NoError(t, err)
	require.JSONEq(t, string(encInp), string(encResp))
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

	output := getDocuments(t, project, index, "1", "2")
	require.Equal(t, 2, len(output))
	for i, out := range output {
		encResp, err := util.MapToJSON(out["doc"].(map[string]any))
		require.NoError(t, err)

		encInp, err := util.MapToJSON(docs[i])
		require.NoError(t, err)
		require.JSONEq(t, string(encInp), string(encResp))
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

	output = getDocuments(t, project, index, "1", "3")
	require.Equal(t, 2, len(output))
	for i, out := range output {
		encResp, err := util.MapToJSON(out["doc"].(map[string]any))
		require.NoError(t, err)

		encInp, err := util.MapToJSON(docsNext[i])
		require.NoError(t, err)
		require.JSONEq(t, string(encInp), string(encResp))
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

	output := getDocuments(t, project, index, "1", "2", "3")
	require.Equal(t, 3, len(output))
	for i, out := range output {
		if i == 2 {
			// last is nil
			require.Nil(t, out)
			continue
		}
		encResp, err := util.MapToJSON(out["doc"].(map[string]any))
		require.NoError(t, err)

		encInp, err := util.MapToJSON(docsUpdated[i])
		require.NoError(t, err)
		require.JSONEq(t, string(encInp), string(encResp))
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

	output := getDocuments(t, project, index, "1", "2", "3")
	require.Equal(t, 3, len(output))
	for i, out := range output {
		if i == 0 || i == 2 {
			// 1 and 3 are deleted
			require.Nil(t, out)
			continue
		}

		encResp, err := util.MapToJSON(out["doc"].(map[string]any))
		require.NoError(t, err)

		encInp, err := util.MapToJSON(docs[i])
		require.NoError(t, err)
		require.JSONEq(t, string(encInp), string(encResp))
	}
}

func getDocuments(t *testing.T, project string, index string, ids ...string) []Doc {
	req := expect(t).GET(fmt.Sprintf("/v1/projects/%s/search/indexes/%s/documents", project, index))
	for _, id := range ids {
		req.WithQuery("ids", id)
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
