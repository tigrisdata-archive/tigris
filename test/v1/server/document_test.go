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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
	"gopkg.in/gavv/httpexpect.v1"
)

func TestInsert_Bad_NotFoundRequest(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)
	cases := []struct {
		databaseName   string
		collectionName string
		documents      []Doc
		expMessage     string
		status         int
	}{
		{
			"random_project1",
			coll,
			[]Doc{{"pkey_int": 1}},
			"project doesn't exist 'random_project1'",
			http.StatusNotFound,
		}, {
			db,
			"random_collection",
			[]Doc{{"pkey_int": 1}},
			"collection doesn't exist 'random_collection'",
			http.StatusNotFound,
		}, {
			"",
			coll,
			[]Doc{{"pkey_int": 1}},
			"invalid database name",
			http.StatusBadRequest,
		}, {
			db,
			"",
			[]Doc{{"pkey_int": 1}},
			"invalid collection name",
			http.StatusBadRequest,
		}, {
			db,
			coll,
			[]Doc{},
			"empty documents received",
			http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		resp := insertDocuments(t, c.databaseName, c.collectionName, c.documents, true)

		code := api.Code_INVALID_ARGUMENT
		if c.status == http.StatusNotFound {
			code = api.Code_NOT_FOUND
		}
		testError(resp, c.status, code, c.expMessage)
	}
}

func TestInsert_AlreadyExists(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     1,
			"int_value":    10,
			"string_value": "simple_insert",
			"bool_value":   true,
			"double_value": 10.01,
			"bytes_value":  []byte(`"simple_insert"`),
		},
	}

	insertDocuments(t, db, coll, inputDocument, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	resp := insertDocuments(t, db, coll, inputDocument, true)

	testError(resp, http.StatusConflict, api.Code_ALREADY_EXISTS, "duplicate key value, violates key constraint")
}

func TestInsert_EmptyArray(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collectionName := "test_empty_array"
	cases := []struct {
		schema           Map
		inputDoc         []Doc
		primaryKeyLookup Map
	}{
		{
			schema: Map{
				"schema": Map{
					"title": collectionName,
					"properties": Map{
						"int_value": Map{
							"type": "integer",
						},
						"empty_array": Map{
							"type":     "array",
							"maxItems": 0,
						},
					},
					"primary_key": []string{"int_value"},
				},
			},
			inputDoc: []Doc{
				{
					"int_value":   10,
					"empty_array": []string{},
				},
			},
		},
	}

	createCollection(t, db, collectionName, cases[0].schema).Status(http.StatusOK)

	insertDocuments(t, db, collectionName, cases[0].inputDoc, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	readResp := readByFilter(t, db, collectionName, Map{}, nil, nil, nil)

	var doc map[string]jsoniter.RawMessage
	require.Equal(t, 1, len(readResp))
	require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &doc))

	actualDoc := []byte(doc["data"])
	expDoc, err := jsoniter.Marshal(cases[0].inputDoc[0])
	require.NoError(t, err)
	require.JSONEq(t, string(expDoc), string(actualDoc))

	insertDocuments(t, db, collectionName, []Doc{{
		"int_value":   11,
		"empty_array": []any{1, 1},
	}}, true).
		Status(http.StatusBadRequest)
}

func TestInsert_SupportedPrimaryKeys(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	b64 := base64.StdEncoding.EncodeToString([]byte(`base64 string`))

	uuidValue := uuid.New().String()
	collectionName := "test_supported_primary_keys"
	cases := []struct {
		schema           Map
		inputDoc         []Doc
		primaryKeyLookup Map
	}{
		{
			schema: Map{
				"schema": Map{
					"title": collectionName,
					"properties": Map{
						"int_value": Map{
							"type": "integer",
						},
						"bytes_value": Map{
							"type":   "string",
							"format": "byte",
						},
					},
					"primary_key": []any{"bytes_value"},
				},
			},
			inputDoc: []Doc{
				{
					"int_value":   10,
					"bytes_value": b64,
				},
			},
			primaryKeyLookup: Map{
				"bytes_value": b64,
			},
		}, {
			schema: Map{
				"schema": Map{
					"title": collectionName,
					"properties": Map{
						"int_value": Map{
							"type": "integer",
						},
						"uuid_value": Map{
							"type":   "string",
							"format": "uuid",
						},
					},
					"primary_key": []any{"uuid_value"},
				},
			},
			inputDoc: []Doc{
				{
					"int_value":  10,
					"uuid_value": uuidValue,
				},
			},
			primaryKeyLookup: Map{
				"uuid_value": uuidValue,
			},
		}, {
			schema: Map{
				"schema": Map{
					"title": collectionName,
					"properties": Map{
						"int_value": Map{
							"type": "integer",
						},
						"date_time_value": Map{
							"type":   "string",
							"format": "date-time",
						},
					},
					"primary_key": []any{"date_time_value"},
				},
			},
			inputDoc: []Doc{
				{
					"int_value":       10,
					"date_time_value": "2015-12-21T17:42:34Z",
				},
			},
			primaryKeyLookup: Map{
				"date_time_value": "2015-12-21T17:42:34Z",
			},
		}, {
			schema: Map{
				"schema": Map{
					"title": collectionName,
					"properties": Map{
						"int_value": Map{
							"type": "integer",
						},
						"string_value": Map{
							"type": "string",
						},
					},
					"primary_key": []any{"string_value"},
				},
			},
			inputDoc: []Doc{
				{
					"int_value":    10,
					"string_value": "hello",
				},
			},
			primaryKeyLookup: Map{
				"string_value": "hello",
			},
		},
	}
	for _, c := range cases {
		dropCollection(t, db, collectionName)
		createCollection(t, db, collectionName, c.schema).Status(http.StatusOK)

		var key string
		var value any
		for k, v := range c.primaryKeyLookup {
			key = k
			value = v
		}

		insertDocuments(t, db, collectionName, c.inputDoc, true).
			Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "inserted").
			ValueEqual("keys", []map[string]any{{key: value}})

		readResp := readByFilter(t, db, collectionName, c.primaryKeyLookup, nil, nil, nil)

		var doc map[string]jsoniter.RawMessage
		require.Greater(t, len(readResp), 0)
		require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &doc))

		actualDoc := []byte(doc["data"])
		expDoc, err := jsoniter.Marshal(c.inputDoc[0])
		require.NoError(t, err)
		require.JSONEq(t, string(expDoc), string(actualDoc))
	}
}

func TestInsert_SingleRow(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	b64 := base64.StdEncoding.EncodeToString([]byte(`base64 string`))

	inputDocument := []Doc{
		{
			"pkey_int":        10,
			"int_value":       10,
			"string_value":    "simple_insert",
			"bool_value":      true,
			"double_value":    10.01,
			"bytes_value":     b64,
			"date_time_value": "2015-12-21T17:42:34Z",
			"uuid_value":      uuid.New().String(),
			"array_value": []Doc{
				{
					"id":      1,
					"product": "foo",
				},
			},
			"object_value": Map{
				"name": "hi",
			},
		},
	}

	tstart := time.Now().UTC()
	insertDocuments(t, db, coll, inputDocument, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted").
		ValueEqual("keys", []map[string]any{{"pkey_int": 10}}).
		Path("$.metadata").Object().
		Value("created_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readResp := readByFilter(t,
		db,
		coll,
		Map{
			"pkey_int": 10,
		},
		nil,
		nil,
		nil)

	var doc map[string]jsoniter.RawMessage
	require.Equal(t, 1, len(readResp))
	require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &doc))

	actualDoc := []byte(doc["data"])
	expDoc, err := jsoniter.Marshal(inputDocument[0])
	require.NoError(t, err)
	require.JSONEq(t, string(expDoc), string(actualDoc))
}

func TestInsert_CreatedUpdatedAt(t *testing.T) {
	dbName := fmt.Sprintf("db_test_%s", t.Name())
	defer deleteProject(t, dbName)

	for _, index := range []bool{false, true} {
		deleteProject(t, dbName)
		createProject(t, dbName)

		collectionName := fmt.Sprintf("test_collection_%s", t.Name())
		createCollection(t, dbName, collectionName,
			Map{
				"schema": Map{
					"title": collectionName,
					"properties": Map{
						"id": Map{
							"type": "integer",
						},
						"int_value": Map{
							"type":  "integer",
							"index": index,
						},
						"string_value": Map{
							"type":  "string",
							"index": index,
						},
						"created_at": Map{
							"type":   "string",
							"format": "date-time",
							"index":  index,
						},
						"updated_at": Map{
							"type":   "string",
							"format": "date-time",
							"index":  index,
						},
						"metadata": Map{
							"type": "object",
							"properties": Map{
								"created_at": Map{
									"type":   "string",
									"format": "date-time",
								},
							},
						},
					},
				},
			}).Status(http.StatusOK)

		inputDoc := []Doc{{"id": 1, "int_value": 1, "string_value": "foo", "created_at": "2023-02-05T21:45:35Z", "updated_at": "2023-02-05T23:04:33.01234567Z", "metadata": Doc{"created_at": "2023-02-05T21:45:35.01234Z"}}}
		insertDocuments(t, dbName, collectionName, inputDoc, true).
			Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "inserted")

		cases := []struct {
			filter Map
			exp    bool
		}{
			{
				Map{
					"created_at": "2023-02-05T21:45:35Z",
				},
				true,
			},
			{
				Map{
					"updated_at": "2023-02-05T23:04:33.01234567Z",
				},
				true,
			},
			{
				Map{
					"metadata.created_at": "2023-02-05T21:45:35.01234Z",
				},
				true,
			},
			{
				Map{
					"metadata.created_at": "2023-02-05T21:45:35Z",
				},
				false,
			},
		}
		for _, c := range cases {
			readResp := readByFilter(t,
				dbName,
				collectionName,
				c.filter,
				nil,
				nil,
				nil)

			var doc map[string]jsoniter.RawMessage
			if !c.exp {
				require.Equal(t, 0, len(readResp))
				continue
			}

			require.Equal(t, 1, len(readResp))
			require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &doc))

			actualDoc := []byte(doc["data"])
			expDoc, err := jsoniter.Marshal(inputDoc[0])
			require.NoError(t, err)
			require.JSONEq(t, string(expDoc), string(actualDoc))
		}
	}
}

func TestInsert_ReadNullsAndByte(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	byteV := []byte("62ea6a943d44b10e1b6b8797")
	inputDocument := []Doc{
		{
			"pkey_int":           10,
			"int_value":          nil,
			"string_value":       nil,
			"bool_value":         nil,
			"double_value":       nil,
			"bytes_value":        byteV,
			"date_time_value":    nil,
			"uuid_value":         nil,
			"simple_array_value": []any{"abc", nil, "def"},
			"array_value": []Doc{
				{
					"id":      1,
					"product": nil,
				},
			},
			"object_value": Map{
				"name": nil,
			},
		},
	}

	expect(t).POST(getDocumentURL(db, coll, "insert")).
		WithJSON(Map{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted").
		ValueEqual("keys", []map[string]any{{"pkey_int": 10}})

	cases := []struct {
		filter Map
	}{
		{
			Map{
				"pkey_int": 10,
			},
		},
		{
			Map{
				"string_value": nil,
			},
		},
		{
			Map{
				"int_value": nil,
			},
		},
		{
			Map{
				"double_value": nil,
			},
		},
		{
			Map{
				"bytes_value": byteV,
			},
		},
	}
	for _, c := range cases {
		readResp := readByFilter(t,
			db,
			coll,
			c.filter,
			nil,
			nil,
			nil)

		var doc map[string]jsoniter.RawMessage
		require.Equal(t, 1, len(readResp))
		require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &doc))

		actualDoc := []byte(doc["data"])
		expDoc, err := jsoniter.Marshal(inputDocument[0])
		require.NoError(t, err)
		require.JSONEq(t, string(expDoc), string(actualDoc))
	}
}

func TestInsert_StringInt64(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     "9223372036854775799",
			"int_value":    "9223372036854775800",
			"string_value": "simple_insert",
			"array_value": []Doc{
				{
					"id":      "9223372036854775801",
					"product": "foo",
				}, {
					"id":      9223372036854775802,
					"product": "foo",
				},
			},
			"object_value": Map{
				"bignumber": "9223372036854775751",
			},
		},
	}

	tstart := time.Now().UTC()
	insertDocuments(t, db, coll, inputDocument, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted").
		ValueEqual("keys", []map[string]any{{"pkey_int": 9223372036854775799}}).
		Path("$.metadata").Object().
		Value("created_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readResp := readByFilter(t,
		db,
		coll,
		Map{
			"pkey_int": 9223372036854775799,
		},
		nil,
		nil,
		nil)

	var doc map[string]jsoniter.RawMessage
	require.Equal(t, 1, len(readResp))
	require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &doc))

	actualDoc := []byte(doc["data"])
	expDoc := []byte(`{"pkey_int":9223372036854776000,"int_value":9223372036854776000,"string_value":"simple_insert","array_value":[{"id":9223372036854776000,"product":"foo"},{"id":9223372036854776000,"product":"foo"}],"object_value":{"bignumber":9223372036854776000}}`)
	require.JSONEq(t, string(expDoc), string(actualDoc))
}

func TestInsert_MultipleRows(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     20,
			"int_value":    20,
			"string_value": "simple_insert1",
			"bool_value":   true,
			"double_value": 20.00001,
			"bytes_value":  []byte(`"simple_insert1"`),
		},
		{
			"pkey_int":     30,
			"int_value":    30,
			"string_value": "simple_insert2",
			"bool_value":   false,
			"double_value": 20.0002,
			"bytes_value":  []byte(`"simple_insert2"`),
		},
	}

	insertDocuments(t, db, coll, inputDocument, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted").
		ValueEqual("keys", []map[string]any{{"pkey_int": 20}, {"pkey_int": 30}})

	readResp := readByFilter(t,
		db,
		coll,
		Map{
			"$or": []Doc{
				{"pkey_int": 20},
				{"pkey_int": 30},
			},
		},
		nil,
		nil,
		nil)

	require.Equal(t, 2, len(readResp))
	for i := 0; i < len(inputDocument); i++ {
		var doc map[string]jsoniter.RawMessage
		require.NoError(t, jsoniter.Unmarshal(readResp[i]["result"], &doc))

		actualDoc := []byte(doc["data"])
		expDoc, err := jsoniter.Marshal(inputDocument[i])
		require.NoError(t, err)
		require.JSONEq(t, string(expDoc), string(actualDoc))
	}
}

func TestInsert_AutoGenerated(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	testAutoGenerated(t, db, coll, Map{"type": "string", "autoGenerate": true})
	testAutoGenerated(t, db, coll, Map{
		"type":         "string",
		"autoGenerate": true,
		"format":       "byte",
	})
	testAutoGenerated(t, db, coll, Map{
		"type":         "string",
		"format":       "uuid",
		"autoGenerate": true,
	})
	testAutoGenerated(t, db, coll, Map{
		"type":         "string",
		"format":       "date-time",
		"autoGenerate": true,
	})
	testAutoGenerated(t, db, coll, Map{
		"type":         "integer",
		"format":       "int32",
		"autoGenerate": true,
	})
	testAutoGenerated(t, db, coll, Map{"type": "integer", "autoGenerate": true})
}

func TestInsert_SchemaUpdate(t *testing.T) {
	dbName := fmt.Sprintf("db_test_%s", t.Name())

	deleteProject(t, dbName)
	createProject(t, dbName)
	defer deleteProject(t, dbName)

	collectionName := fmt.Sprintf("test_collection_%s", t.Name())
	createCollection(t, dbName, collectionName,
		Map{
			"schema": Map{
				"title": collectionName,
				"properties": Map{
					"int_value": Map{
						"type": "integer",
					},
					"string_value": Map{
						"type": "string",
					},
				},
			},
		}).Status(http.StatusOK)

	inputDoc := []Doc{{"int_value": 1, "string_value": "foo"}}
	insertDocuments(t, dbName, collectionName, inputDoc, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	inputDoc = []Doc{{"int_value": 1, "string_value": "foo", "extra_field": "bar"}}
	insertDocuments(t, dbName, collectionName, inputDoc, true).
		Status(http.StatusBadRequest).
		JSON().
		Path("$.error").Object().
		ValueEqual("code", api.CodeToString(api.Code_INVALID_ARGUMENT))

	// update the schema
	createCollection(t, dbName, collectionName,
		Map{
			"schema": Map{
				"title": collectionName,
				"properties": Map{
					"int_value": Map{
						"type": "integer",
					},
					"string_value": Map{
						"type": "string",
					},
					"extra_field": Map{
						"type": "string",
					},
				},
			},
		}).Status(http.StatusOK)

	// try same insert
	insertDocuments(t, dbName, collectionName, inputDoc, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")
}

func testAutoGenerated(t *testing.T, dbName string, collectionName string, pkey Map) {
	dropCollection(t, dbName, collectionName)
	createCollection(t, dbName, collectionName,
		Map{
			"schema": Map{
				"title": collectionName,
				"properties": Map{
					"int_value": Map{
						"type": "integer",
					},
					"pkey": pkey,
				},
				"primary_key": []any{"pkey"},
			},
		}).Status(http.StatusOK)

	generatorType, ok := pkey["format"].(string)
	if !ok {
		generatorType = pkey["type"].(string)
		if generatorType == "integer" {
			generatorType = "int64"
		}
	}

	// insert 1, 2
	inputDoc := []Doc{{"int_value": 1}, {"int_value": 2}}
	insertDocuments(t, dbName, collectionName, inputDoc, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	// insert 3
	thirdDoc := []Doc{{"int_value": 3}}
	insertDocuments(t, dbName, collectionName, thirdDoc, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	// test zero values for every type
	var pk any
	switch generatorType {
	case "int32":
		pk = 0
	case "int64":
		pk = 0
	case "string":
		pk = ""
	case "uuid":
		pk = uuid.UUID{}.String()
	case "date-time":
		pk = time.Time{}.Format(time.RFC3339Nano)
	case "byte":
		pk = base64.StdEncoding.EncodeToString([]byte(""))
	}

	fourthDoc := []Doc{{"pkey": pk, "int_value": 4}}
	insertDocuments(t, dbName, collectionName, fourthDoc, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	// test user set values
	switch generatorType {
	case "int32":
		pk = int32(123)
	case "string", "uuid":
		pk = "11111111-00b6-4eb5-a64d-351be56afe36"
	case "int64":
		pk = int64(123)
	case "date-time":
		pk = time.Now().UTC().Add(-1000000 * time.Hour).Format(time.RFC3339Nano)
	case "byte":
		pk = base64.StdEncoding.EncodeToString([]byte("abc"))
	}

	fifthDoc := []Doc{{"pkey": pk, "int_value": 5}}
	i := expect(t).POST(getDocumentURL(dbName, collectionName, "insert")).
		WithJSON(Map{
			"documents": fifthDoc,
		}).Expect().Status(http.StatusOK).JSON().Object().Raw()
	require.Equal(t, "inserted", i["status"])
	k := i["keys"].([]any)
	require.Less(t, 0, len(k))
	assert.EqualValues(t, pk, k[0].(map[string]any)["pkey"])

	readResp := readByFilter(t, dbName, collectionName, nil, nil, nil, nil)
	decodedResult := make([]map[string]any, 5)
	for _, response := range readResp {
		var doc map[string]jsoniter.RawMessage
		require.NoError(t, jsoniter.Unmarshal(response["result"], &doc))
		var actualDoc map[string]any
		require.NoError(t, jsoniter.Unmarshal(doc["data"], &actualDoc))

		val := int64(actualDoc["int_value"].(float64))
		if val > 5 || val < 1 {
			require.Fail(t, fmt.Sprintf("not expected value %d %T", val, actualDoc["int_value"]))
		}
		decodedResult[val-1] = actualDoc
	}

	validate := func(response map[string]any, inputDoc Doc, iteration int) {
		require.Equal(t, inputDoc["int_value"], int(response["int_value"].(float64)))
		require.NotNil(t, response["pkey"])
		switch generatorType {
		case "int32":
			if iteration == 4 {
				require.Equal(t, 123, int(response["pkey"].(float64)))
			} else {
				require.Equal(t, iteration+1, int(response["pkey"].(float64)))
			}
		case "string", "uuid":
			s, err := uuid.Parse(response["pkey"].(string))
			require.NoError(t, err)
			require.NotEqual(t, uuid.UUID{}.String(), s.String())
		case "int64":
			if iteration == 4 {
				require.Equal(t, 123, int(response["pkey"].(float64)))
			} else {
				ts := time.Unix(0, int64(response["pkey"].(float64)))
				require.True(t, ts.After(time.Now().Add(-1*time.Minute)))
			}
		case "date-time":
			ts, err := time.Parse(time.RFC3339Nano, response["pkey"].(string))
			require.NoError(t, err)
			if iteration == 4 {
				require.Equal(t, pk, response["pkey"].(string))
			} else {
				require.True(t, ts.After(time.Now().Add(-1*time.Minute)))
			}
			require.NotEqual(t, time.Time{}, ts)
		case "byte":
			s, err := base64.StdEncoding.DecodeString(response["pkey"].(string))
			require.NoError(t, err)
			assert.NotEqual(t, "", string(s))
		}
	}
	validate(decodedResult[0], inputDoc[0], 0)
	validate(decodedResult[1], inputDoc[1], 1)
	validate(decodedResult[2], thirdDoc[0], 2)
	validate(decodedResult[3], fourthDoc[0], 3)
	validate(decodedResult[4], fifthDoc[0], 4)
}

func TestInsertUpdate_Defaults(t *testing.T) {
	start := time.Now().UTC()

	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_defaults"
	schema := []byte(`{
	"schema": {
		"properties": {
			"int_f": {
				"type": "integer",
				"index": true
			},
			"created": {
				"type": "string",
				"format": "date-time",
				"createdAt": true,
				"index": true
			},
			"updated": {
				"type": "string",
				"format": "date-time",
				"updatedAt": true,
				"index": true
			},
			"arr_obj": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"created": {
							"type": "string",
							"format": "date-time",
							"createdAt": true
						},
						"updated": {
							"type": "string",
							"format": "date-time",
							"updatedAt": true
						},
						"int_f": {
							"type": "integer"
						}
					}
				}
			},
			"obj_u": {
				"type": "object",
				"properties": {
					"updated": {
						"type": "string",
						"format": "date-time",
						"updatedAt": true
					}
				}
			},
			"obj_c": {
				"type": "object",
				"properties": {
					"created": {
						"type": "string",
						"format": "date-time",
						"default": "now()"
					}
				}
			},
			"int_arr": {
				"type": "array",
				"items": {
					"type": "integer"
				}
			},
			"pkey_int": {
				"type": "integer"
			}
		},
		"primary_key": ["pkey_int"],
		"title": "test_defaults"
	}
}`)
	var schemaObj map[string]any
	require.NoError(t, jsoniter.Unmarshal(schema, &schemaObj))
	createCollection(t, db, collection, schemaObj).Status(200)

	inputDocument := []Doc{
		{
			"pkey_int": 100,
			"arr_obj": []Doc{
				{
					"int_f": 1,
				}, {
					"int_f":   2,
					"created": time.Now().Format(time.RFC3339),
				}, {
					"int_f": 3,
				},
			},
		},
	}

	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	readResp := readByFilter(t,
		db,
		collection,
		Map{
			"pkey_int": 100,
		},
		nil,
		nil,
		nil)

	var data map[string]any
	require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &data))
	var doc = data["data"].(map[string]any)

	createdAtBefore, err := time.Parse(time.RFC3339, doc["created"].(string))
	require.NoError(t, err)
	require.WithinRange(t, createdAtBefore, start, time.Now().UTC())
	require.Nil(t, doc["updated"])

	createdAtObjBefore, err := time.Parse(time.RFC3339, doc["obj_c"].(map[string]any)["created"].(string))
	require.NoError(t, err)
	require.WithinRange(t, createdAtObjBefore, start, time.Now().UTC())
	require.Nil(t, doc["obj_u"])

	require.Equal(t, doc["arr_obj"].([]any)[0].(map[string]any)["created"], doc["arr_obj"].([]any)[2].(map[string]any)["created"])
	require.NotEqual(t, doc["arr_obj"].([]any)[0].(map[string]any)["created"], doc["arr_obj"].([]any)[1].(map[string]any)["created"])
	require.Nil(t, doc["arr_obj"].([]any)[0].(map[string]any)["updated"])

	// update now, this should fill "updated" but "created"/"ts" fields should remain same
	updateByFilter(t,
		db,
		collection,
		Map{
			"filter": Map{
				"pkey_int": 100,
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"int_f": 10,
				},
				"$push": Map{
					"arr_obj": Map{
						"int_f": 123,
					},
					"int_arr": 5,
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1)

	readResp = readByFilter(t,
		db,
		collection,
		Map{
			"pkey_int": 100,
		},
		nil,
		nil,
		nil)

	require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &data))
	doc = data["data"].(map[string]any)

	createdAtAfter, err := time.Parse(time.RFC3339, doc["created"].(string))
	require.NoError(t, err)
	require.Equal(t, createdAtBefore, createdAtAfter)

	updatedAt, err := time.Parse(time.RFC3339, doc["updated"].(string))
	require.NoError(t, err)
	require.WithinRange(t, updatedAt, start, time.Now().UTC())

	updateAtObj, err := time.Parse(time.RFC3339, doc["obj_u"].(map[string]any)["updated"].(string))
	require.NoError(t, err)
	require.WithinRange(t, updateAtObj, start, time.Now().UTC())

	require.Equal(t, doc["arr_obj"].([]any)[0].(map[string]any)["updated"], doc["arr_obj"].([]any)[2].(map[string]any)["updated"])
	require.Equal(t, doc["arr_obj"].([]any)[0].(map[string]any)["updated"], doc["arr_obj"].([]any)[1].(map[string]any)["updated"])

	// push field asserts
	require.Equal(t, 4, len(doc["arr_obj"].([]any)))
	require.Equal(t, float64(123), doc["arr_obj"].([]any)[3].(map[string]any)["int_f"])
	require.Equal(t, 1, len(doc["int_arr"].([]any)))
	require.Equal(t, float64(5), doc["int_arr"].([]any)[0])

	// not supporting default values in array
	require.Nil(t, doc["arr_obj"].([]any)[3].(map[string]any)["created"])
	require.Nil(t, doc["arr_obj"].([]any)[3].(map[string]any)["updated"])
}

func TestInsertUpdate_AllDefaults(t *testing.T) {
	start := time.Now().UTC()

	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_defaults"
	schema := []byte(`{
"schema": {
  "properties": {
    "int_f": {
      "type": "integer",
      "default": 11
    },
    "bool_f": {
      "type": "boolean",
      "default": true
    },
    "double_f": {
      "type": "number",
      "default": 1.1
    },
    "cuid": {
      "type": "string",
      "default": "cuid()"
    },
    "uuid": {
      "type": "string",
      "default": "uuid()"
    },
    "ts": {
      "type": "string",
      "format": "date-time",
      "default": "now()"
    },
    "created": {
      "type": "string",
      "format": "date-time",
      "createdAt": true
    },
    "updated": {
      "type": "string",
      "format": "date-time",
      "updatedAt": true
    },
    "arr_obj": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          },
          "zipcodes": {
            "type": "array",
            "items": {
              "type": "integer"
            },
            "default": [12345, 54321]
          }
        }
      }
    },
    "string_arr": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "default": ["a"]
    },
    "obj": {
      "type": "object",
      "properties": {
        "int_field": {
          "type": "integer",
          "default": 10
        }
      }
    },
    "pkey_int": {
      "type": "integer"
  }
  },
  "primary_key": ["pkey_int"],
  "title": "test_defaults"
}
}`)
	var schemaObj map[string]any
	require.NoError(t, jsoniter.Unmarshal(schema, &schemaObj))
	createCollection(t, db, collection, schemaObj).Status(200)

	inputDocument := []Doc{
		{
			"pkey_int": 100,
		},
	}

	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	readResp := readByFilter(t,
		db,
		collection,
		Map{
			"pkey_int": 100,
		},
		nil,
		nil,
		nil)

	var data map[string]any
	require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &data))
	var doc = data["data"].(map[string]any)
	require.True(t, doc["bool_f"].(bool))
	require.Equal(t, 1.1, doc["double_f"].(float64))
	require.Equal(t, float64(11), doc["int_f"].(float64))
	require.Equal(t, []any{"a"}, doc["string_arr"].([]any))
	require.Equal(t, float64(10), doc["obj"].(map[string]any)["int_field"].(float64))

	createdAtBefore, err := time.Parse(time.RFC3339, doc["created"].(string))
	require.NoError(t, err)
	require.WithinRange(t, createdAtBefore, start, time.Now().UTC())
	require.Nil(t, doc["updated"])
	ts, err := time.Parse(time.RFC3339, doc["ts"].(string))
	require.NoError(t, err)
	require.WithinRange(t, ts, start, time.Now())

	// createdAt and now() should be same
	require.Equal(t, doc["created"].(string), doc["ts"].(string))

	_, err = uuid.Parse(doc["uuid"].(string))
	require.NoError(t, err)

	// update now, this should fill "updated" but "created"/"ts" fields should remain same
	updateByFilter(t,
		db,
		collection,
		Map{
			"filter": Map{
				"pkey_int": 100,
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"double_f": 2.1,
					"int_f":    20,
				},
				"$push": Map{
					"arr_obj": Map{
						"name": "ABC",
					},
					"string_arr": "Hello",
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1)

	readResp = readByFilter(t,
		db,
		collection,
		Map{
			"pkey_int": 100,
		},
		nil,
		nil,
		nil)

	require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &data))
	doc = data["data"].(map[string]any)
	require.True(t, doc["bool_f"].(bool))
	require.Equal(t, 2.1, doc["double_f"].(float64))
	require.Equal(t, float64(20), doc["int_f"].(float64))
	require.Equal(t, []any{"a", "Hello"}, doc["string_arr"].([]any))
	require.Equal(t, float64(10), doc["obj"].(map[string]any)["int_field"].(float64))

	// push field assert
	require.Equal(t, "ABC", doc["arr_obj"].([]any)[0].(map[string]any)["name"])

	createdAtAfter, err := time.Parse(time.RFC3339, doc["created"].(string))
	require.NoError(t, err)
	tsAfter, err := time.Parse(time.RFC3339, doc["ts"].(string))
	require.NoError(t, err)

	require.Equal(t, createdAtBefore, createdAtAfter)
	require.Equal(t, createdAtBefore, tsAfter)

	updatedAt, err := time.Parse(time.RFC3339, doc["updated"].(string))
	require.NoError(t, err)
	require.WithinRange(t, updatedAt, start, time.Now().UTC())

	_, err = uuid.Parse(doc["uuid"].(string))
	require.NoError(t, err)
}

func TestUpdate_BadRequest(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	cases := []struct {
		database   string
		collection string
		fields     Map
		filter     Map
		expMessage string
		status     int
	}{
		{
			"random_project1",
			coll,
			Map{
				"$set": Map{
					"string_value": "simple_update",
				},
			},
			Map{
				"pkey_int": 1,
			},
			"project doesn't exist 'random_project1'",
			http.StatusNotFound,
		}, {
			db,
			"random_collection",
			Map{
				"$set": Map{
					"string_value": "simple_update",
				},
			},
			Map{
				"pkey_int": 1,
			},
			"collection doesn't exist 'random_collection'",
			http.StatusNotFound,
		}, {
			"",
			coll,
			Map{
				"$set": Map{
					"string_value": "simple_update",
				},
			},
			Map{
				"pkey_int": 1,
			},
			"invalid database name",
			http.StatusBadRequest,
		}, {
			db,
			"",
			Map{
				"$set": Map{
					"string_value": "simple_update",
				},
			},
			Map{
				"pkey_int": 1,
			},
			"invalid collection name",
			http.StatusBadRequest,
		}, {
			db,
			coll,
			nil,
			Map{
				"pkey_int": 1,
			},
			"empty fields received",
			http.StatusBadRequest,
		}, {
			db,
			coll,
			Map{
				"$set": Map{
					"string_value": "simple_update",
				},
			},
			nil,
			"filter is a required field",
			http.StatusBadRequest,
		},
		{
			db,
			coll,
			Map{
				"$push": Map{
					"simple_array_value": 1,
				},
			},
			Map{
				"pkey_int": 1,
			},
			"json schema validation failed for field 'simple_array_value/0' reason 'expected string or null, but got number'",
			http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		resp := expect(t).PUT(getDocumentURL(c.database, c.collection, "update")).
			WithJSON(Map{
				"fields": c.fields,
				"filter": c.filter,
			}).
			Expect()

		code := api.Code_INVALID_ARGUMENT
		if c.status == http.StatusNotFound {
			code = api.Code_NOT_FOUND
		}
		testError(resp, c.status, code, c.expMessage)
	}
}

func TestUpdate_SingleRow(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     100,
			"int_value":    100,
			"string_value": "simple_insert1_update",
			"bool_value":   true,
			"double_value": 100.00001,
			"bytes_value":  []byte(`"simple_insert1_update"`),
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		inputDocument)

	tstart := time.Now().UTC()
	updateByFilter(t,
		db,
		coll,
		Map{
			"filter": Map{
				"pkey_int": 100,
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"int_value":    200,
					"string_value": "simple_insert1_update_modified",
					"bool_value":   false,
					"double_value": 200.00001,
					"bytes_value":  []byte(`"simple_insert1_update_modified"`),
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1).
		Path("$.metadata").Object().
		Value("updated_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		[]Doc{
			{
				"pkey_int":     100,
				"int_value":    200,
				"string_value": "simple_insert1_update_modified",
				"bool_value":   false,
				"double_value": 200.00001,
				"bytes_value":  []byte(`"simple_insert1_update_modified"`),
			},
		})
}

func TestUpdate_Int64AsString(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":  100,
			"int_value": 100,
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		inputDocument)

	tstart := time.Now().UTC()
	updateByFilter(t,
		db,
		coll,
		Map{
			"filter": Map{
				"pkey_int": 100,
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"int_value": "9223372036854775577",
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1).
		Path("$.metadata").Object().
		Value("updated_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		[]Doc{
			{
				"pkey_int":  100,
				"int_value": 9223372036854775577,
			},
		})
}

func TestUpdate_NullField(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     100,
			"int_value":    100,
			"string_value": "simple_insert1_update",
			"bool_value":   true,
			"double_value": 100.00001,
			"bytes_value":  []byte(`"simple_insert1_update"`),
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		inputDocument)

	tstart := time.Now().UTC()
	updateByFilter(t,
		db,
		coll,
		Map{
			"filter": Map{
				"pkey_int": 100,
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"int_value":    nil,
					"string_value": "simple_insert1_update_modified",
					"bool_value":   false,
					"double_value": 200.00001,
					"bytes_value":  []byte(`"simple_insert1_update_modified"`),
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1).
		Path("$.metadata").Object().
		Value("updated_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		[]Doc{
			{
				"pkey_int":     100,
				"int_value":    nil,
				"string_value": "simple_insert1_update_modified",
				"bool_value":   false,
				"double_value": 200.00001,
				"bytes_value":  []byte(`"simple_insert1_update_modified"`),
			},
		})
}

func TestUpdate_SchemaValidationError(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     100,
			"int_value":    100,
			"string_value": "simple_insert1_update",
			"bool_value":   true,
			"double_value": 100.00001,
			"bytes_value":  []byte(`"simple_insert1_update"`),
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		inputDocument)

	cases := []struct {
		documents  Map
		expMessage string
		expCode    api.Code
	}{
		{
			Map{
				"$set": Map{
					"string_value": 1,
				},
			},
			"json schema validation failed for field 'string_value' reason 'expected string or null, but got number'",
			api.Code_INVALID_ARGUMENT,
		}, {
			Map{
				"$set": Map{
					"int_value": 1.1,
				},
			},
			"json schema validation failed for field 'int_value' reason 'expected integer or null, but got number'",
			api.Code_INVALID_ARGUMENT,
		},
	}
	for _, c := range cases {
		resp := expect(t).PUT(getDocumentURL(db, coll, "update")).
			WithJSON(Map{
				"fields": c.documents,
				"filter": Map{
					"pkey_int": 1,
				},
			}).Expect()
		testError(resp, http.StatusBadRequest, c.expCode, c.expMessage)
	}
}

func TestUpdate_MultipleRows(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     110,
			"int_value":    1000,
			"string_value": "simple_insert110",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert110"`),
		},
		{
			"pkey_int":     120,
			"int_value":    2000,
			"string_value": "simple_insert120",
			"bool_value":   false,
			"double_value": 2000.22221,
			"bytes_value":  []byte(`"simple_insert120"`),
		},
		{
			"pkey_int":     130,
			"int_value":    3000,
			"string_value": "simple_insert130",
			"bool_value":   true,
			"double_value": 3000.999999,
			"bytes_value":  []byte(`"simple_insert130"`),
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$or": []Doc{
			{"pkey_int": 110},
			{"pkey_int": 120},
			{"pkey_int": 130},
		},
	}
	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		inputDocument)

	// first try updating a no-op operation i.e. random filter value
	updateByFilter(t,
		db,
		coll,
		Map{
			"filter": Map{
				"pkey_int": 10000,
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"int_value": 0,
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "updated")

	// read all documents back
	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		inputDocument)

	// Update keys 120 and 130
	updateByFilter(t,
		db,
		coll,
		Map{
			"filter": Map{
				"$or": []Doc{
					{"pkey_int": 120},
					{"pkey_int": 130},
				},
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"int_value":          12345,
					"string_value":       "modified_120_130",
					"added_value_double": 1234.999999,
					"added_string_value": "new_key_added",
					"bytes_value":        []byte(`"modified_120_130"`),
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 2)

	outDocument := []Doc{
		// this didn't change as-is
		{
			"pkey_int":     110,
			"int_value":    1000,
			"string_value": "simple_insert110",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert110"`),
		},
		{
			"pkey_int":           120,
			"int_value":          12345,
			"string_value":       "modified_120_130",
			"bool_value":         false,
			"double_value":       2000.22221,
			"bytes_value":        []byte(`"modified_120_130"`),
			"added_value_double": 1234.999999,
			"added_string_value": "new_key_added",
		},
		{
			"pkey_int":           130,
			"int_value":          12345,
			"string_value":       "modified_120_130",
			"bool_value":         true,
			"double_value":       3000.999999,
			"bytes_value":        []byte(`"modified_120_130"`),
			"added_value_double": 1234.999999,
			"added_string_value": "new_key_added",
		},
	}
	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		outDocument)
}

func TestUpdate_Limit(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     110,
			"int_value":    1000,
			"string_value": "simple_insert110",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert110"`),
		},
		{
			"pkey_int":     120,
			"int_value":    2000,
			"string_value": "simple_insert120",
			"bool_value":   false,
			"double_value": 2000.22221,
			"bytes_value":  []byte(`"simple_insert120"`),
		},
		{
			"pkey_int":     130,
			"int_value":    3000,
			"string_value": "simple_insert130",
			"bool_value":   true,
			"double_value": 3000.999999,
			"bytes_value":  []byte(`"simple_insert130"`),
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$or": []Doc{
			{"pkey_int": 110},
			{"pkey_int": 120},
			{"pkey_int": 130},
		},
	}
	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		inputDocument)

	filter := Map{
		"filter": Map{
			"$or": []Doc{
				{"pkey_int": 110},
				{"pkey_int": 120},
				{"pkey_int": 130},
			},
		},
	}
	fields := Map{
		"fields": Map{
			"$set": Map{
				"int_value": 12345,
			},
		},
	}

	payload := make(Map)
	for key, value := range filter {
		payload[key] = value
	}
	for key, value := range fields {
		payload[key] = value
	}
	payload["options"] = Map{
		"limit": 1,
	}

	e := expect(t)
	e.PUT(getDocumentURL(db, coll, "update")).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1)
}

func TestUpdate_UsingCollation(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)
	inputDocument := []Doc{
		{
			"pkey_int":     110,
			"int_value":    1,
			"string_value": "lower",
		},
		{
			"pkey_int":     120,
			"int_value":    2,
			"string_value": "loweR",
		},
		{
			"pkey_int":     130,
			"int_value":    3,
			"string_value": "upper",
		},
		{
			"pkey_int":     140,
			"int_value":    4,
			"string_value": "UPPER",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		nil,
		nil,
		inputDocument)

	updateByFilter(t,
		db,
		coll,
		Map{
			"filter": Map{
				"string_value": "lower",
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"int_value": 100,
				},
			},
		},
		Map{
			"collation": Map{
				"case": "ci",
			},
		}).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "updated").
		ValueEqual("modified_count", 2)
	outDocument := []Doc{
		{
			"pkey_int":     110,
			"int_value":    100,
			"string_value": "lower",
		},
		{
			"pkey_int":     120,
			"int_value":    100,
			"string_value": "loweR",
		},
		{
			"pkey_int":     130,
			"int_value":    3,
			"string_value": "upper",
		},
		{
			"pkey_int":     140,
			"int_value":    4,
			"string_value": "UPPER",
		},
	}

	readAndValidate(t,
		db,
		coll,
		nil,
		nil,
		outDocument)
}

func TestUpdate_OnAnyField(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)
	inputDocument := []Doc{
		{
			"pkey_int":           110,
			"int_value":          1000,
			"string_value":       "simple_insert110",
			"bool_value":         true,
			"double_value":       1000.000001,
			"bytes_value":        []byte(`"simple_insert110"`),
			"added_string_value": "before",
		},
		{
			"pkey_int":           120,
			"int_value":          2000,
			"string_value":       "simple_insert120_130",
			"bool_value":         false,
			"double_value":       2000.222,
			"bytes_value":        []byte(`"simple_insert120"`),
			"added_string_value": "before",
		},
		{
			"pkey_int":           130,
			"int_value":          3000,
			"string_value":       "simple_insert120_130",
			"bool_value":         true,
			"double_value":       3000.999999,
			"bytes_value":        []byte(`"simple_insert130"`),
			"added_string_value": "before",
		},
	}

	cases := []struct {
		filter   Map
		modified int
		changed  []int
	}{
		{
			Map{
				"filter": Map{
					"int_value": 1000,
				},
			},
			1,
			[]int{0},
		}, {
			Map{
				"filter": Map{
					"string_value": "simple_insert110",
				},
			},
			1,
			[]int{0},
		}, {
			Map{
				"filter": Map{
					"double_value": 2000.222,
				},
			},
			1,
			[]int{1},
		}, {
			Map{
				"filter": Map{
					"string_value": "simple_insert120_130",
				},
			},
			2,
			[]int{1, 2},
		},
	}
	for _, c := range cases {
		// should always succeed with mustNotExists as false
		insertDocuments(t, db, coll, inputDocument, false).
			Status(http.StatusOK)

		readFilter := Map{
			"$or": []Doc{
				{"string_value": "simple_insert110"},
				{"int_value": 2000},
				{"double_value": 3000.999999},
			},
		}
		readAndValidatePkeyOrder(t,
			db,
			coll,
			readFilter,
			nil,
			inputDocument,
			nil)

		updateByFilter(t,
			db,
			coll,
			c.filter,
			Map{
				"fields": Map{
					"$set": Map{
						"added_string_value": "after",
					},
				},
			},
			nil).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "updated").
			ValueEqual("modified_count", c.modified)

		// read all documents back
		testUpdateValidate(t, db, coll, readFilter, inputDocument, c.changed)
	}
}

func testUpdateValidate(t *testing.T, db string, collection string, filter Map, input []Doc, changed []int) {
	out := readByFilter(t, db, collection, filter, nil, nil, []Map{{"pkey_int": "$asc"}})

	var notChanged []int
	for i := range input {
		found := false
		for _, c := range changed {
			if i == c {
				found = true
				break
			}
		}
		if found {
			continue
		}
		notChanged = append(notChanged, i)
	}

	for _, i := range changed {
		var data map[string]jsoniter.RawMessage
		require.NoError(t, jsoniter.Unmarshal(out[i]["result"], &data))

		var doc map[string]jsoniter.RawMessage
		require.NoError(t, jsoniter.Unmarshal(data["data"], &doc))

		require.Equal(t, "\"after\"", string(doc["added_string_value"]))
	}

	for _, i := range notChanged {
		validateInputDocToRes(t, out[i], input[i], filter)
	}
}

func TestUpdate_Range(t *testing.T) {
	db, coll := setupTests(t)
	compositeColl := coll + "_composite"
	createCollection(t, db, compositeColl, testCreateSchemaComposite).Status(http.StatusOK)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":           110,
			"int_value":          1000,
			"string_value":       "simple_insert110",
			"bool_value":         true,
			"double_value":       1.234,
			"bytes_value":        []byte(`"simple_insert110"`),
			"added_string_value": "before",
		},
		{
			"pkey_int":           120,
			"int_value":          2000,
			"string_value":       "simple_insert120",
			"bool_value":         false,
			"double_value":       7.3,
			"bytes_value":        []byte(`"simple_insert120"`),
			"added_string_value": "before",
		},
		{
			"pkey_int":           130,
			"int_value":          3000,
			"string_value":       "simple_insert130",
			"bool_value":         true,
			"double_value":       3.34,
			"bytes_value":        []byte(`"simple_insert130"`),
			"added_string_value": "before",
		},
	}

	cases := []struct {
		filter   Map
		modified int
		changed  []int
	}{
		{
			Map{
				"filter": Map{
					"$and": []Doc{
						{"double_value": Map{"$gt": 1.23}},
						{"double_value": Map{"$lte": 3.34}},
					},
				},
			},
			2,
			[]int{0, 2},
		}, {
			Map{
				"filter": Map{
					"$and": []Doc{
						{"string_value": Map{"$gt": "simple_insert110"}},
						{"string_value": Map{"$lte": "simple_insert130"}},
					},
				},
			},
			2,
			[]int{1, 2},
		}, {
			Map{
				"filter": Map{
					"$and": []Doc{
						{"int_value": Map{"$gte": 1000}},
						{"int_value": Map{"$lt": 3000}},
					},
				},
			},
			2,
			[]int{0, 1},
		}, {
			Map{
				"filter": Map{
					"$and": []Doc{
						{"pkey_int": Map{"$gt": 110}},
						{"pkey_int": Map{"$lte": 130}},
					},
				},
			},
			2,
			[]int{1, 2},
		},
	}
	for _, c := range cases {
		for _, collName := range []string{coll, compositeColl} {
			// should always succeed with mustNotExists as false
			insertDocuments(t, db, collName, inputDocument, false).
				Status(http.StatusOK)

			readFilter := Map{
				"$and": []Doc{
					{"pkey_int": Map{"$gte": 110}},
					{"pkey_int": Map{"$lte": 130}},
				},
			}
			readAndValidatePkeyOrder(t,
				db,
				collName,
				readFilter,
				nil,
				inputDocument,
				nil)

			updateByFilter(t,
				db,
				collName,
				c.filter,
				Map{
					"fields": Map{
						"$set": Map{
							"added_string_value": "after",
						},
					},
				},
				nil).Status(http.StatusOK).
				JSON().
				Object().
				ValueEqual("status", "updated").
				ValueEqual("modified_count", c.modified)

			// read all documents back
			testUpdateValidate(t, db, collName, readFilter, inputDocument, c.changed)
		}
	}
}

func TestUpdate_SetAndUnset(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     100,
			"int_value":    100,
			"string_value": "simple_insert1_update",
			"bool_value":   true,
			"double_value": 100.00001,
			"bytes_value":  []byte(`"simple_insert1_update"`),
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		inputDocument)

	cases := []struct {
		userInput Map
		expOut    []Doc
	}{
		{
			Map{
				"fields": Map{
					"$set": Map{
						"int_value":    200,
						"string_value": "simple_insert1_update_modified",
						"bool_value":   false,
						"double_value": 200.00001,
						"bytes_value":  []byte(`"simple_insert1_update_modified"`),
					},
					"$unset": []string{"string_value", "bytes_value"},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"int_value":    200,
				"bool_value":   false,
				"double_value": 200.00001,
			}},
		},
		{
			Map{
				"fields": Map{
					"$set": Map{
						"string_value": "string2",
					},
					"$unset": []string{"int_value", "bytes_value"},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"bool_value":   false,
				"double_value": 200.00001,
				"string_value": "string2",
			}},
		},
		{
			Map{
				"fields": Map{
					"$set": Map{
						"int_value":   400,
						"bytes_value": []byte(`"bytes3"`),
					},
					"$unset": []string{"string_value", "bool_value", "double_value"},
				},
			},
			[]Doc{{
				"pkey_int":    100,
				"int_value":   400,
				"bytes_value": []byte(`"bytes3"`),
			}},
		},
	}
	for _, c := range cases {
		tstart := time.Now().UTC()
		updateByFilter(t,
			db,
			coll,
			Map{
				"filter": Map{
					"pkey_int": 100,
				},
			},
			c.userInput,
			nil).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("modified_count", 1).
			Path("$.metadata").Object().
			Value("updated_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

		readAndValidate(t,
			db,
			coll,
			Map{
				"pkey_int": 100,
			},
			nil,
			c.expOut)
	}
}

func TestUpdate_Push(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     100,
			"int_value":    100,
			"string_value": "simple_insert1_update",
			"bool_value":   true,
			"double_value": 100.00001,
			"bytes_value":  []byte(`"simple_insert1_update"`),
			"array_value": []Doc{
				{
					"id":      9223372036854775801,
					"product": "foo",
				},
			},
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		inputDocument)

	cases := []struct {
		userInput Map
		expOut    []Doc
	}{
		{
			Map{
				"fields": Map{
					"$push": Map{
						"array_value": Map{
							"id":      9223372036854775123,
							"product": "bar",
						},
					},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"int_value":    100,
				"string_value": "simple_insert1_update",
				"bool_value":   true,
				"double_value": 100.00001,
				"bytes_value":  []byte(`"simple_insert1_update"`),
				"array_value": []Doc{
					{
						"id":      float64(9223372036854775801),
						"product": "foo",
					},
					{
						"id":      float64(9223372036854775123),
						"product": "bar",
					},
				},
			}},
		},
		{
			Map{
				"fields": Map{
					"$push": Map{
						"simple_array_value": "hello world",
					},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"int_value":    100,
				"string_value": "simple_insert1_update",
				"bool_value":   true,
				"double_value": 100.00001,
				"bytes_value":  []byte(`"simple_insert1_update"`),
				"array_value": []Doc{
					{
						"id":      float64(9223372036854775801),
						"product": "foo",
					},
					{
						"id":      float64(9223372036854775123),
						"product": "bar",
					},
				},
				"simple_array_value": []string{"hello world"},
			}},
		},
		{
			Map{
				"fields": Map{
					"$push": Map{
						"array_value": Map{
							"id":      "9223372036854775456",
							"product": "table",
						},
						"simple_array_value": "abc def",
					},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"int_value":    100,
				"string_value": "simple_insert1_update",
				"bool_value":   true,
				"double_value": 100.00001,
				"bytes_value":  []byte(`"simple_insert1_update"`),
				"array_value": []Doc{
					{
						"id":      float64(9223372036854775801),
						"product": "foo",
					},
					{
						"id":      float64(9223372036854775123),
						"product": "bar",
					},
					{
						"id":      float64(9223372036854775456),
						"product": "table",
					},
				},
				"simple_array_value": []string{"hello world", "abc def"},
			}},
		},
	}
	for _, c := range cases {
		tstart := time.Now().UTC()
		updateByFilter(t,
			db,
			coll,
			Map{
				"filter": Map{
					"pkey_int": 100,
				},
			},
			c.userInput,
			nil).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("modified_count", 1).
			Path("$.metadata").Object().
			Value("updated_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

		readAndValidate(t,
			db,
			coll,
			Map{
				"pkey_int": 100,
			},
			nil,
			c.expOut)
	}
}

func TestUpdate_AtomicOperations(t *testing.T) {
	cases := []struct {
		userInput Map
		expOut    []Doc
	}{
		{
			Map{
				"fields": Map{
					"$increment": Map{
						"int_value":          1,
						"double_value":       1.1,
						"added_value_double": 1, // this should not present, should be ignored
					},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"int_value":    3,
				"bool_value":   true,
				"double_value": 3.2,
				"string_value": "simple_insert1_update",
			}},
		},
		{
			Map{
				"fields": Map{
					"$decrement": Map{
						"int_value":    1,
						"double_value": 1.1,
					},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"int_value":    1,
				"bool_value":   true,
				"double_value": 1.0,
				"string_value": "simple_insert1_update",
			}},
		},
		{
			Map{
				"fields": Map{
					"$multiply": Map{
						"int_value":    5,
						"double_value": 2.1,
					},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"int_value":    10,
				"bool_value":   true,
				"double_value": 4.41,
				"string_value": "simple_insert1_update",
			}},
		},
		{
			Map{
				"fields": Map{
					"$divide": Map{
						"int_value":    2,
						"double_value": 2.1,
					},
				},
			},
			[]Doc{{
				"pkey_int":     100,
				"int_value":    1,
				"bool_value":   true,
				"double_value": 1,
				"string_value": "simple_insert1_update",
			}},
		},
	}
	for _, c := range cases {
		testUpdateAtomicOperations(t, c.userInput, c.expOut)
	}

	testUpdateAtomicOperationsFailure(t, Map{
		"fields": Map{
			"$increment": Map{
				"int_value":    1.01,
				"double_value": 1.1,
			},
		}}, 400, "{\"error\":{\"code\":\"INVALID_ARGUMENT\",\"message\":\"floating operations are not allowed on integer field\"}}")

	testUpdateAtomicOperationsFailure(t, Map{
		"fields": Map{
			"$increment": Map{
				"not_present_field": 1,
			},
		}}, 400, "{\"error\":{\"code\":\"INVALID_ARGUMENT\",\"message\":\"Field `not_present_field` is not present in collection\"}}")
}

func testUpdateAtomicOperations(t *testing.T, userInput Map, expOut []Doc) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     100,
			"int_value":    2,
			"bool_value":   true,
			"double_value": 2.1,
			"string_value": "simple_insert1_update",
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	tstart := time.Now().UTC()
	updateByFilter(t,
		db,
		coll,
		Map{
			"filter": Map{
				"pkey_int": 100,
			},
		},
		userInput,
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1).
		Path("$.metadata").Object().
		Value("updated_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readAndValidate(t,
		db,
		coll,
		Map{
			"pkey_int": 100,
		},
		nil,
		expOut)
}

func testUpdateAtomicOperationsFailure(t *testing.T, userInput Map, expErrorCode int, expErrorMsg string) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     100,
			"int_value":    2,
			"bool_value":   true,
			"double_value": 2.1,
			"string_value": "simple_insert1_update",
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	rawMessage := updateByFilter(t,
		db,
		coll,
		Map{
			"filter": Map{
				"pkey_int": 100,
			},
		},
		userInput,
		nil).Status(expErrorCode).Body().Raw()
	require.Equal(t, expErrorMsg, rawMessage)
}

func TestUpdate_MutatePrimaryKey(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_mutatepkey_collection"
	schema := Map{
		"schema": Map{
			"title": collection,
			"properties": Map{
				"a": Map{
					"type": "integer",
				},
				"b": Map{
					"type": "string",
				},
				"c": Map{
					"type": "integer",
				},
				"d": Map{
					"type": "string",
				},
				"e": Map{
					"type": "integer",
				},
			},
			"primary_key": []any{"a", "b"},
		},
	}

	inputDocument := []Doc{
		{
			"a": 1,
			"b": "1",
			"c": 1,
			"d": "1",
			"e": 1,
		},
	}

	cases := []struct {
		userInput          Map
		oldShouldBeRemoved bool
		expOutPKey         Map
		expOut             []Doc
		expError           string
	}{
		{
			Map{
				"fields": Map{
					"$set": Map{
						"b": "2",
					},
				},
			},
			true,
			Map{
				"a": 1,
				"b": "2",
			},
			[]Doc{{
				"a": 1,
				"b": "2",
				"c": 1,
				"d": "1",
				"e": 1,
			}},
			"",
		},
		{
			Map{
				"fields": Map{
					"$set": Map{
						"a": 2,
					},
				},
			},
			true,
			Map{
				"a": 2,
				"b": "1",
			},
			[]Doc{{
				"a": 2,
				"b": "1",
				"c": 1,
				"d": "1",
				"e": 1,
			}},
			"",
		},
		{
			Map{
				"fields": Map{
					"$set": Map{
						"c": 2,
					},
				},
			},
			false,
			Map{
				"a": 1,
				"b": "1",
			},
			[]Doc{{
				"a": 1,
				"b": "1",
				"c": 2,
				"d": "1",
				"e": 1,
			}},
			"",
		},
		{
			Map{
				"fields": Map{
					"$set": Map{
						"d": "2",
					},
				},
			},
			false,
			Map{
				"a": 1,
				"b": "1",
			},
			[]Doc{{
				"a": 1,
				"b": "1",
				"c": 1,
				"d": "2",
				"e": 1,
			}},
			"",
		},
		{
			Map{
				"fields": Map{
					"$set": Map{
						"a": nil,
					},
				},
			},
			false,
			Map{
				"a": 1,
				"b": "1",
			},
			[]Doc{{
				"a": 1,
				"b": "1",
				"c": 1,
				"d": "2",
				"e": 1,
			}},
			"primary key field can't be set as null",
		},
		{
			Map{
				"fields": Map{
					"$unset": []string{"a"},
				},
			},
			true,
			Map{
				"filter": Map{
					"b": "1",
				},
			},
			[]Doc{{
				"b": "1",
				"c": 1,
				"d": "2",
			}},
			"primary key field can't be unset",
		},
		{
			Map{
				"fields": Map{
					"$unset": []string{"b"},
				},
			},
			true,
			Map{
				"a": 1,
			},
			[]Doc{{
				"a": 1,
				"c": 1,
				"d": "2",
			}},
			"primary key field can't be unset",
		},
		{
			Map{
				"fields": Map{
					"$unset": []string{"c", "d"},
				},
			},
			false,
			Map{
				"a": 1,
				"b": "1",
			},
			[]Doc{{
				"a": 1,
				"b": "1",
				"e": 1,
			}},
			"",
		},
	}
	for _, c := range cases {
		createCollection(t, db, collection, schema).Status(200)

		insertDocuments(t, db, collection, inputDocument, false).
			Status(http.StatusOK)

		if len(c.expError) == 0 {
			updateByFilter(t,
				db,
				collection,
				Map{
					"filter": Map{
						"e": 1,
					},
				},
				c.userInput,
				nil).Status(http.StatusOK).
				JSON().
				Object().
				ValueEqual("modified_count", 1).
				Path("$.metadata").Object()
		} else {
			updateByFilter(t,
				db,
				collection,
				Map{
					"filter": Map{
						"e": 1,
					},
				},
				c.userInput,
				nil).Status(http.StatusBadRequest).
				JSON().Path("$.error").Object().
				ValueEqual("message", c.expError)

			continue
		}

		readAndValidate(t,
			db,
			collection,
			c.expOutPKey,
			nil,
			c.expOut)

		dropCollection(t, db, collection)
	}
}

func TestUpdate_Object(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_update_collection"
	schema := Map{
		"schema": Map{
			"title": collection,
			"properties": Map{
				"a": Map{
					"type": "integer",
				},
				"b": Map{
					"type": "string",
				},
				"c": Map{
					"type": "object",
					"properties": Map{
						"f1": Map{
							"type": "string",
						},
						"f2": Map{
							"type": "string",
						},
					},
				},
			},
			"primary_key": []any{"a"},
		},
	}
	createCollection(t, db, collection, schema).Status(200)

	inputDocument := []Doc{
		{
			"a": 1,
			"b": "1",
			"c": Doc{"f1": "A", "f2": "B"},
		},
	}

	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	updateByFilter(t,
		db,
		collection,
		Map{
			"filter": Map{
				"b": "1",
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"c": Doc{"f1": "F1"},
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1).
		Path("$.metadata").Object()

	readAndValidate(t,
		db,
		collection,
		Map{
			"b": "1",
		},
		nil,
		[]Doc{{
			"a": 1,
			"b": "1",
			"c": Doc{"f1": "F1"},
		}})
}

func TestUpdate_ObjectDotNotation(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_update_collection"
	schema := Map{
		"schema": Map{
			"title": collection,
			"properties": Map{
				"a": Map{
					"type": "integer",
				},
				"b": Map{
					"type": "string",
				},
				"c": Map{
					"type": "object",
					"properties": Map{
						"f1": Map{
							"type": "string",
						},
						"f2": Map{
							"type": "string",
						},
					},
				},
				"d": Map{
					"type":       "object",
					"properties": Map{},
				},
			},
			"primary_key": []any{"a"},
		},
	}
	createCollection(t, db, collection, schema).Status(200)

	inputDocument := []Doc{
		{
			"a": 1,
			"b": "1",
			"c": Doc{"f1": "A", "f2": "B"},
			"d": Doc{"f1": "A", "f2": "B"},
		},
	}

	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	updateByFilter(t,
		db,
		collection,
		Map{
			"filter": Map{
				"b": "1",
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"c.f1": "updated_A",
					"d.f1": "updated_A",
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1).
		Path("$.metadata").Object()

	readAndValidate(t,
		db,
		collection,
		Map{
			"b": "1",
		},
		nil,
		[]Doc{{
			"a": 1,
			"b": "1",
			"c": Doc{"f1": "updated_A", "f2": "B"},
			"d": Doc{"f1": "updated_A", "f2": "B"},
		}})

	updateByFilter(t,
		db,
		collection,
		Map{
			"filter": Map{
				"b": "1",
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"c.f2": "updated_B",
					"d.f2": "updated_B",
				},
			},
		},
		nil).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("modified_count", 1).
		Path("$.metadata").Object()

	readAndValidate(t,
		db,
		collection,
		Map{
			"b": "1",
		},
		nil,
		[]Doc{{
			"a": 1,
			"b": "1",
			"c": Doc{"f1": "updated_A", "f2": "updated_B"},
			"d": Doc{"f1": "updated_A", "f2": "updated_B"},
		}})
}

func TestDelete_BadRequest(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	cases := []struct {
		databaseName   string
		collectionName string
		filter         Map
		expMessage     string
		status         int
	}{
		{
			"random_project1",
			coll,
			Map{"pkey_int": 1},
			"project doesn't exist 'random_project1'",
			http.StatusNotFound,
		}, {
			db,
			"random_collection",
			Map{"pkey_int": 1},
			"collection doesn't exist 'random_collection'",
			http.StatusNotFound,
		}, {
			"",
			coll,
			Map{"pkey_int": 1},
			"invalid database name",
			http.StatusBadRequest,
		}, {
			db,
			"",
			Map{"pkey_int": 1},
			"invalid collection name",
			http.StatusBadRequest,
		}, {
			db,
			coll,
			nil,
			"filter is a required field",
			http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		resp := expect(t).DELETE(getDocumentURL(c.databaseName, c.collectionName, "delete")).
			WithJSON(Map{
				"filter": c.filter,
			}).
			Expect()

		code := api.Code_INVALID_ARGUMENT
		if c.status == http.StatusNotFound {
			code = api.Code_NOT_FOUND
		}
		testError(resp, c.status, code, c.expMessage)
	}
}

func TestDelete_SingleRow(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":  40,
			"int_value": 10,
		},
	}

	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		Map{"pkey_int": 40},
		nil,
		inputDocument)

	tstart := time.Now().UTC()
	deleteByFilter(t, db, coll, Map{
		"filter": Map{"pkey_int": 40},
	}).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "deleted").
		Path("$.metadata").Object().
		Value("deleted_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readAndValidate(t,
		db,
		coll,
		Map{"pkey_int": 40},
		nil,
		nil)
}

func TestDelete_MultipleRows(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     50,
			"string_value": "simple_insert50",
		},
		{
			"pkey_int":     60,
			"string_value": "simple_insert60",
		},
		{
			"pkey_int":     70,
			"string_value": "simple_insert70",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$or": []Doc{
			{"pkey_int": 50},
			{"pkey_int": 60},
			{"pkey_int": 70},
		},
	}
	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		inputDocument)

	// first try deleting a no-op operation i.e. random filter value
	deleteByFilter(t, db, coll, Map{
		"filter": Map{
			"pkey_int": 10000,
		},
	}).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "deleted")

	// read all documents back
	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		inputDocument)

	// DELETE keys 50 and 70
	deleteByFilter(t, db, coll, Map{
		"filter": Map{
			"$or": []Doc{
				{"pkey_int": 50},
				{"pkey_int": 70},
			},
		},
	}).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "deleted")

	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		[]Doc{
			{
				"pkey_int":     60,
				"string_value": "simple_insert60",
			},
		},
	)
}

func TestRead_BadRequest(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	cases := []struct {
		databaseName   string
		collectionName string
		filter         Map
		expMessage     string
		status         int
	}{
		{
			"random_project1",
			coll,
			Map{"pkey_int": 1},
			"project doesn't exist 'random_project1'",
			http.StatusNotFound,
		}, {
			db,
			"random_collection",
			Map{"pkey_int": 1},
			"collection doesn't exist 'random_collection'",
			http.StatusNotFound,
		}, {
			"",
			coll,
			Map{"pkey_int": 1},
			"invalid database name",
			http.StatusBadRequest,
		}, {
			db,
			"",
			Map{"pkey_int": 1},
			"invalid collection name",
			http.StatusBadRequest,
		}, {
			db,
			coll,
			nil,
			"filter is a required field",
			http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		resp := expect(t).POST(getDocumentURL(c.databaseName, c.collectionName, "read")).
			WithJSON(Map{
				"filter": c.filter,
			}).
			Expect()

		code := api.Code_INVALID_ARGUMENT
		if c.status == http.StatusNotFound {
			code = api.Code_NOT_FOUND
		}
		testError(resp, c.status, code, c.expMessage)
	}
}

func TestDelete_OnAnyField(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     50,
			"int_value":    50,
			"string_value": "simple_insert50",
			"bytes_value":  []byte(`"simple_insert50"`),
		},
		{
			"pkey_int":     70,
			"int_value":    70,
			"string_value": "simple_insert70",
			"double_value": 1.234,
		},
		{
			"pkey_int":     60,
			"int_value":    60,
			"string_value": "simple_insert60",
			"bytes_value":  []byte(`"simple_insert60"`),
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$or": []Doc{
			{"int_value": 50},
			{"string_value": "simple_insert60"},
			{"double_value": 1.234},
		},
	}

	readAndValidatePkeyOrder(t,
		db,
		coll,
		readFilter,
		nil,
		inputDocument,
		nil)

	deleteByFilter(t, db, coll, Map{
		"filter": Map{
			"$or": []Doc{
				{"int_value": 50},
				{"double_value": 1.234},
			},
		},
	}).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "deleted")

	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		[]Doc{
			{
				"pkey_int":     60,
				"int_value":    60,
				"string_value": "simple_insert60",
				"bytes_value":  []byte(`"simple_insert60"`),
			},
		},
	)
}

func TestDelete_Range(t *testing.T) {
	db, coll := setupTests(t)
	compositeColl := coll + "_composite"
	createCollection(t, db, compositeColl, testCreateSchemaComposite).Status(http.StatusOK)

	defer cleanupTests(t, db)

	for _, collName := range []string{coll, compositeColl} {
		inputDocument := []Doc{
			{
				"pkey_int":     50,
				"int_value":    50,
				"string_value": "simple_insert50",
				"bytes_value":  []byte(`"simple_insert50"`),
				"double_value": 1.734,
			},
			{
				"pkey_int":     70,
				"int_value":    70,
				"string_value": "simple_insert70",
				"double_value": 1.234,
			},
			{
				"pkey_int":     60,
				"int_value":    60,
				"string_value": "simple_insert60",
				"bytes_value":  []byte(`"simple_insert60"`),
				"double_value": 2.3,
			},
			{
				"pkey_int":     80,
				"int_value":    80,
				"string_value": "simple_insert80",
				"bytes_value":  []byte(`"simple_insert80"`),
				"double_value": 3.34,
			},
			{
				"pkey_int":     90,
				"int_value":    90,
				"string_value": "simple_insert800",
				"bytes_value":  []byte(`"simple_insert80"`),
				"double_value": 3.12,
			},
		}

		// should always succeed with mustNotExists as false
		insertDocuments(t, db, collName, inputDocument, false).
			Status(http.StatusOK)

		readFilter := Map{
			"$and": []Doc{
				{"double_value": Map{"$gt": 1.23}},
				{"double_value": Map{"$lte": 3.34}},
			},
		}

		readAndValidatePkeyOrder(t,
			db,
			collName,
			readFilter,
			nil,
			inputDocument,
			nil)

		deleteByFilter(t, db, collName, Map{
			"filter": Map{
				"$and": []Doc{
					{"double_value": Map{"$gt": 1.73}},
					{"double_value": Map{"$lte": 3.12}},
				},
			},
		}).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "deleted")

		readAndValidateOrder(t,
			db,
			collName,
			readFilter,
			nil,
			[]Map{
				{
					"pkey_int": "$asc",
				},
			},
			[]Doc{
				{
					"pkey_int":     70,
					"int_value":    70,
					"string_value": "simple_insert70",
					"double_value": 1.234,
				},
				{
					"pkey_int":     80,
					"int_value":    80,
					"string_value": "simple_insert80",
					"bytes_value":  []byte(`"simple_insert80"`),
					"double_value": 3.34,
				},
			},
		)
	}
}

func TestCountDocuments(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     50,
			"string_value": "simple_insert50",
		},
		{
			"pkey_int":     60,
			"string_value": "simple_insert60",
		},
		{
			"pkey_int":     70,
			"string_value": "simple_insert70",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	e := expect(t)
	e.POST(getDocumentURL(db, coll, "count")).
		WithJSON(Map{
			"filter": Map{
				"pkey_int": 50,
			}}).
		Expect().Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("count", 1)

	e.POST(getDocumentURL(db, coll, "count")).
		WithJSON(Map{
			"filter": Map{
				"$or": []Doc{
					{"pkey_int": 50},
					{"pkey_int": 70},
				},
			}}).
		Expect().Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("count", 2)

	e.POST(getDocumentURL(db, coll, "count")).
		WithJSON(Map{
			"filter": Map{},
		}).
		Expect().Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("count", 3)
}

func TestRead_Collation(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     210,
			"string_value": "lower",
		},
		{
			"pkey_int":     220,
			"string_value": "loweR",
		},
		{
			"pkey_int":     230,
			"string_value": "upper",
		},
		{
			"pkey_int":     230,
			"string_value": "UPPER",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	cases := []struct {
		filter       Map
		expDocuments []Doc
	}{
		{
			Map{
				"string_value": Map{
					"$eq": "lower",
				},
			},
			[]Doc{
				{
					"pkey_int":     210,
					"string_value": "lower",
				},
			},
		}, {
			Map{
				"string_value": Map{
					"$eq": "lower",
					"collation": Map{
						"case": "ci",
					},
				},
			},
			[]Doc{
				{
					"pkey_int":     210,
					"string_value": "lower",
				},
				{
					"pkey_int":     220,
					"string_value": "loweR",
				},
			},
		}, {
			Map{
				"string_value": Map{
					"$eq": "UPPER",
					"collation": Map{
						"case": "cs",
					},
				},
			},
			[]Doc{
				{
					"pkey_int":     230,
					"string_value": "UPPER",
				},
			},
		},
	}
	for _, c := range cases {
		readAndValidatePkeyOrder(t,
			db,
			coll,
			c.filter,
			nil,
			c.expDocuments,
			nil)
	}
}

func TestRead_CollationReqLevel(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     210,
			"string_value": "lower",
		},
		{
			"pkey_int":     220,
			"string_value": "loweR",
		},
		{
			"pkey_int":     230,
			"string_value": "upper",
		},
		{
			"pkey_int":     230,
			"string_value": "UPPER",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	cases := []struct {
		collation    Map
		filter       Map
		expDocuments []Doc
	}{
		{
			nil,
			Map{
				"string_value": Map{
					"$eq": "lower",
				},
			},
			[]Doc{
				{
					"pkey_int":     210,
					"string_value": "lower",
				},
			},
		}, {
			Map{
				"collation": Map{
					"case": "ci",
				},
			},
			Map{
				"string_value": Map{
					"$eq": "lower",
				},
			},
			[]Doc{
				{
					"pkey_int":     210,
					"string_value": "lower",
				},
				{
					"pkey_int":     220,
					"string_value": "loweR",
				},
			},
		}, {
			Map{
				"collation": Map{
					"case": "cs",
				},
			},
			Map{
				"string_value": Map{
					"$eq": "UPPER",
				},
			},
			[]Doc{
				{
					"pkey_int":     230,
					"string_value": "UPPER",
				},
			},
		},
	}
	for _, c := range cases {
		readAndValidatePkeyOrder(t,
			db,
			coll,
			c.filter,
			nil,
			c.expDocuments,
			c.collation)
	}
}

func TestRead_MultipleRows(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     210,
			"int_value":    1000,
			"string_value": "simple_insert110",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert110"`),
		},
		{
			"pkey_int":     220,
			"int_value":    2000,
			"string_value": "simple_insert120",
			"bool_value":   false,
			"double_value": 2000.22221,
			"bytes_value":  []byte(`"simple_insert120"`),
		},
		{
			"pkey_int":     230,
			"string_value": "simple_insert130",
			"bytes_value":  []byte(`"simple_insert130"`),
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$or": []Doc{
			{"pkey_int": 210},
			{"pkey_int": 220},
			{"pkey_int": 230},
		},
	}

	cases := []struct {
		fields       Map
		expDocuments []Doc
	}{
		{
			Map{
				"int_value":    1,
				"string_value": 1,
				"bytes_value":  1,
			},
			[]Doc{
				{
					"int_value":    1000,
					"string_value": "simple_insert110",
					"bytes_value":  []byte(`"simple_insert110"`),
				},
				{
					"int_value":    2000,
					"string_value": "simple_insert120",
					"bytes_value":  []byte(`"simple_insert120"`),
				},
				{
					"string_value": "simple_insert130",
					"bytes_value":  []byte(`"simple_insert130"`),
				},
			},
		}, {
			// bool is not present in the third document
			Map{
				"string_value": 1,
				"bool_value":   1,
			},
			[]Doc{
				{
					"string_value": "simple_insert110",
					"bool_value":   true,
				},
				{
					"string_value": "simple_insert120",
					"bool_value":   false,
				},
				{
					"string_value": "simple_insert130",
				},
			},
		}, {
			// both are not present in the third document
			Map{
				"double_value": 1,
				"bool_value":   1,
			},
			[]Doc{
				{
					"double_value": 1000.000001,
					"bool_value":   true,
				},
				{
					"double_value": 2000.22221,
					"bool_value":   false,
				},
				{},
			},
		},
	}
	for _, c := range cases {
		readAndValidate(t,
			db,
			coll,
			readFilter,
			c.fields,
			c.expDocuments)
	}
}

func TestRead_RangePKey(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_rangepkey_collection"
	schema := Map{
		"schema": Map{
			"title": collection,
			"properties": Map{
				"Id": Map{
					"type": "integer",
				},
				"name": Map{
					"type": "string",
				},
			},
			"primary_key": []any{"Id"},
		},
	}
	createCollection(t, db, collection, schema).Status(200)

	inputDocument := []Doc{
		{
			"Id":   1,
			"name": "A",
		},
		{
			"Id":   2,
			"name": "B",
		},
		{
			"Id":   3,
			"name": "C",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$and": []Doc{
			{"Id": Map{"$gt": 1}},
			{"Id": Map{"$lt": 3}},
		},
	}

	readAndValidate(t,
		db,
		collection,
		readFilter,
		nil,
		inputDocument[1:2])
}

func TestRead_SortPKey(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_sortpkey_collection"
	schema := Map{
		"schema": Map{
			"title": collection,
			"properties": Map{
				"createdAt": Map{
					"type": "string",
				},
				"name": Map{
					"type": "string",
				},
			},
			"primary_key": []interface{}{"createdAt"},
		},
	}
	createCollection(t, db, collection, schema).Status(200)

	inputDocument := []Doc{
		{
			"createdAt": "2023-04-23T01:04:17Z",
			"name":      "foo",
		},
		{
			"createdAt": "2023-04-21T01:04:17Z",
			"name":      "bar",
		},
		{
			"createdAt": "2023-04-25T01:04:17Z",
			"name":      "foo",
		},
		{
			"name":      "bar",
			"createdAt": "2023-04-20T01:04:17Z",
		}, {
			"createdAt": "2023-04-23T01:05:17Z",
			"name":      "foo",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	readAndValidateOrder(t,
		db,
		collection,
		Map{
			"name": "foo",
		},
		nil,
		[]Map{
			{
				"createdAt": "$desc",
			},
		},
		append([]Doc{inputDocument[2]}, inputDocument[4], inputDocument[0]),
	)

	readAndValidateOrder(t,
		db,
		collection,
		Map{
			"name": "bar",
		},
		nil,
		[]Map{
			{
				"createdAt": "$desc",
			},
		},
		append([]Doc{inputDocument[1]}, inputDocument[3]),
	)
}

func TestRead_SortPKeyComposite(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_sortpkey_collection"
	schema := Map{
		"schema": Map{
			"title": collection,
			"properties": Map{
				"tenant": Map{
					"type": "string",
				},
				"createdAt": Map{
					"type": "string",
				},
				"name": Map{
					"type": "string",
				},
			},
			"primary_key": []interface{}{"tenant", "createdAt"},
		},
	}
	createCollection(t, db, collection, schema).Status(200)

	inputDocument := []Doc{
		{
			"tenant":    "A",
			"createdAt": "2023-04-25T01:04:17Z",
			"name":      "foo",
		},
		{
			"tenant":    "B",
			"createdAt": "2023-04-21T01:04:17Z",
			"name":      "bar_bar",
		},
		{
			"tenant":    "C",
			"createdAt": "2023-04-23T01:04:17Z",
			"name":      "bar_foo",
		},
		{
			"tenant":    "A",
			"name":      "bar",
			"createdAt": "2023-04-20T01:04:17Z",
		}, {
			"tenant":    "B",
			"createdAt": "2023-04-23T01:05:17Z",
			"name":      "foo_bar",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	readAndValidateOrder(t,
		db,
		collection,
		Map{
			"tenant": "B",
		},
		nil,
		[]Map{
			{
				"createdAt": "$desc",
			},
		},
		append([]Doc{inputDocument[4]}, inputDocument[1]),
	)

	readAndValidateOrder(t,
		db,
		collection,
		Map{
			"tenant": "A",
		},
		nil,
		[]Map{
			{
				"createdAt": "$desc",
			},
		},
		append([]Doc{inputDocument[0]}, inputDocument[3]),
	)
}

func TestRead_RangePKeyInternalIdField(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_rangepkey_collection"
	schema := Map{
		"schema": Map{
			"title": collection,
			"properties": Map{
				"id": Map{
					"type": "integer",
				},
				"name": Map{
					"type": "string",
				},
			},
		},
	}
	createCollection(t, db, collection, schema).Status(200)

	inputDocument := []Doc{
		{
			"id":   1,
			"name": "A",
		},
		{
			"id":   2,
			"name": "B",
		},
		{
			"id":   3,
			"name": "C",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$and": []Doc{
			{"id": Map{"$gt": 1}},
			{"id": Map{"$lt": 3}},
		},
	}

	readAndValidate(t,
		db,
		collection,
		readFilter,
		nil,
		inputDocument[1:2])
}

func TestRead_EntireCollection(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     1000,
			"int_value":    1000,
			"string_value": "simple_insert1000",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert1000"`),
		},
		{
			"pkey_int":     1010,
			"int_value":    2000,
			"string_value": "simple_insert1010",
			"bool_value":   false,
			"double_value": 2000.22221,
			"bytes_value":  []byte(`"simple_insert1010"`),
		},
		{
			"pkey_int":     1020,
			"string_value": "simple_insert1020",
			"bytes_value":  []byte(`"simple_insert1020"`),
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		db,
		coll,
		nil,
		nil,
		inputDocument)
}

func TestRead_LimitSkip(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     1000,
			"int_value":    1000,
			"string_value": "simple_insert1000",
		},
		{
			"pkey_int":     2000,
			"int_value":    2000,
			"string_value": "simple_insert2000",
		},
		{
			"pkey_int":     3000,
			"string_value": "simple_insert3000",
			"bytes_value":  []byte(`"simple_insert3000"`),
		},
		{
			"pkey_int":     4000,
			"string_value": "simple_insert4000",
			"bytes_value":  []byte(`"simple_insert4000"`),
		},
		{
			"pkey_int":     5000,
			"string_value": "simple_insert5000",
			"bytes_value":  []byte(`"simple_insert5000"`),
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	readAndValidateWithOptions(t,
		db,
		coll,
		nil,
		nil,
		Map{
			"limit": 2,
		},
		inputDocument[0:2])

	readAndValidateWithOptions(t,
		db,
		coll,
		nil,
		nil,
		Map{
			"limit": 2,
			"skip":  2,
		},
		inputDocument[2:4])

	readAndValidateWithOptions(t,
		db,
		coll,
		nil,
		nil,
		Map{
			"limit": 2,
			"skip":  4,
		},
		inputDocument[4:])
}

func TestRead_NestedFields(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	dropCollection(t, db, coll)
	createCollection(t, db, coll,
		Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"id": Map{"type": "integer"},
					"object_field": Map{
						"type": "object",
						"properties": Map{
							"nested_id":  Map{"type": "integer"},
							"nested_str": Map{"type": "string"},
						},
					},
				},
			},
		}).Status(http.StatusOK)

	inputDoc := []Doc{{"id": 1, "object_field": Map{"nested_id": 1, "nested_str": "foo"}}, {"id": 2, "object_field": Map{"nested_id": 2, "nested_str": "bar"}}}
	insertDocuments(t, db, coll, inputDoc, true).
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	readResp := readByFilter(t,
		db,
		coll,
		Map{
			"object_field.nested_str": "bar",
		},
		nil,
		nil,
		nil)

	var doc map[string]jsoniter.RawMessage
	require.Equal(t, 1, len(readResp))
	require.NoError(t, jsoniter.Unmarshal(readResp[0]["result"], &doc))

	actualDoc := []byte(doc["data"])
	expDoc, err := jsoniter.Marshal(inputDoc[1])
	require.NoError(t, err)
	require.JSONEq(t, string(expDoc), string(actualDoc))
}

func TestRead_AcceptApplicationJSON(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	inputDocument := []Doc{
		{
			"pkey_int":     210,
			"int_value":    1000,
			"string_value": "simple_insert110",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert110"`),
		},
		{
			"pkey_int":     220,
			"int_value":    2000,
			"string_value": "simple_insert120",
			"bool_value":   false,
			"double_value": 2000.22221,
			"bytes_value":  []byte(`"simple_insert120"`),
		},
		{
			"pkey_int":     230,
			"string_value": "simple_insert130",
			"bytes_value":  []byte(`"simple_insert130"`),
		},
		{
			"pkey_int":     240,
			"string_value": "simple_insert140",
			"bytes_value":  []byte(`"simple_insert140"`),
		},
		{
			"pkey_int":     250,
			"string_value": "simple_insert150",
			"bytes_value":  []byte(`"simple_insert150"`),
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	cases := []struct {
		filter       Map
		limitAndSkip Map
		expDocuments []int
	}{
		{
			Map{
				"$or": []Doc{
					{"pkey_int": 210},
					{"pkey_int": 220},
					{"pkey_int": 230},
				},
			},
			Map{
				"limit": 2,
			},
			[]int{0, 1},
		}, {
			Map{},
			Map{
				"limit": 2,
			},
			[]int{0, 1},
		}, {
			Map{},
			Map{
				"limit": 2,
				"skip":  2,
			},
			[]int{2, 3},
		}, {
			Map{},
			Map{
				"limit": 10,
			},
			[]int{0, 1, 2, 3, 4},
		}, {
			Map{},
			Map{
				"limit": 5,
				"skip":  5,
			},
			nil,
		},
	}
	for _, c := range cases {
		var expDocuments []Doc
		for _, idx := range c.expDocuments {
			expDocuments = append(expDocuments, inputDocument[idx])
		}
		readAndValidateAcceptApplicationJSON(t,
			db,
			coll,
			c.filter,
			nil,
			c.limitAndSkip,
			nil,
			expDocuments)
	}
}

func TestTransaction_BadID(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	e := expect(t)
	r := e.POST(fmt.Sprintf("/v1/projects/%s/database/transactions/begin", db)).
		Expect().Status(http.StatusOK).
		Body().Raw()

	res := struct {
		TxCtx api.TransactionCtx `json:"tx_ctx"`
	}{}

	err := jsoniter.Unmarshal([]byte(r), &res)
	require.NoError(t, err)

	resp := e.POST(getDocumentURL(db, coll, "insert")).
		WithJSON(Map{"documents": []Doc{{}}}).
		WithHeader("Tigris-Tx-Id", "some id").
		WithHeader("Tigris-Tx-Origin", res.TxCtx.Origin).Expect()
	testError(resp, http.StatusInternalServerError, api.Code_INTERNAL, "session is gone")
}

func TestFilteringOnArrays_Primitives(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_nested_objects_arrays"
	schema := []byte(`{
  "schema": {
    "properties": {
      "arr_obj": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "name": {
              "type": "string"
            },
            "zipcodes": {
              "type": "array",
              "items": {
                "type": "string"
              }
            }
          }
        }
      },
      "string_arr": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "int_arr": {
        "type": "array",
        "items": {
          "type": "integer"
        }
      },
      "double_arr": {
        "type": "array",
        "items": {
          "type": "number"
        }
      },
      "bool_arr": {
        "type": "array",
        "items": {
          "type": "boolean"
        }
      },
      "arr_of_arr": {
        "type": "array",
        "items": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "obj": {
        "type": "object",
        "properties": {
          "array_obj": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "university": {
                  "type": "string"
                }
              }
            }
          },
          "str_field": {
            "type": "string"
          },
		  "arr_primitive": {
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        }
      },
      "pkey_int": {
        "type": "integer"
	  }
    },
    "primary_key": ["pkey_int"],
    "title": "test_nested_objects_arrays"
  }
}`)
	var schemaObj map[string]any
	require.NoError(t, jsoniter.Unmarshal(schema, &schemaObj))
	createCollection(t, db, collection, schemaObj).Status(200)

	jsonDocuments := []byte(`[
  {
    "pkey_int": 1,
    "arr_of_arr": [
      [
        "shopping",
        "clothes",
        "shoes"
      ],
      [
        "playing",
        "soccerr",
        "tennis"
      ]
    ],
    "obj": {
      "array_obj": [
        {
          "university": "abc@university"
        },
        {
          "university": "cal@university"
        }
      ],
      "str_field": "masters",
      "arr_primitive": [
        "cars",
        "bikes"
      ]
    },
    "arr_obj": [
      {
        "name": "classic",
        "zipcodes": [
          "95008",
          "94089"
        ]
      }
    ],
    "string_arr": [
      "paris",
      "italy"
    ],
    "int_arr": [
      10,
      20
    ],
    "double_arr": [
      11.1,
      12.2
    ],
    "bool_arr": [
      true,
      false
    ]
  },
  {
    "pkey_int": 2,
    "arr_of_arr": [
      [
        "product",
        "branding"
      ],
      [
        "marketing"
      ]
    ],
    "obj": {
      "array_obj": [
        {
          "university": "stan@university"
        },
        {
          "university": "mit@university"
        }
      ],
      "str_field": "bachelors",
      "arr_primitive": [
        "tennis",
        "soccer",
        "football"
      ]
    },
    "arr_obj": [
      {
        "name": "exemplary",
        "zipcodes": [
          "1234"
        ]
      }
    ],
    "string_arr": [
      "switzerland",
      "london"
    ],
    "int_arr": [
      30,
      40
    ],
    "double_arr": [
      21.1,
      22.2
    ],
    "bool_arr": [
      true,
      false
    ]
  },
  {
    "pkey_int": 3,
    "arr_of_arr": [
      [
        "books",
        "novels",
        "product"
      ],
      [
        "system",
        "reading"
      ]
    ],
    "obj": {
      "array_obj": [
        {
          "university": "berk@university"
        },
        {
          "university": "harvard@university"
        }
      ],
      "str_field": "psychology",
      "arr_primitive": [
        "books",
        "music"
      ]
    },
    "arr_obj": [
      {
        "name": "demo",
        "zipcodes": [
          "95123"
        ]
      }
    ],
    "string_arr": [
      "rome",
      "italy"
    ],
    "int_arr": [
      40,
      50
    ],
    "double_arr": [
      51.1,
      52.2
    ],
    "bool_arr": [
      true,
      false
    ]
  }
]`)

	var inputRaw []jsoniter.RawMessage
	require.NoError(t, jsoniter.Unmarshal(jsonDocuments, &inputRaw))
	var inputDocument []Doc
	for _, raw := range inputRaw {
		var doc Doc
		require.NoError(t, jsoniter.Unmarshal(raw, &doc))
		inputDocument = append(inputDocument, doc)
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	// second document
	readAndValidate(t,
		db,
		collection,
		Map{
			"obj.arr_primitive": "tennis",
		},
		nil,
		inputDocument[1:2])

	// first document, string array
	readAndValidate(t,
		db,
		collection,
		Map{
			"string_arr": "paris",
		},
		nil,
		inputDocument[0:1])

	// first document, double array type
	readAndValidate(t,
		db,
		collection,
		Map{
			"double_arr": 11.1,
		},
		nil,
		inputDocument[0:1])

	// third document, complete array match
	readAndValidate(t,
		db,
		collection,
		Map{
			"string_arr": []any{"rome", "italy"},
		},
		nil,
		inputDocument[2:3])

	readAndValidate(t,
		db,
		collection,
		Map{
			"arr_obj.name": "classic",
		},
		nil,
		inputDocument[0:1],
	)
}

func TestFilteringOnArrays(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	collection := "test_nested_objects_arrays"
	schema := []byte(`{
  "schema": {
    "properties": {
      "arr_obj": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "name": {
              "type": "string"
            },
            "zipcodes": {
              "type": "array",
              "items": {
                "type": "string"
              }
            }
          }
        }
      },
      "string_arr": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "int_arr": {
        "type": "array",
        "items": {
          "type": "integer"
        }
      },
      "double_arr": {
        "type": "array",
        "items": {
          "type": "number"
        }
      },
      "bool_arr": {
        "type": "array",
        "items": {
          "type": "boolean"
        }
      },
      "arr_of_arr": {
        "type": "array",
        "items": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "obj": {
        "type": "object",
        "properties": {
          "array_obj": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "university": {
                  "type": "string"
                }
              }
            }
          },
          "str_field": {
            "type": "string"
          },
		  "arr_primitive": {
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        }
      },
      "pkey_int": {
        "type": "integer"
	  }
    },
    "primary_key": ["pkey_int"],
    "title": "test_nested_objects_arrays"
  }
}`)
	var schemaObj map[string]any
	require.NoError(t, jsoniter.Unmarshal(schema, &schemaObj))
	createCollection(t, db, collection, schemaObj).Status(200)

	jsonDocuments := []byte(`[
  {
    "pkey_int": 1,
    "arr_of_arr": [
      [
        "shopping",
        "clothes has braces ( and > ( < and escaped operators or some emoji 😀",
        "shoes"
      ],
      [
        "playing",
        "soccerr",
        "tennis"
      ]
    ],
    "obj": {
      "array_obj": [
        {
          "university": "abc@university"
        },
        {
          "university": "cal@university"
        }
      ],
      "str_field": "masters",
      "arr_primitive": [
        "cars",
        "bikes"
      ]
    },
    "arr_obj": [
      {
        "name": "classic",
        "zipcodes": [
          "95008",
          "94089"
        ]
      }
    ],
    "string_arr": [
      "paris",
      "italy"
    ],
    "int_arr": [
      10,
      20
    ],
    "double_arr": [
      11.1,
      12.2
    ],
    "bool_arr": [
      true,
      false
    ]
  },
  {
    "pkey_int": 2,
    "arr_of_arr": [
      [
        "product",
        "branding"
      ],
      [
        "marketing"
      ]
    ],
    "obj": {
      "array_obj": [
        {
          "university": "stan@university"
        },
        {
          "university": "mit@university"
        }
      ],
      "str_field": "bachelors",
      "arr_primitive": [
        "tennis",
        "soccer",
        "football"
      ]
    },
    "arr_obj": [
      {
        "name": "exemplary",
        "zipcodes": [
          "1234"
        ]
      }
    ],
    "string_arr": [
      "switzerland",
      "london"
    ],
    "int_arr": [
      30,
      40
    ],
    "double_arr": [
      21.1,
      22.2
    ],
    "bool_arr": [
      true,
      false
    ]
  },
  {
    "pkey_int": 3,
    "arr_of_arr": [
      [
        "books",
        "novels",
        "product"
      ],
      [
        "system",
        "reading"
      ]
    ],
    "obj": {
      "array_obj": [
        {
          "university": "berk@university"
        },
        {
          "university": "harvard@university"
        }
      ],
      "str_field": "psychology",
      "arr_primitive": [
        "books",
        "music"
      ]
    },
    "arr_obj": [
      {
        "name": "demo",
        "zipcodes": [
          "95123"
        ]
      }
    ],
    "string_arr": [
      "rome",
      "italy"
    ],
    "int_arr": [
      40,
      50
    ],
    "double_arr": [
      51.1,
      52.2
    ],
    "bool_arr": [
      true,
      false
    ]
  }
]`)

	var inputRaw []jsoniter.RawMessage
	require.NoError(t, jsoniter.Unmarshal(jsonDocuments, &inputRaw))
	var inputDocument []Doc
	for _, raw := range inputRaw {
		var doc Doc
		require.NoError(t, jsoniter.Unmarshal(raw, &doc))
		inputDocument = append(inputDocument, doc)
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, collection, inputDocument, false).
		Status(http.StatusOK)

	cases := []struct {
		filter       Map
		expDocuments []Doc
		order        []Map
	}{
		{
			Map{
				"arr_of_arr": "clothes has braces ( and > ( < and escaped operators or some emoji 😀",
			},
			inputDocument[0:1],
		    nil,
		}, {
			Map{
				"obj.array_obj.university": "cal@university",
			},
			inputDocument[0:1],
		    nil,
		},{
			Map{
				"obj.arr_primitive": "cars",
			},
			inputDocument[0:1],
			nil,
		}, {
			Map{
				"arr_obj.zipcodes": 94089,
			},
			inputDocument[0:1],
			nil,
		}, {
			Map{
				"string_arr": "paris",
			},
			inputDocument[0:1],
			nil,
		}, {
			Map{
				"int_arr": 20,
			},
			inputDocument[0:1],
			nil,
		}, {
			Map{
				"double_arr": 12.2,
			},
			inputDocument[0:1],
			nil,
		}, {
			Map{
				"bool_arr": false,
			},
			inputDocument,
			[]Map{
				{"pkey_int": "$asc"},
			},
		},
	}
	for _, c := range cases {
		readAndValidateOrder(t,
			db,
			collection,
			c.filter,
			nil,
			c.order,
			c.expDocuments)
	}
}

func TestRead_Sorted(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	dropCollection(t, db, coll)
	createCollection(t, db, coll,
		Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"id":           Map{"type": "integer", "searchIndex": true, "sort": true, "index": true},
					"int_value":    Map{"type": "integer"},
					"string_value": Map{"type": "string", "searchIndex": true, "sort": true, "index": true},
					"search_only":  Map{"type": "string", "searchIndex": true, "sort": true},
				},
			},
		}).Status(http.StatusOK)

	inputDocument := []Doc{
		{
			"id":           210,
			"int_value":    1000,
			"string_value": "zab",
			"search_only":  "zab",
		},
		{
			"id":           250,
			"int_value":    2000,
			"string_value": "cef",
			"search_only":  "cef",
		},
		{
			"id":           230,
			"int_value":    2000,
			"string_value": "cbf",
			"search_only":  "cbf",
		},
		{
			"id":           220,
			"int_value":    3000,
			"string_value": "aae",
			"search_only":  "aae",
		},
		{
			"id":           240,
			"int_value":    2000,
			"string_value": "Aae",
			"search_only":  "Aae",
		},
		{
			"id":           260,
			"int_value":    5000,
			"string_value": "aab",
			"search_only":  "zab",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	cases := []struct {
		filters      Map
		sortOrder    []Map
		expDocuments []Doc
	}{
		{
			nil,
			[]Map{
				{
					"id": "$asc",
				},
			},
			[]Doc{
				{
					"id":           210,
					"int_value":    1000,
					"string_value": "zab",
					"search_only":  "zab",
				},
				{
					"id":           220,
					"int_value":    3000,
					"string_value": "aae",
					"search_only":  "aae",
				},
				{
					"id":           230,
					"int_value":    2000,
					"string_value": "cbf",
					"search_only":  "cbf",
				},
				{
					"id":           240,
					"int_value":    2000,
					"string_value": "Aae",
					"search_only":  "Aae",
				},
				{
					"id":           250,
					"int_value":    2000,
					"string_value": "cef",
					"search_only":  "cef",
				},
				{
					"id":           260,
					"int_value":    5000,
					"string_value": "aab",
					"search_only":  "zab",
				},
			},
		},
		{
			nil,
			[]Map{
				{
					"string_value": "$asc",
				},
			},
			[]Doc{
				{
					"id":           260,
					"int_value":    5000,
					"string_value": "aab",
					"search_only":  "zab",
				},
				{
					"id":           220,
					"int_value":    3000,
					"string_value": "aae",
					"search_only":  "aae",
				},
				{
					"id":           240,
					"int_value":    2000,
					"string_value": "Aae",
					"search_only":  "Aae",
				},
				{
					"id":           230,
					"int_value":    2000,
					"string_value": "cbf",
					"search_only":  "cbf",
				},
				{
					"id":           250,
					"int_value":    2000,
					"string_value": "cef",
					"search_only":  "cef",
				},
				{
					"id":           210,
					"int_value":    1000,
					"string_value": "zab",
					"search_only":  "zab",
				},
			},
		},
		{
			Map{"id": Map{"$gte": 0}},
			[]Map{
				{
					"search_only": "$asc",
				},
			},
			[]Doc{
				{
					"id":           240,
					"int_value":    2000,
					"string_value": "Aae",
					"search_only":  "Aae",
				},
				{
					"id":           220,
					"int_value":    3000,
					"string_value": "aae",
					"search_only":  "aae",
				},
				{
					"id":           230,
					"int_value":    2000,
					"string_value": "cbf",
					"search_only":  "cbf",
				},
				{
					"id":           250,
					"int_value":    2000,
					"string_value": "cef",
					"search_only":  "cef",
				},
				{
					"id":           260,
					"int_value":    5000,
					"string_value": "aab",
					"search_only":  "zab",
				},
				{
					"id":           210,
					"int_value":    1000,
					"string_value": "zab",
					"search_only":  "zab",
				},
			},
		},
		{
			Map{"id": Map{"$gte": 0}},
			[]Map{
				{
					"search_only": "$asc",
				},
				{
					"string_value": "$desc",
				},
			},
			[]Doc{
				{
					"id":           240,
					"int_value":    2000,
					"string_value": "Aae",
					"search_only":  "Aae",
				},
				{
					"id":           220,
					"int_value":    3000,
					"string_value": "aae",
					"search_only":  "aae",
				},
				{
					"id":           230,
					"int_value":    2000,
					"string_value": "cbf",
					"search_only":  "cbf",
				},
				{
					"id":           250,
					"int_value":    2000,
					"string_value": "cef",
					"search_only":  "cef",
				},
				{
					"id":           210,
					"int_value":    1000,
					"string_value": "zab",
					"search_only":  "zab",
				},
				{
					"id":           260,
					"int_value":    5000,
					"string_value": "aab",
					"search_only":  "zab",
				},
			},
		},
	}
	for _, c := range cases {
		readAndValidateOrder(t,
			db,
			coll,
			c.filters,
			nil,
			c.sortOrder,
			c.expDocuments)
	}
}

func TestRead_Unicode(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	dropCollection(t, db, coll)
	createCollection(t, db, coll,
		Map{
			"schema": Map{
				"title": coll,
				"properties": Map{
					"id":           Map{"type": "integer"},
					"index_value": Map{"type": "string", "index": true},
					"search_value":  Map{"type": "string", "searchIndex": true, "sort": true},
					"local_value":  Map{"type": "string"},
				},
			},
		}).Status(http.StatusOK)

	inputDocument := []Doc{
		{
			"id":           1,
			"index_value":    "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
			"search_value": "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
			"local_value":  "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
		},
		{
			"id":           2,
			"index_value":   "has braces ( and > ( < and escaped operators 안녕 or some emoji",
			"search_value": "has braces ( and > ( < and escaped operators 안녕 or some emoji",
			"local_value":  "has braces ( and > ( < and escaped operators 안녕 or some emoji",
		}, {
			"id":           3,
			"index_value":   "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
			"search_value": "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
			"local_value":  "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(t, db, coll, inputDocument, false).
		Status(http.StatusOK)

	cases := []struct {
		filters      Map
		sortOrder    []Map
		expDocuments []Doc
	}{
		{
			Map{
				"index_value": "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
			},
			[]Map{
				{
					"index_value": "$desc",
				},
			},
			[]Doc{inputDocument[2], inputDocument[0]},
		},
		{
			Map{
				"local_value": "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
			},
			nil,
			[]Doc{inputDocument[0], inputDocument[2]},
		},
		{
			Map{
				"search_value": "has braces ( and > ( < and escaped operators 안녕 or some emoji 😀",
			},
			[]Map{
				{
					"search_value": "$desc",
				},
			},
			[]Doc{inputDocument[2], inputDocument[0]},
		},
	}
	for _, c := range cases {
		readAndValidateOrder(t,
			db,
			coll,
			c.filters,
			nil,
			c.sortOrder,
			c.expDocuments)
	}
}

func TestImport(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	one := []byte(`{
			"str_field" : "str_value",
			"int_field" : 1,
			"float_field" : 1.1,
			"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
			"time_field" : "2022-11-04T16:17:23.967964263-07:00",
			"bool_field" : true,
			"binary_field": "cGVlay1hLWJvbwo=",
			"objects" : {
				"str_field" : "str_value",
				"int_field" : 1,
				"float_field" : 1.1,
				"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
				"time_field" : "2022-11-04T16:17:23.967964263-07:00",
				"bool_field" : true,
				"binary_field": "cGVlay1hLWJvbwo="
			},
			"arrays" : [ {
			"str_field" : "str_value",
			"int_field" : 1,
			"float_field" : 1.1,
			"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
			"time_field" : "2022-11-04T16:17:23.967964263-07:00",
			"bool_field" : true,
			"binary_field": "cGVlay1hLWJvbwo="
		} ],
			"prim_array" : [ "str" ]
	}`)

	cases := []struct {
		name          string
		coll          string
		docs          []json.RawMessage
		pk            []string
		autogenerated []string
		create        bool
		exp           string
		err           int
	}{
		{name: "auto create", coll: "import_test", pk: []string{"uuid_field"}, autogenerated: []string{"uuid_field"}, docs: []json.RawMessage{one}, create: true, err: http.StatusOK,
			exp: `{"collection": "import_test",
		"indexes": [
			{"name": "_tigris_created_at", "state": "INDEX ACTIVE"},
			{"name": "_tigris_updated_at", "state": "INDEX ACTIVE"}
		],
		"metadata": {},
		"size": 0,
		"schema": {
		    "title": "import_test",
		    "properties": {
		      "arrays": {
		        "type": "array",
		        "items": {
		          "type": "object",
		          "properties": {
		            "binary_field": { "type": "string", "format": "byte" },
		            "bool_field": { "type": "boolean" },
		            "float_field": { "type": "number" },
		            "int_field": { "type": "integer" },
		            "str_field": { "type": "string" },
		            "time_field": { "type": "string", "format": "date-time" },
		            "uuid_field": { "type": "string", "format": "uuid" }
		          }
		        }
		      },
		      "binary_field": { "type": "string", "format": "byte" },
		      "bool_field": { "type": "boolean" },
		      "float_field": { "type": "number" },
		      "int_field": { "type": "integer" },
		      "objects": {
		        "type": "object",
		        "properties": {
		          "binary_field": { "type": "string", "format": "byte" },
		          "bool_field": { "type": "boolean" },
		          "float_field": { "type": "number" },
		          "int_field": { "type": "integer" },
		          "str_field": { "type": "string" },
		          "time_field": { "type": "string", "format": "date-time" },
		          "uuid_field": { "type": "string", "format": "uuid" }
		        }
		      },
		      "prim_array": {
		        "type": "array",
		        "items": {
		          "type": "string"
		        }
		      },
		      "str_field": { "type": "string" },
		      "time_field": { "type": "string", "format": "date-time" },
		      "uuid_field": { "type": "string", "format": "uuid", "autoGenerate": true }
		    },
		    "primary_key": [ "uuid_field" ]
			}}`},

		{name: "test no create", coll: "import_test_no_create", pk: []string{}, autogenerated: []string{}, docs: []json.RawMessage{one}, create: false,
			err: http.StatusNotFound,
		},

		{name: "evolve schema", coll: "import_test_evolve", pk: []string{"id"}, autogenerated: []string{},
			docs: []json.RawMessage{
				json.RawMessage(`{ "id" : 1, "str_field" : "str_value" }`),
				json.RawMessage(`{ "id" : 2, "int_field": 1 }`),
			},
			create: true,
			err:    http.StatusOK,
			exp: `{
					"collection": "import_test_evolve",
					"indexes": [
						{"name": "_tigris_created_at", "state": "INDEX ACTIVE"},
						{"name": "_tigris_updated_at", "state": "INDEX ACTIVE"}
					],
					"metadata": {},
					"size": 0,
					"schema": {
						"title": "import_test_evolve",
						"properties": {
							"id": { "type": "integer" },
							"int_field": { "type": "integer" },
							"str_field": { "type": "string" }
						},
						"primary_key": [ "id" ]
					}
				}`,
		},
		{name: "multi pk", coll: "import_test_multi_pk", pk: []string{"id", "id2"}, autogenerated: []string{},
			docs: []json.RawMessage{
				json.RawMessage(`{ "id" : 1, "id2": "str", "str_field" : "str_value" }`),
				json.RawMessage(`{ "id" : 2, "id2": "str", "int_field": 1 }`),
			},
			create: true,
			err:    http.StatusOK,
			exp: `{
					"collection": "import_test_multi_pk",
					"indexes": [
						{"name": "_tigris_created_at", "state": "INDEX ACTIVE"},
						{"name": "_tigris_updated_at", "state": "INDEX ACTIVE"}
					],
					"metadata": {},
					"size": 0,
					"schema": {
						"title": "import_test_multi_pk",
						"properties": {
							"id": { "type": "integer" },
							"id2": { "type": "string" },
							"int_field": { "type": "integer" },
							"str_field": { "type": "string" }
						},
						"primary_key": [ "id", "id2" ]
					}
				}`,
		},
		{name: "duplicate key", coll: "import_test_dup_key", pk: []string{"id"}, autogenerated: []string{},
			docs: []json.RawMessage{
				json.RawMessage(`{ "id" : 1, "str_field" : "str_value" }`),
				json.RawMessage(`{ "id" : 1, "int_field": 1 }`),
			},
			create: true,
			err:    http.StatusConflict,
		},
		{name: "evolve schema in separate batch", coll: "import_test_evolve_2_batch", pk: []string{"id"}, autogenerated: []string{},
			docs: []json.RawMessage{
				json.RawMessage(`{ "id" : 1, "str_field" : "str_value" }`),
				json.RawMessage(`{ "id" : 2, "int_field": 1 }`),
				json.RawMessage(`{ "id" : 3, "int_field_too": 2 }`),
			},
			create: true,
			err:    http.StatusOK,
			exp: `{
			"collection": "import_test_evolve_2_batch",
			"indexes": [
				{"name": "_tigris_created_at", "state": "INDEX ACTIVE"},
				{"name": "_tigris_updated_at", "state": "INDEX ACTIVE"}
			],
			"metadata": {},
			"size": 0,
			"schema": {
				"title": "import_test_evolve_2_batch",
				"properties": {
					"id": { "type": "integer" },
					"int_field": { "type": "integer" },
					"str_field": { "type": "string" },
					"int_field_too": { "type": "integer" }
				},
				"primary_key": [ "id" ]
			}
		}`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dropCollection(t, db, c.coll)

			var resp *httpexpect.Response

			if len(c.docs) > 2 {
				resp = expect(t).POST(getDocumentURL(db, c.coll, "import")).
					WithJSON(Map{
						"create_collection": c.create,
						"autogenerated":     c.autogenerated,
						"primary_key":       c.pk,
						"documents":         c.docs[:2],
					}).
					Expect()

				// Test multi batch import.
				// Run the rest of the documents in a separate batch.
				resp.Status(http.StatusOK)

				resp = expect(t).POST(getDocumentURL(db, c.coll, "import")).
					WithJSON(Map{
						"create_collection": c.create,
						"autogenerated":     c.autogenerated,
						"primary_key":       c.pk,
						"documents":         c.docs[2:],
					}).
					Expect()
			} else {
				resp = expect(t).POST(getDocumentURL(db, c.coll, "import")).
					WithJSON(Map{
						"create_collection": c.create,
						"autogenerated":     c.autogenerated,
						"primary_key":       c.pk,
						"documents":         c.docs,
					}).
					Expect()
			}

			if c.err == http.StatusOK {
				resp.Status(c.err).JSON().Object().
					ValueEqual("status", "inserted")

				resp = describeCollection(t, db, c.coll, Map{})

				sch := resp.Status(http.StatusOK).
					JSON().
					Object().
					ValueEqual("collection", c.coll).Raw()
				// set size of the collection in description 0 for test
				sch["size"] = 0.0
				b, err := jsoniter.Marshal(sch)
				require.NoError(t, err)
				require.JSONEq(t, c.exp, string(b))
			} else {
				resp.Status(c.err)
			}
		})
	}
}

func TestComplexObjectsCollectionSearch(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	collectionName := "c1"
	schema := []byte(`{
  "schema": {
    "title": "c1",
    "properties": {
      "a": {
        "searchIndex": true,
        "type": "integer"
      },
      "b": {
        "searchIndex": true,
        "type": "string"
      },
      "c": {
        "type": "object",
        "properties": {
          "a": {
            "searchIndex": true,
            "type": "integer"
          },
          "b": {
            "searchIndex": true,
            "type": "string"
          },
          "c": {
            "type": "object",
            "properties": {
              "a": {
                "searchIndex": true,
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
                    },
                    "b": {
                      "type": "string"
                    }
                  }
                }
              },
              "d": {
                "searchIndex": true,
                "type": "array",
                "items": {
                  "type": "string"
                }
              }
            }
          },
          "d": {
            "searchIndex": true,
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
                },
                "b": {
                  "type": "string"
                }
              }
            }
          },
          "f": {
            "searchIndex": true,
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "g": {
            "searchIndex": true,
            "type": "array",
            "items": {
              "type": "integer"
            }
          },
          "h": {
            "type": "array",
            "items": {
              "type": "object"
            }
          }
        }
      },
      "d": {
        "searchIndex": true,
        "type": "object",
        "properties": {}
      },
      "e": {
        "searchIndex": true,
        "type": "array",
        "items": {
          "type": "object",
          "properties": {}
        }
      },
      "f": {
        "searchIndex": true,
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "a": {
              "type": "string"
            },
            "b": {
              "type": "string"
            }
          }
        }
      },
      "g": {
        "searchIndex": true,
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "h": {
        "searchIndex": true,
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "i": {
        "searchIndex": true,
        "type": "array",
        "items": {
          "type": "integer"
        }
      },
      "j": {
        "searchIndex": true,
        "type": "array",
        "items": {
          "type": "object"
        }
      }
    }
  }
}`)
	var schemaObj map[string]any
	require.NoError(t, jsoniter.Unmarshal(schema, &schemaObj))

	createCollection(t, project, collectionName, schemaObj).Status(http.StatusOK)

	docA := Doc{
		"a": 1,
		"b": "first document",
		"c": Map{
			"a": 10,
			"b": "nested object under c",
			"c": Map{
				"a": "foo",
				"b": Map{"name": "this is free flow object but not indexed"},
				"c": []Map{{"a": "car"}, {"a": "bike", "b": nil}},
				"d": []string{"PARIS", "LONDON", "ENGLAND"},
			},
			"d": []string{"SANTA CLARA", "SAN JOSE"},
			"e": []Map{{"a": "football", "b": nil}, {"a": "basketball", "b": nil}},
			"f": nil,
			"g": nil,
			"h": nil,
		},
		"d": Map{"agent": "free flow object top level"},
		"e": []Map{{"random": "array of free flow object", "some_null": nil}, {"random": "array of free flow object", "some_null": nil}},
		"f": []Map{{"a": "array of object with a field", "b": nil}, {"a": "array of object with a field second", "b": nil}},
		"g": []string{"NEW YORK", "MIAMI"},
		"h": nil,
		"i": nil,
		"j": nil,
	}

	insertDocuments(t, project, collectionName, []Doc{docA}, false).
		Status(http.StatusOK)

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
			expect(t).POST(fmt.Sprintf("/v1/projects/%s/database/collections/%s/documents/search", project, collectionName)).
				WithJSON(c.expError).
				Expect().
				Status(http.StatusBadRequest)

			continue
		}
		res := getSearchResults(t, project, collectionName, c.query, true)
		require.Equal(t, 1, len(res.Result.Hits))
	}
}

func TestDocumentsChunking(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	collectionName := "fake_collection"
	var schemaObj map[string]any
	require.NoError(t, jsoniter.Unmarshal(FakeCollectionSchema, &schemaObj))

	t.Run("insert_read", func(t *testing.T) {
		createCollection(t, project, collectionName, schemaObj).Status(http.StatusOK)
		defer dropCollection(t, project, collectionName)

		_, documents := GenerateFakesForDoc(t, []string{"1", "2", "3"})
		insertDocuments(t, project, collectionName, documents, true).
			Status(http.StatusOK)

		readAndValidateOrder(t,
			project,
			collectionName,
			nil,
			nil,
			nil,
			documents)

		readAndValidateOrder(t,
			project,
			collectionName,
			nil,
			nil,
			[]Map{{"id": "$desc"}},
			[]Doc{documents[2], documents[1], documents[0]})
	})
	t.Run("replace_read", func(t *testing.T) {
		createCollection(t, project, collectionName, schemaObj).Status(http.StatusOK)
		defer dropCollection(t, project, collectionName)

		_, documents := GenerateFakesForDocWithPlaceholder(t, []string{"1", "2", "3"}, []string{"first", "second", "third"})
		insertDocuments(t, project, collectionName, documents, false).
			Status(http.StatusOK)

		readAndValidateOrder(t,
			project,
			collectionName,
			nil,
			nil,
			nil,
			documents)

		readAndValidateOrder(t,
			project,
			collectionName,
			nil,
			nil,
			[]Map{{"id": "$desc"}},
			[]Doc{documents[2], documents[1], documents[0]})

		readAndValidateOrder(t,
			project,
			collectionName,
			Map{
				"$or": []Doc{
					{"placeholder": "first"},
					{"placeholder": "second"},
					{"placeholder": "third"},
				},
			},
			nil,
			[]Map{{"placeholder": "$desc"}},
			[]Doc{documents[2], documents[1], documents[0]})
	})
	t.Run("update_read", func(t *testing.T) {
		createCollection(t, project, collectionName, schemaObj).Status(http.StatusOK)
		defer dropCollection(t, project, collectionName)

		fakes, documents := GenerateFakesForDoc(t, []string{"1", "2", "3"})
		insertDocuments(t, project, collectionName, documents, false).
			Status(http.StatusOK)

		fakes[0].Name = "updated_name"
		fakes[0].Nested.Address.City = "updated_city"
		fakes[0].Cars = []string{"updated_cars"}
		updateByFilter(t,
			project,
			collectionName,
			Map{
				"filter": Map{
					"id": "1",
				},
			},
			Map{
				"fields": Map{
					"$set": Map{
						"name": fakes[0].Name,
						"nested.address.city": fakes[0].Nested.Address.City,
						"cars": fakes[0].Cars,
					},
				},
			},
			nil).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("modified_count", 1)

		documents[0] = GenerateDocFromFake(t, fakes[0])

		readAndValidateOrder(t,
			project,
			collectionName,
			nil,
			nil,
			nil,
			documents)

		readAndValidateOrder(t,
			project,
			collectionName,
			nil,
			nil,
			[]Map{{"id": "$desc"}},
			[]Doc{documents[2], documents[1], documents[0]})

	})
	t.Run("delete_read", func(t *testing.T) {
		createCollection(t, project, collectionName, schemaObj).Status(http.StatusOK)
		defer dropCollection(t, project, collectionName)

		_, documents := GenerateFakesForDocWithPlaceholder(t, []string{"1", "2", "3"}, []string{"first", "second", "third"})
		insertDocuments(t, project, collectionName, documents, false).
			Status(http.StatusOK)

		deleteByFilter(t,
			project,
			collectionName,
			Map{
			"filter":
				Map{"placeholder": "first"},
		}).
			Status(http.StatusOK)

		readAndValidateOrder(t,
			project,
			collectionName,
			Map{
				"$or": []Doc{
					{"placeholder": "first"},
					{"placeholder": "second"},
					{"placeholder": "third"},
				},
			},
			nil,
			[]Map{{"placeholder": "$desc"}},
			[]Doc{documents[2], documents[1]})
	})
}

func insertDocuments(t *testing.T, db string, collection string, documents []Doc, mustNotExist bool) *httpexpect.Response {
	e := expect(t)

	if mustNotExist {
		return e.POST(getDocumentURL(db, collection, "insert")).
			WithJSON(Map{
				"documents": documents,
			}).Expect()
	} else {
		return e.PUT(getDocumentURL(db, collection, "replace")).
			WithJSON(Map{
				"documents": documents,
			}).Expect()
	}
}

func buildCollectionIndexes(t *testing.T, db string, collection string) *httpexpect.Response {
	return expect(t).POST(getDocumentURL(db, collection, "index")).Expect().Status(http.StatusOK)
}

func updateByFilter(t *testing.T, db string, collection string, filter Map, fields Map, collation Map) *httpexpect.Response {
	payload := make(Map)
	for key, value := range filter {
		payload[key] = value
	}
	for key, value := range fields {
		payload[key] = value
	}
	if collation != nil {
		payload["options"] = collation
	}

	e := expect(t)
	return e.PUT(getDocumentURL(db, collection, "update")).
		WithJSON(payload).
		Expect()
}

func deleteByFilter(t *testing.T, db string, collection string, filter Map) *httpexpect.Response {
	e := expect(t)
	return e.DELETE(getDocumentURL(db, collection, "delete")).
		WithJSON(filter).
		Expect()
}

func readExpError(t *testing.T, db string, collection string, filter Map, expectedErrorCode int) {
	payload := make(Map)
	if filter == nil {
		payload["filter"] = jsoniter.RawMessage(`{}`)
	} else {
		payload["filter"] = filter
	}

	e := expect(t)
	e.POST(getDocumentURL(db, collection, "read")).
		WithJSON(payload).
		Expect().
		Status(expectedErrorCode).
		Body().
		Raw()
}

func readByFilter(t *testing.T, db string, collection string, filter Map, fields Map, options Map, order []Map) []map[string]jsoniter.RawMessage {
	payload := make(Map)
	payload["fields"] = fields
	if filter == nil {
		payload["filter"] = json.RawMessage(`{}`)
	} else {
		payload["filter"] = filter
	}
	if len(order) > 0 {
		payload["sort"] = order
	}
	if options != nil {
		payload["options"] = options
	}

	e := expect(t)
	str := e.POST(getDocumentURL(db, collection, "read")).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).
		Body().
		Raw()

	var resp []map[string]jsoniter.RawMessage
	dec := jsoniter.NewDecoder(bytes.NewReader([]byte(str)))
	for dec.More() {
		var mp map[string]jsoniter.RawMessage
		require.NoError(t, dec.Decode(&mp))
		resp = append(resp, mp)
	}

	return resp
}

func explainQuery(t *testing.T, db string, collection string, filter Map, fields Map, options Map, order []Map) *api.ExplainResponse {
	payload := make(Map)
	payload["fields"] = fields
	if filter == nil {
		payload["filter"] = json.RawMessage(`{}`)
	} else {
		payload["filter"] = filter
	}
	if len(order) > 0 {
		payload["sort"] = order
	}
	if options != nil {
		payload["options"] = options
	}

	e := expect(t)
	str := e.POST(getDocumentURL(db, collection, "explain")).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).
		Body().
		Raw()

	explain := &api.ExplainResponse{}
	err := jsoniter.Unmarshal([]byte(str), explain)
	require.NoError(t, err)
	return explain
}

func readAndValidatePkeyOrder(t *testing.T, db string, collection string, filter Map, fields Map, inputDocument []Doc, collation Map) {
	readResp := readByFilter(t, db, collection, filter, fields, collation, nil)
	require.Equal(t, len(inputDocument), len(readResp))

	var primaryKeys []int
	inputKeyToValue := make(map[int]Doc)
	for i := 0; i < len(inputDocument); i++ {
		pk := inputDocument[i]["pkey_int"].(int)
		inputKeyToValue[pk] = inputDocument[i]
		primaryKeys = append(primaryKeys, pk)
	}

	outputKeyToValue := make(map[int]Doc)
	for i := 0; i < len(inputDocument); i++ {
		var data Doc
		require.NoError(t, jsoniter.Unmarshal(readResp[i]["result"], &data))
		doc := data["data"].(map[string]any)
		outputKeyToValue[int(doc["pkey_int"].(float64))] = doc
	}
	sort.Ints(primaryKeys)

	for _, p := range primaryKeys {
		expDoc, err := jsoniter.Marshal(inputKeyToValue[p])
		require.NoError(t, err)

		actualDoc, err := jsoniter.Marshal(outputKeyToValue[p])
		require.NoError(t, err)
		require.JSONEqf(t, string(expDoc), string(actualDoc), "exp '%s' actual '%s'", string(expDoc), string(actualDoc))
	}
}

func readAndValidateAcceptApplicationJSON(t *testing.T, db string, collection string, filter Map, fields Map, options Map, order []Map, inputDocument []Doc) {
	payload := make(Map)
	payload["fields"] = fields
	if filter == nil {
		payload["filter"] = json.RawMessage(`{}`)
	} else {
		payload["filter"] = filter
	}
	if len(order) > 0 {
		payload["sort"] = order
	}
	if options != nil {
		payload["options"] = options
	}

	e := expect(t)
	str := e.POST(getDocumentURL(db, collection, "read")).
		WithJSON(payload).
		WithHeader("accept", "application/json").
		Expect().
		Status(http.StatusOK).
		Body().
		Raw()

	type result struct {
		Result struct {
			Data []jsoniter.RawMessage `json:"data"`
		} `json:"result"`
	}

	var res result
	require.NoError(t, jsoniter.Unmarshal([]byte(str), &res))
	require.Equal(t, len(inputDocument), len(res.Result.Data))
	for i, element := range res.Result.Data {
		var data map[string]jsoniter.RawMessage
		require.NoError(t, jsoniter.Unmarshal(element, &data))
		delete(data, schema.ReservedFields[schema.CreatedAt])
		delete(data, schema.ReservedFields[schema.UpdatedAt])

		out, err := jsoniter.Marshal(data)
		require.NoError(t, err)

		expDoc, err := jsoniter.Marshal(inputDocument[i])
		require.NoError(t, err)
		require.JSONEq(t, string(expDoc), string(out))
	}
}

func readAndValidateWithOptions(t *testing.T, db string, collection string, filter Map, fields Map, options Map, inputDocument []Doc) {
	readResp := readByFilter(t, db, collection, filter, fields, options, nil)
	require.Equal(t, len(inputDocument), len(readResp))

	for i := 0; i < len(inputDocument); i++ {
		validateInputDocToRes(t, readResp[i], inputDocument[i], filter)
	}
}

func readAndValidate(t *testing.T, db string, collection string, filter Map, fields Map, inputDocument []Doc) {
	readResp := readByFilter(t, db, collection, filter, fields, nil, nil)
	require.Equal(t, len(inputDocument), len(readResp), filter)

	for i := 0; i < len(inputDocument); i++ {
		validateInputDocToRes(t, readResp[i], inputDocument[i], filter)
	}
}

func readAndValidateOrder(t *testing.T, db string, collection string, filter Map, fields Map, order []Map, inputDocument []Doc) {
	readResp := readByFilter(t, db, collection, filter, fields, nil, order)
	require.Equal(t, len(inputDocument), len(readResp))

	for i := 0; i < len(inputDocument); i++ {
		validateInputDocToRes(t, readResp[i], inputDocument[i], filter)
	}
}

func validateInputDocToRes(t *testing.T, readResp map[string]jsoniter.RawMessage, input Doc, filter Map) {
	var doc map[string]jsoniter.RawMessage
	require.NoError(t, jsoniter.Unmarshal(readResp["result"], &doc))

	actualDoc := []byte(doc["data"])
	expDoc, err := jsoniter.Marshal(input)
	require.NoError(t, err)
	require.JSONEqf(t, string(expDoc), string(actualDoc), "exp '%s' actual '%s'", string(expDoc), string(actualDoc), fmt.Sprintf("%v", filter))
}
