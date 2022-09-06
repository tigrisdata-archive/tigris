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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
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
			"random_database1",
			coll,
			[]Doc{{"pkey_int": 1}},
			"database doesn't exist 'random_database1'",
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
		resp := expect(t).POST(getDocumentURL(c.databaseName, c.collectionName, "insert")).
			WithJSON(Map{
				"documents": c.documents,
			}).
			Expect()

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

	e := expect(t)
	e.POST(getDocumentURL(db, coll, "insert")).
		WithJSON(Map{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	resp := e.POST(getDocumentURL(db, coll, "insert")).
		WithJSON(Map{"documents": inputDocument}).Expect()
	testError(resp, http.StatusConflict, api.Code_ALREADY_EXISTS, "duplicate key value, violates key constraint")
}

func TestInsert_SchemaValidationError(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	cases := []struct {
		documents  []Doc
		expMessage string
	}{
		{
			[]Doc{
				{
					"pkey_int":  1,
					"int_value": 10.20,
				},
			},
			"json schema validation failed for field 'int_value' reason 'expected integer, but got number'",
		}, {
			[]Doc{
				{
					"pkey_int":     1,
					"string_value": 12,
				},
			},
			"json schema validation failed for field 'string_value' reason 'expected string, but got number'",
		}, {
			[]Doc{{"bytes_value": 12.30}},
			"json schema validation failed for field 'bytes_value' reason 'expected string, but got number'",
		}, {
			[]Doc{{"bytes_value": "not enough"}},
			"json schema validation failed for field 'bytes_value' reason ''not enough' is not valid 'byte''",
		}, {
			[]Doc{{"date_time_value": "Mon, 02 Jan 2006"}},
			"json schema validation failed for field 'date_time_value' reason ''Mon, 02 Jan 2006' is not valid 'date-time''",
		}, {
			[]Doc{{"uuid_value": "abc-bcd"}},
			"json schema validation failed for field 'uuid_value' reason ''abc-bcd' is not valid 'uuid''",
		}, {
			[]Doc{
				{
					"pkey_int":  10,
					"extra_key": "abc-bcd",
				},
			},
			"json schema validation failed for field '' reason 'additionalProperties 'extra_key' not allowed'",
		},
	}
	for _, c := range cases {
		resp := expect(t).POST(getDocumentURL(db, coll, "insert")).
			WithJSON(Map{"documents": c.documents}).Expect()

		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, c.expMessage)
	}
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
					"primary_key": []interface{}{"bytes_value"},
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
					"primary_key": []interface{}{"uuid_value"},
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
					"primary_key": []interface{}{"date_time_value"},
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
					"primary_key": []interface{}{"string_value"},
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
		var value interface{}
		for k, v := range c.primaryKeyLookup {
			key = k
			value = v
		}
		e := expect(t)
		e.POST(getDocumentURL(db, collectionName, "insert")).
			WithJSON(Map{
				"documents": c.inputDoc,
			}).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "inserted").
			ValueEqual("keys", []map[string]interface{}{{key: value}})

		readResp := readByFilter(t, db, collectionName, c.primaryKeyLookup, nil)

		var doc map[string]json.RawMessage
		require.Greater(t, len(readResp), 0)
		require.NoError(t, json.Unmarshal(readResp[0]["result"], &doc))

		var actualDoc = []byte(doc["data"])
		expDoc, err := json.Marshal(c.inputDoc[0])
		require.NoError(t, err)
		require.Equal(t, expDoc, actualDoc)
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
	expect(t).POST(getDocumentURL(db, coll, "insert")).
		WithJSON(Map{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted").
		ValueEqual("keys", []map[string]interface{}{{"pkey_int": 10}}).
		Path("$.metadata").Object().
		Value("created_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readResp := readByFilter(t, db, coll, Map{
		"pkey_int": 10,
	}, nil)

	var doc map[string]json.RawMessage
	require.Equal(t, 1, len(readResp))
	require.NoError(t, json.Unmarshal(readResp[0]["result"], &doc))

	var actualDoc = []byte(doc["data"])
	expDoc, err := json.Marshal(inputDocument[0])
	require.NoError(t, err)
	require.Equal(t, expDoc, actualDoc)
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

	e := expect(t)
	e.POST(getDocumentURL(db, coll, "insert")).
		WithJSON(Map{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted").
		ValueEqual("keys", []map[string]interface{}{{"pkey_int": 20}, {"pkey_int": 30}})

	readResp := readByFilter(t, db, coll, Map{
		"$or": []Doc{
			{"pkey_int": 20},
			{"pkey_int": 30},
		},
	}, nil)

	require.Equal(t, 2, len(readResp))
	for i := 0; i < len(inputDocument); i++ {
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(readResp[i]["result"], &doc))

		var actualDoc = []byte(doc["data"])
		expDoc, err := json.Marshal(inputDocument[i])
		require.NoError(t, err)
		require.Equal(t, expDoc, actualDoc)
	}
}

func TestInsert_AutoGenerated(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	testAutoGenerated(t, db, coll, Map{"type": "string", "autoGenerate": true})
	testAutoGenerated(t, db, coll, Map{
		"type":         "string",
		"autoGenerate": true,
		"format":       "byte"})
	testAutoGenerated(t, db, coll, Map{
		"type":         "string",
		"format":       "uuid",
		"autoGenerate": true})
	testAutoGenerated(t, db, coll, Map{
		"type":         "string",
		"format":       "date-time",
		"autoGenerate": true})
	testAutoGenerated(t, db, coll, Map{
		"type":         "integer",
		"format":       "int32",
		"autoGenerate": true,
	})
	testAutoGenerated(t, db, coll, Map{"type": "integer", "autoGenerate": true})
}

func TestInsert_SchemaUpdate(t *testing.T) {
	dbName := fmt.Sprintf("db_test")

	dropDatabase(t, dbName)
	createDatabase(t, dbName)
	defer dropDatabase(t, dbName)

	collectionName := fmt.Sprintf("test_collection")
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

	var inputDoc = []Doc{{"int_value": 1, "string_value": "foo"}}
	e := expect(t)
	e.POST(getDocumentURL(dbName, collectionName, "insert")).
		WithJSON(Map{
			"documents": inputDoc,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	inputDoc = []Doc{{"int_value": 1, "string_value": "foo", "extra_field": "bar"}}
	e.POST(getDocumentURL(dbName, collectionName, "insert")).
		WithJSON(Map{
			"documents": inputDoc,
		}).
		Expect().
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
	e.POST(getDocumentURL(dbName, collectionName, "insert")).
		WithJSON(Map{
			"documents": inputDoc,
		}).
		Expect().
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
	var inputDoc = []Doc{{"int_value": 1}, {"int_value": 2}}
	e := expect(t)
	e.POST(getDocumentURL(dbName, collectionName, "insert")).
		WithJSON(Map{
			"documents": inputDoc,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	// insert 3
	thirdDoc := []Doc{{"int_value": 3}}
	e = expect(t)
	e.POST(getDocumentURL(dbName, collectionName, "insert")).
		WithJSON(Map{
			"documents": thirdDoc,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	// test zero values for every type
	var pk interface{}
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
	e = expect(t)
	e.POST(getDocumentURL(dbName, collectionName, "insert")).
		WithJSON(Map{
			"documents": fourthDoc,
		}).Expect().Status(http.StatusOK).
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
	e = expect(t)
	i := e.POST(getDocumentURL(dbName, collectionName, "insert")).
		WithJSON(Map{
			"documents": fifthDoc,
		}).Expect().Status(http.StatusOK).JSON().Object().Raw()
	require.Equal(t, "inserted", i["status"])
	k := i["keys"].([]interface{})
	require.Less(t, 0, len(k))
	assert.EqualValues(t, pk, k[0].(map[string]interface{})["pkey"])

	readResp := readByFilter(t, dbName, collectionName, nil, nil)
	var decodedResult = make([]map[string]interface{}, 5)
	for _, response := range readResp {
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(response["result"], &doc))
		var actualDoc map[string]interface{}
		require.NoError(t, json.Unmarshal(doc["data"], &actualDoc))

		val := int64(actualDoc["int_value"].(float64))
		if val > 5 || val < 1 {
			require.Fail(t, fmt.Sprintf("not expected value %d %T", val, actualDoc["int_value"]))
		}
		decodedResult[val-1] = actualDoc
	}

	validate := func(response map[string]interface{}, inputDoc Doc, iteration int) {
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
			"random_database1",
			coll,
			Map{
				"$set": Map{
					"string_value": "simple_update",
				},
			},
			Map{
				"pkey_int": 1,
			},
			"database doesn't exist 'random_database1'",
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
		}).Status(http.StatusOK).
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
		}).Status(http.StatusOK).
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
			"json schema validation failed for field 'string_value' reason 'expected string, but got number'",
			api.Code_INVALID_ARGUMENT,
		}, {
			Map{
				"$set": Map{
					"int_value": "1",
				},
			},
			"json schema validation failed for field 'int_value' reason 'expected integer, but got string'",
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
		}).Status(http.StatusOK).
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
		}).Status(http.StatusOK).
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
		readAndValidate(t,
			db,
			coll,
			readFilter,
			nil,
			inputDocument)

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
			}).Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "updated").
			ValueEqual("modified_count", c.modified)

		// read all documents back
		testUpdateOnAnyField(t, db, coll, readFilter, inputDocument, c.changed)
	}
}

func testUpdateOnAnyField(t *testing.T, db string, collection string, filter Map, input []Doc, changed []int) {
	out := readByFilter(t, db, collection, filter, nil)

	var notChanged []int
	for i := range input {
		found := false
		for _, c := range changed {
			if i == c {
				found = true
			}
		}
		if found {
			continue
		}
		notChanged = append(notChanged, i)
	}

	for _, i := range changed {
		var data map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(out[i]["result"], &data))

		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data["data"], &doc))

		require.Equal(t, "\"after\"", string(doc["added_string_value"]))
	}

	for _, i := range notChanged {
		validateInputDocToRes(t, out[i], input[i])
	}
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
			"random_database1",
			coll,
			Map{"pkey_int": 1},
			"database doesn't exist 'random_database1'",
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
			"random_database1",
			coll,
			Map{"pkey_int": 1},
			"database doesn't exist 'random_database1'",
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
			"pkey_int":     60,
			"int_value":    60,
			"string_value": "simple_insert60",
			"bytes_value":  []byte(`"simple_insert60"`),
		},
		{
			"pkey_int":     70,
			"int_value":    70,
			"string_value": "simple_insert70",
			"double_value": 1.234,
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

	readAndValidate(t,
		db,
		coll,
		readFilter,
		nil,
		inputDocument)

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
			c.expDocuments)
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
			"primary_key": []interface{}{"Id"},
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

	var inputDoc = []Doc{{"id": 1, "object_field": Map{"nested_id": 1, "nested_str": "foo"}}, {"id": 2, "object_field": Map{"nested_id": 2, "nested_str": "bar"}}}
	e := expect(t)
	e.POST(getDocumentURL(db, coll, "insert")).
		WithJSON(Map{
			"documents": inputDoc,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	readResp := readByFilter(t, db, coll, Map{
		"object_field.nested_str": "bar",
	}, nil)

	var doc map[string]json.RawMessage
	require.Equal(t, 1, len(readResp))
	require.NoError(t, json.Unmarshal(readResp[0]["result"], &doc))

	var actualDoc = []byte(doc["data"])
	expDoc, err := json.Marshal(inputDoc[1])
	require.NoError(t, err)
	require.JSONEq(t, string(expDoc), string(actualDoc))
}

func TestTransaction_BadID(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	e := expect(t)
	r := e.POST(fmt.Sprintf("/api/v1/databases/%s/transactions/begin", db)).
		Expect().Status(http.StatusOK).
		Body().Raw()

	res := struct {
		TxCtx api.TransactionCtx `json:"tx_ctx"`
	}{}

	err := json.Unmarshal([]byte(r), &res)
	require.NoError(t, err)

	resp := e.POST(getDocumentURL(db, coll, "insert")).
		WithJSON(Map{"documents": []Doc{{}}}).
		WithHeader("Tigris-Tx-Id", "some id").
		WithHeader("Tigris-Tx-Origin", res.TxCtx.Origin).Expect()
	testError(resp, http.StatusInternalServerError, api.Code_INTERNAL, "session is gone")
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

func updateByFilter(t *testing.T, db string, collection string, filter Map, fields Map) *httpexpect.Response {
	var payload = make(Map)
	for key, value := range filter {
		payload[key] = value
	}
	for key, value := range fields {
		payload[key] = value
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

func readByFilter(t *testing.T, db string, collection string, filter Map, fields Map) []map[string]json.RawMessage {
	var payload = make(Map)
	payload["fields"] = fields
	if filter == nil {
		payload["filter"] = json.RawMessage(`{}`)
	} else {
		payload["filter"] = filter
	}

	e := expect(t)
	str := e.POST(getDocumentURL(db, collection, "read")).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).
		Body().
		Raw()

	var resp []map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader([]byte(str)))
	for dec.More() {
		var mp map[string]json.RawMessage
		require.NoError(t, dec.Decode(&mp))
		resp = append(resp, mp)
	}

	return resp
}

func readAndValidatePkeyOrder(t *testing.T, db string, collection string, filter Map, fields Map, inputDocument []Doc) {
	readResp := readByFilter(t, db, collection, filter, fields)
	require.Equal(t, len(inputDocument), len(readResp))

	var primaryKeys []int
	var inputKeyToValue = make(map[int]Doc)
	for i := 0; i < len(inputDocument); i++ {
		pk := inputDocument[i]["pkey_int"].(int)
		inputKeyToValue[pk] = inputDocument[i]
		primaryKeys = append(primaryKeys, pk)
	}

	var outputKeyToValue = make(map[int]Doc)
	for i := 0; i < len(inputDocument); i++ {
		var data Doc
		require.NoError(t, json.Unmarshal(readResp[i]["result"], &data))
		doc := data["data"].(map[string]any)
		outputKeyToValue[int(doc["pkey_int"].(float64))] = doc
	}
	sort.Ints(primaryKeys)

	for _, p := range primaryKeys {
		expDoc, err := json.Marshal(inputKeyToValue[p])
		require.NoError(t, err)

		actualDoc, err := json.Marshal(outputKeyToValue[p])
		require.NoError(t, err)
		require.JSONEqf(t, string(expDoc), string(actualDoc), "exp '%s' actual '%s'", string(expDoc), string(actualDoc))
	}
}

func readAndValidate(t *testing.T, db string, collection string, filter Map, fields Map, inputDocument []Doc) {
	readResp := readByFilter(t, db, collection, filter, fields)
	require.Equal(t, len(inputDocument), len(readResp))

	for i := 0; i < len(inputDocument); i++ {
		validateInputDocToRes(t, readResp[i], inputDocument[i])
	}
}

func validateInputDocToRes(t *testing.T, readResp map[string]json.RawMessage, input Doc) {
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(readResp["result"], &doc))

	var actualDoc = []byte(doc["data"])
	expDoc, err := json.Marshal(input)
	require.NoError(t, err)
	require.JSONEqf(t, string(expDoc), string(actualDoc), "exp '%s' actual '%s'", string(expDoc), string(actualDoc))
}
