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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

type Map map[string]interface{}
type Doc Map

const (
	defaultDatabaseTestName   = "integration_db2"
	defaultCollectionTestName = "test_collection"
)

type DocumentSuite struct {
	suite.Suite

	collection string
	database   string
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

func getDocumentURL(databaseName, collectionName string, methodName string) string {
	return fmt.Sprintf("/api/v1/databases/%s/collections/%s/documents/%s", databaseName, collectionName, methodName)
}

func SetupDatabaseSuite(t *testing.T, dbName string) {
	dropDatabase(t, dbName)
	createDatabase(t, dbName).Status(http.StatusOK)
}

func SetupSuite(t *testing.T, dbName string, collectionName string) {
	dropDatabase(t, dbName)
	createDatabase(t, dbName).Status(http.StatusOK)
	createCollection(t, dbName, collectionName, testCreateSchema).Status(http.StatusOK)
}

func TearDownSuite(t *testing.T, dbName string) {
	dropDatabase(t, dbName).Status(http.StatusOK)
}

func (s *DocumentSuite) SetupSuite() {
	dropDatabase(s.T(), s.database)
	createDatabase(s.T(), s.database).Status(http.StatusOK)
	createCollection(s.T(), s.database, s.collection, testCreateSchema).Status(http.StatusOK)
}

func (s *DocumentSuite) TearDownSuite() {
	dropDatabase(s.T(), s.database).Status(http.StatusOK)
}

func (s *DocumentSuite) TestInsert_Bad_NotFoundRequest() {
	cases := []struct {
		databaseName   string
		collectionName string
		documents      []Doc
		expMessage     string
		status         int
	}{
		{
			"random_database1",
			s.collection,
			[]Doc{{"pkey_int": 1}},
			"database doesn't exist 'random_database1'",
			http.StatusNotFound,
		}, {
			s.database,
			"random_collection",
			[]Doc{{"pkey_int": 1}},
			"collection doesn't exist 'random_collection'",
			http.StatusNotFound,
		}, {
			"",
			s.collection,
			[]Doc{{"pkey_int": 1}},
			"invalid database name",
			http.StatusBadRequest,
		}, {
			s.database,
			"",
			[]Doc{{"pkey_int": 1}},
			"invalid collection name",
			http.StatusBadRequest,
		}, {
			s.database,
			s.collection,
			[]Doc{},
			"empty documents received",
			http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		resp := expect(s.T()).POST(getDocumentURL(c.databaseName, c.collectionName, "insert")).
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

func (s *DocumentSuite) TestInsert_AlreadyExists() {
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

	e := expect(s.T())
	e.POST(getDocumentURL(s.database, s.collection, "insert")).
		WithJSON(Map{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	resp := e.POST(getDocumentURL(s.database, s.collection, "insert")).
		WithJSON(Map{"documents": inputDocument}).Expect()
	testError(resp, http.StatusConflict, api.Code_ALREADY_EXISTS, "duplicate key value, violates key constraint")
}

func (s *DocumentSuite) TestInsert_SchemaValidationError() {
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
		resp := expect(s.T()).POST(getDocumentURL(s.database, s.collection, "insert")).
			WithJSON(Map{"documents": c.documents}).Expect()

		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, c.expMessage)
	}
}

func (s *DocumentSuite) TestInsert_SupportedPrimaryKeys() {
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
		dropCollection(s.T(), s.database, collectionName)
		createCollection(s.T(), s.database, collectionName, c.schema).Status(http.StatusOK)

		var key string
		var value interface{}
		for k, v := range c.primaryKeyLookup {
			key = k
			value = v
		}
		e := expect(s.T())
		e.POST(getDocumentURL(s.database, collectionName, "insert")).
			WithJSON(Map{
				"documents": c.inputDoc,
			}).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("status", "inserted").
			ValueEqual("keys", []map[string]interface{}{{key: value}})

		readResp := readByFilter(s.T(), s.database, collectionName, c.primaryKeyLookup, nil)

		var doc map[string]json.RawMessage
		require.Greater(s.T(), len(readResp), 0)
		require.NoError(s.T(), json.Unmarshal(readResp[0]["result"], &doc))

		var actualDoc = []byte(doc["data"])
		expDoc, err := json.Marshal(c.inputDoc[0])
		require.NoError(s.T(), err)
		require.Equal(s.T(), expDoc, actualDoc)
	}
}

func (s *DocumentSuite) TestInsert_SingleRow() {
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
	expect(s.T()).POST(getDocumentURL(s.database, s.collection, "insert")).
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

	readResp := readByFilter(s.T(), s.database, s.collection, Map{
		"pkey_int": 10,
	}, nil)

	var doc map[string]json.RawMessage
	require.Equal(s.T(), 1, len(readResp))
	require.NoError(s.T(), json.Unmarshal(readResp[0]["result"], &doc))

	var actualDoc = []byte(doc["data"])
	expDoc, err := json.Marshal(inputDocument[0])
	require.NoError(s.T(), err)
	require.Equal(s.T(), expDoc, actualDoc)
}

func (s *DocumentSuite) TestInsert_MultipleRows() {
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

	e := expect(s.T())
	e.POST(getDocumentURL(s.database, s.collection, "insert")).
		WithJSON(Map{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted").
		ValueEqual("keys", []map[string]interface{}{{"pkey_int": 20}, {"pkey_int": 30}})

	readResp := readByFilter(s.T(), s.database, s.collection, Map{
		"$or": []Doc{
			{"pkey_int": 20},
			{"pkey_int": 30},
		},
	}, nil)

	require.Equal(s.T(), 2, len(readResp))
	for i := 0; i < len(inputDocument); i++ {
		var doc map[string]json.RawMessage
		require.NoError(s.T(), json.Unmarshal(readResp[i]["result"], &doc))

		var actualDoc = []byte(doc["data"])
		expDoc, err := json.Marshal(inputDocument[i])
		require.NoError(s.T(), err)
		require.Equal(s.T(), expDoc, actualDoc)
	}
}

func TestInsert_AutoGenerated(t *testing.T) {
	SetupSuite(t, defaultDatabaseTestName, defaultCollectionTestName)
	defer TearDownSuite(t, defaultDatabaseTestName)

	testAutoGenerated(t, defaultDatabaseTestName, defaultCollectionTestName, Map{"type": "string", "autoGenerate": true})
	testAutoGenerated(t, defaultDatabaseTestName, defaultCollectionTestName, Map{
		"type":         "string",
		"autoGenerate": true,
		"format":       "byte"})
	testAutoGenerated(t, defaultDatabaseTestName, defaultCollectionTestName, Map{
		"type":         "string",
		"format":       "uuid",
		"autoGenerate": true})
	testAutoGenerated(t, defaultDatabaseTestName, defaultCollectionTestName, Map{
		"type":         "string",
		"format":       "date-time",
		"autoGenerate": true})
	testAutoGenerated(t, defaultDatabaseTestName, defaultCollectionTestName, Map{
		"type":         "integer",
		"format":       "int32",
		"autoGenerate": true,
	})
	testAutoGenerated(t, defaultDatabaseTestName, defaultCollectionTestName, Map{"type": "integer", "autoGenerate": true})
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

func (s *DocumentSuite) TestUpdate_BadRequest() {
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
			s.collection,
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
			s.database,
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
			s.collection,
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
			s.database,
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
			s.database,
			s.collection,
			nil,
			Map{
				"pkey_int": 1,
			},
			"empty fields received",
			http.StatusBadRequest,
		}, {
			s.database,
			s.collection,
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
		resp := expect(s.T()).PUT(getDocumentURL(c.database, c.collection, "update")).
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

func (s *DocumentSuite) TestUpdate_SingleRow() {
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

	insertDocuments(s.T(), s.database, s.collection, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(s.T(),
		s.database,
		s.collection,
		Map{
			"pkey_int": 100,
		},
		nil,
		inputDocument)

	tstart := time.Now().UTC()
	updateByFilter(s.T(),
		s.database,
		s.collection,
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

	readAndValidate(s.T(),
		s.database,
		s.collection,
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

func (s *DocumentSuite) TestUpdate_SchemaValidationError() {
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

	insertDocuments(s.T(), s.database, s.collection, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(s.T(),
		s.database,
		s.collection,
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
		resp := expect(s.T()).PUT(getDocumentURL(s.database, s.collection, "update")).
			WithJSON(Map{
				"fields": c.documents,
				"filter": Map{
					"pkey_int": 1,
				},
			}).Expect()
		testError(resp, http.StatusBadRequest, c.expCode, c.expMessage)
	}
}

func (s *DocumentSuite) TestUpdate_MultipleRows() {
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
	insertDocuments(s.T(), s.database, s.collection, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$or": []Doc{
			{"pkey_int": 110},
			{"pkey_int": 120},
			{"pkey_int": 130},
		},
	}
	readAndValidate(s.T(),
		s.database,
		s.collection,
		readFilter,
		nil,
		inputDocument)

	// first try updating a no-op operation i.e. random filter value
	updateByFilter(s.T(),
		s.database,
		s.collection,
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
	readAndValidate(s.T(),
		s.database,
		s.collection,
		readFilter,
		nil,
		inputDocument)

	// Update keys 120 and 130
	updateByFilter(s.T(),
		s.database,
		s.collection,
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
	readAndValidate(s.T(),
		s.database,
		s.collection,
		readFilter,
		nil,
		outDocument)
}

func (s *DocumentSuite) TestDelete_BadRequest() {
	cases := []struct {
		databaseName   string
		collectionName string
		filter         Map
		expMessage     string
		status         int
	}{
		{
			"random_database1",
			s.collection,
			Map{"pkey_int": 1},
			"database doesn't exist 'random_database1'",
			http.StatusNotFound,
		}, {
			s.database,
			"random_collection",
			Map{"pkey_int": 1},
			"collection doesn't exist 'random_collection'",
			http.StatusNotFound,
		}, {
			"",
			s.collection,
			Map{"pkey_int": 1},
			"invalid database name",
			http.StatusBadRequest,
		}, {
			s.database,
			"",
			Map{"pkey_int": 1},
			"invalid collection name",
			http.StatusBadRequest,
		}, {
			s.database,
			s.collection,
			nil,
			"filter is a required field",
			http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		resp := expect(s.T()).DELETE(getDocumentURL(c.databaseName, c.collectionName, "delete")).
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

func (s *DocumentSuite) TestDelete_SingleRow() {
	inputDocument := []Doc{
		{
			"pkey_int":  40,
			"int_value": 10,
		},
	}

	insertDocuments(s.T(), s.database, s.collection, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(s.T(),
		s.database,
		s.collection,
		Map{"pkey_int": 40},
		nil,
		inputDocument)

	tstart := time.Now().UTC()
	deleteByFilter(s.T(), s.database, s.collection, Map{
		"filter": Map{"pkey_int": 40},
	}).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "deleted").
		Path("$.metadata").Object().
		Value("deleted_at").String().DateTime(time.RFC3339Nano).InRange(tstart, time.Now().UTC().Add(1*time.Second))

	readAndValidate(s.T(),
		s.database,
		s.collection,
		Map{"pkey_int": 40},
		nil,
		nil)
}

func (s *DocumentSuite) TestDelete_MultipleRows() {
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
	insertDocuments(s.T(), s.database, s.collection, inputDocument, false).
		Status(http.StatusOK)

	readFilter := Map{
		"$or": []Doc{
			{"pkey_int": 50},
			{"pkey_int": 60},
			{"pkey_int": 70},
		},
	}
	readAndValidate(s.T(),
		s.database,
		s.collection,
		readFilter,
		nil,
		inputDocument)

	// first try deleting a no-op operation i.e. random filter value
	deleteByFilter(s.T(), s.database, s.collection, Map{
		"filter": Map{
			"pkey_int": 10000,
		},
	}).Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "deleted")

	// read all documents back
	readAndValidate(s.T(),
		s.database,
		s.collection,
		readFilter,
		nil,
		inputDocument)

	// DELETE keys 50 and 70
	deleteByFilter(s.T(), s.database, s.collection, Map{
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

	readAndValidate(s.T(),
		s.database,
		s.collection,
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

func TestRead_MultipleRows(t *testing.T) {
	SetupSuite(t, defaultDatabaseTestName, defaultCollectionTestName)
	defer TearDownSuite(t, defaultDatabaseTestName)

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
	insertDocuments(t, defaultDatabaseTestName, defaultCollectionTestName, inputDocument, false).
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
			defaultDatabaseTestName,
			defaultCollectionTestName,
			readFilter,
			c.fields,
			c.expDocuments)
	}
}

func TestRead_EntireCollection(t *testing.T) {
	SetupSuite(t, defaultDatabaseTestName, defaultCollectionTestName)
	defer TearDownSuite(t, defaultDatabaseTestName)

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
	insertDocuments(t, defaultDatabaseTestName, defaultCollectionTestName, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(t,
		defaultDatabaseTestName,
		defaultCollectionTestName,
		nil,
		nil,
		inputDocument)
}

func TestRead_NestedFields(t *testing.T) {
	SetupDatabaseSuite(t, defaultDatabaseTestName)
	defer TearDownSuite(t, defaultDatabaseTestName)

	createCollection(t, defaultDatabaseTestName, defaultCollectionTestName,
		Map{
			"schema": Map{
				"title": defaultCollectionTestName,
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
	e.POST(getDocumentURL(defaultDatabaseTestName, defaultCollectionTestName, "insert")).
		WithJSON(Map{
			"documents": inputDoc,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("status", "inserted")

	readResp := readByFilter(t, defaultDatabaseTestName, defaultCollectionTestName, Map{
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
	dbName := "db_" + t.Name()
	collName := "test_collection"
	createTestCollection(t, dbName, collName, testCreateSchema)

	e := expect(t)
	r := e.POST(fmt.Sprintf("/api/v1/databases/%s/transactions/begin", dbName)).
		Expect().Status(http.StatusOK).
		Body().Raw()

	res := struct {
		TxCtx api.TransactionCtx `json:"tx_ctx"`
	}{}

	err := json.Unmarshal([]byte(r), &res)
	require.NoError(t, err)

	resp := e.POST(getDocumentURL(dbName, collName, "insert")).
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

func readAndValidate(t *testing.T, db string, collection string, filter Map, fields Map, inputDocument []Doc) {
	readResp := readByFilter(t, db, collection, filter, fields)
	require.Equal(t, len(inputDocument), len(readResp))
	for i := 0; i < len(inputDocument); i++ {
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(readResp[i]["result"], &doc))

		var actualDoc = []byte(doc["data"])
		expDoc, err := json.Marshal(inputDocument[i])
		require.NoError(t, err)
		require.JSONEqf(t, string(expDoc), string(actualDoc), "exp '%s' actual '%s'", string(expDoc), string(actualDoc))
	}
}
