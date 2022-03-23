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
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tigrisdata/tigrisdb/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

type DocumentSuite struct {
	suite.Suite

	collection string
	database   string
}

func getDocumentURL(databaseName, collectionName string, methodName string) string {
	return fmt.Sprintf("/api/v1/databases/%s/collections/%s/documents/%s", databaseName, collectionName, methodName)
}

func (s *DocumentSuite) SetupSuite() {
	createDatabase(s.T(), s.database)
	createCollection(s.T(), s.database, s.collection, testCreateSchema)
}

func (s *DocumentSuite) TearDownSuite() {
	dropDatabase(s.T(), s.database)
}

func (s *DocumentSuite) TestInsert_BadRequest() {
	cases := []struct {
		databaseName   string
		collectionName string
		documents      []interface{}
		expMessage     string
	}{
		{
			"random_database1",
			s.collection,
			[]interface{}{
				map[string]interface{}{
					"pkey_int": 1,
				},
			},
			"database doesn't exists 'random_database1'",
		}, {
			s.database,
			"random_collection",
			[]interface{}{
				map[string]interface{}{
					"pkey_int": 1,
				},
			},
			"collection doesn't exists 'random_collection'",
		}, {
			"",
			s.collection,
			[]interface{}{
				map[string]interface{}{
					"pkey_int": 1,
				},
			},
			"invalid database name",
		}, {
			s.database,
			"",
			[]interface{}{
				map[string]interface{}{
					"pkey_int": 1,
				},
			},
			"invalid collection name",
		}, {
			s.database,
			s.collection,
			[]interface{}{},
			"empty documents received",
		},
	}
	for _, c := range cases {
		e := httpexpect.New(s.T(), config.GetBaseURL())
		e.POST(getDocumentURL(c.databaseName, c.collectionName, "insert")).
			WithJSON(map[string]interface{}{
				"documents": c.documents,
			}).
			Expect().
			Status(http.StatusBadRequest).
			JSON().
			Object().
			ValueEqual("message", c.expMessage)
	}
}

func (s *DocumentSuite) TestInsert_AlreadyExists() {
	inputDocument := []interface{}{
		map[string]interface{}{
			"pkey_int":     1,
			"int_value":    10,
			"string_value": "simple_insert",
			"bool_value":   true,
			"double_value": 10.01,
			"bytes_value":  []byte(`"simple_insert"`),
		},
	}

	e := httpexpect.New(s.T(), config.GetBaseURL())
	e.POST(getDocumentURL(s.database, s.collection, "insert")).
		WithJSON(map[string]interface{}{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		Empty()

	e.POST(getDocumentURL(s.database, s.collection, "insert")).
		WithJSON(map[string]interface{}{
			"documents": inputDocument,
			"options": map[string]interface{}{
				"must_not_exist": true,
			},
		}).
		Expect().
		Status(http.StatusConflict).
		JSON().
		Object().
		ValueEqual("message", "row already exists")
}

func (s *DocumentSuite) TestInsert_SingleRow() {
	inputDocument := []interface{}{
		map[string]interface{}{
			"pkey_int":     10,
			"int_value":    10,
			"string_value": "simple_insert",
			"bool_value":   true,
			"double_value": 10.01,
			"bytes_value":  []byte(`"simple_insert"`),
		},
	}

	e := httpexpect.New(s.T(), config.GetBaseURL())
	e.POST(getDocumentURL(s.database, s.collection, "insert")).
		WithJSON(map[string]interface{}{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		Empty()

	readResp := readByFilter(s.T(), s.database, s.collection, map[string]interface{}{
		"pkey_int": 10,
	}, nil)

	var doc map[string]json.RawMessage
	require.NoError(s.T(), json.Unmarshal(readResp[0]["result"], &doc))

	var actualDoc = []byte(doc["doc"])
	expDoc, err := json.Marshal(inputDocument[0])
	require.NoError(s.T(), err)
	require.Equal(s.T(), expDoc, actualDoc)
}

func (s *DocumentSuite) TestInsert_MultipleRows() {
	inputDocument := []interface{}{
		map[string]interface{}{
			"pkey_int":     20,
			"int_value":    20,
			"string_value": "simple_insert1",
			"bool_value":   true,
			"double_value": 20.00001,
			"bytes_value":  []byte(`"simple_insert1"`),
		},
		map[string]interface{}{
			"pkey_int":     30,
			"int_value":    30,
			"string_value": "simple_insert2",
			"bool_value":   false,
			"double_value": 20.0002,
			"bytes_value":  []byte(`"simple_insert2"`),
		},
	}

	e := httpexpect.New(s.T(), config.GetBaseURL())
	e.POST(getDocumentURL(s.database, s.collection, "insert")).
		WithJSON(map[string]interface{}{
			"documents": inputDocument,
		}).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		Empty()

	readResp := readByFilter(s.T(), s.database, s.collection, map[string]interface{}{
		"$or": []interface{}{
			map[string]interface{}{
				"pkey_int": 20,
			},
			map[string]interface{}{
				"pkey_int": 30,
			},
		},
	}, nil)

	require.Equal(s.T(), 2, len(readResp))
	for i := 0; i < len(inputDocument); i++ {
		var doc map[string]json.RawMessage
		require.NoError(s.T(), json.Unmarshal(readResp[i]["result"], &doc))

		var actualDoc = []byte(doc["doc"])
		expDoc, err := json.Marshal(inputDocument[i])
		require.NoError(s.T(), err)
		require.Equal(s.T(), expDoc, actualDoc)
	}
}

func (s *DocumentSuite) TestUpdate_BadRequest() {
	cases := []struct {
		database   string
		collection string
		fields     map[string]interface{}
		filter     map[string]interface{}
		expMessage string
	}{
		{
			"random_database1",
			s.collection,
			map[string]interface{}{
				"$set": map[string]interface{}{
					"string_value": "simple_update",
				},
			},
			map[string]interface{}{
				"pkey_int": 1,
			},
			"database doesn't exists 'random_database1'",
		}, {
			s.database,
			"random_collection",
			map[string]interface{}{
				"$set": map[string]interface{}{
					"string_value": "simple_update",
				},
			},
			map[string]interface{}{
				"pkey_int": 1,
			},
			"collection doesn't exists 'random_collection'",
		}, {
			"",
			s.collection,
			map[string]interface{}{
				"$set": map[string]interface{}{
					"string_value": "simple_update",
				},
			},
			map[string]interface{}{
				"pkey_int": 1,
			},
			"invalid database name",
		}, {
			s.database,
			"",
			map[string]interface{}{
				"$set": map[string]interface{}{
					"string_value": "simple_update",
				},
			},
			map[string]interface{}{
				"pkey_int": 1,
			},
			"invalid collection name",
		}, {
			s.database,
			s.collection,
			nil,
			map[string]interface{}{
				"pkey_int": 1,
			},
			"empty fields received",
		}, {
			s.database,
			s.collection,
			map[string]interface{}{
				"$set": map[string]interface{}{
					"string_value": "simple_update",
				},
			},
			nil,
			"filter is a required field",
		},
	}
	for _, c := range cases {
		e := httpexpect.New(s.T(), config.GetBaseURL())
		e.PUT(getDocumentURL(c.database, c.collection, "update")).
			WithJSON(map[string]interface{}{
				"fields": c.fields,
				"filter": c.filter,
			}).
			Expect().
			Status(http.StatusBadRequest).
			JSON().
			Object().
			ValueEqual("message", c.expMessage)
	}
}

func (s *DocumentSuite) TestUpdate_SingleRow() {
	inputDocument := []interface{}{
		map[string]interface{}{
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
		map[string]interface{}{
			"pkey_int": 100,
		},
		nil,
		inputDocument)

	updateByFilter(s.T(),
		s.database,
		s.collection,
		map[string]interface{}{
			"filter": map[string]interface{}{
				"pkey_int": 100,
			},
		},
		map[string]interface{}{
			"fields": map[string]interface{}{
				"$set": map[string]interface{}{
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
		Empty()

	readAndValidate(s.T(),
		s.database,
		s.collection,
		map[string]interface{}{
			"pkey_int": 100,
		},
		nil,
		[]interface{}{
			map[string]interface{}{
				"pkey_int":     100,
				"int_value":    200,
				"string_value": "simple_insert1_update_modified",
				"bool_value":   false,
				"double_value": 200.00001,
				"bytes_value":  []byte(`"simple_insert1_update_modified"`),
			},
		})
}

func (s *DocumentSuite) TestUpdate_MultipleRows() {
	inputDocument := []interface{}{
		map[string]interface{}{
			"pkey_int":     110,
			"int_value":    1000,
			"string_value": "simple_insert110",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert110"`),
		},
		map[string]interface{}{
			"pkey_int":     120,
			"int_value":    2000,
			"string_value": "simple_insert120",
			"bool_value":   false,
			"double_value": 2000.22221,
			"bytes_value":  []byte(`"simple_insert120"`),
		},
		map[string]interface{}{
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

	readFilter := map[string]interface{}{
		"$or": []interface{}{
			map[string]interface{}{
				"pkey_int": 110,
			},
			map[string]interface{}{
				"pkey_int": 120,
			},
			map[string]interface{}{
				"pkey_int": 130,
			},
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
		map[string]interface{}{
			"filter": map[string]interface{}{
				"pkey_int": 10000,
			},
		},
		map[string]interface{}{
			"fields": map[string]interface{}{
				"$set": map[string]interface{}{
					"int_value": 0,
				},
			},
		}).Status(http.StatusOK).
		JSON().
		Object().
		Empty()

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
		map[string]interface{}{
			"filter": map[string]interface{}{
				"$or": []interface{}{
					map[string]interface{}{
						"pkey_int": 120,
					},
					map[string]interface{}{
						"pkey_int": 130,
					},
				},
			},
		},
		map[string]interface{}{
			"fields": map[string]interface{}{
				"$set": map[string]interface{}{
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
		Empty()

	outDocument := []interface{}{
		// this didn't change as-is
		map[string]interface{}{
			"pkey_int":     110,
			"int_value":    1000,
			"string_value": "simple_insert110",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert110"`),
		},
		map[string]interface{}{
			"pkey_int":           120,
			"int_value":          12345,
			"string_value":       "modified_120_130",
			"bool_value":         false,
			"double_value":       2000.22221,
			"bytes_value":        []byte(`"modified_120_130"`),
			"added_value_double": 1234.999999,
			"added_string_value": "new_key_added",
		},
		map[string]interface{}{
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
		filter         map[string]interface{}
		expMessage     string
	}{
		{
			"random_database1",
			s.collection,
			map[string]interface{}{
				"pkey_int": 1,
			},
			"database doesn't exists 'random_database1'",
		}, {
			s.database,
			"random_collection",
			map[string]interface{}{
				"pkey_int": 1,
			},
			"collection doesn't exists 'random_collection'",
		}, {
			"",
			s.collection,
			map[string]interface{}{
				"pkey_int": 1,
			},
			"invalid database name",
		}, {
			s.database,
			"",
			map[string]interface{}{
				"pkey_int": 1,
			},
			"invalid collection name",
		}, {
			s.database,
			s.collection,
			nil,
			"filter is a required field",
		},
	}
	for _, c := range cases {
		e := httpexpect.New(s.T(), config.GetBaseURL())
		e.DELETE(getDocumentURL(c.databaseName, c.collectionName, "delete")).
			WithJSON(map[string]interface{}{
				"filter": c.filter,
			}).
			Expect().
			Status(http.StatusBadRequest).
			JSON().
			Object().
			ValueEqual("message", c.expMessage)
	}
}

func (s *DocumentSuite) TestDelete_SingleRow() {
	inputDocument := []interface{}{
		map[string]interface{}{
			"pkey_int":  40,
			"int_value": 10,
		},
	}

	insertDocuments(s.T(), s.database, s.collection, inputDocument, false).
		Status(http.StatusOK)

	readAndValidate(s.T(),
		s.database,
		s.collection,
		map[string]interface{}{
			"pkey_int": 40,
		},
		nil,
		inputDocument)

	deleteByFilter(s.T(), s.database, s.collection, map[string]interface{}{
		"filter": map[string]interface{}{
			"pkey_int": 40,
		},
	}).Status(http.StatusOK).
		JSON().
		Object().
		Empty()

	readAndValidate(s.T(),
		s.database,
		s.collection,
		map[string]interface{}{
			"pkey_int": 40,
		},
		nil,
		nil)
}

func (s *DocumentSuite) TestDelete_MultipleRows() {
	inputDocument := []interface{}{
		map[string]interface{}{
			"pkey_int":     50,
			"string_value": "simple_insert50",
		},
		map[string]interface{}{
			"pkey_int":     60,
			"string_value": "simple_insert60",
		},
		map[string]interface{}{
			"pkey_int":     70,
			"string_value": "simple_insert70",
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(s.T(), s.database, s.collection, inputDocument, false).
		Status(http.StatusOK)

	readFilter := map[string]interface{}{
		"$or": []interface{}{
			map[string]interface{}{
				"pkey_int": 50,
			},
			map[string]interface{}{
				"pkey_int": 60,
			},
			map[string]interface{}{
				"pkey_int": 70,
			},
		},
	}
	readAndValidate(s.T(),
		s.database,
		s.collection,
		readFilter,
		nil,
		inputDocument)

	// first try deleting a no-op operation i.e. random filter value
	deleteByFilter(s.T(), s.database, s.collection, map[string]interface{}{
		"filter": map[string]interface{}{
			"pkey_int": 10000,
		},
	}).Status(http.StatusOK).
		JSON().
		Object().
		Empty()

	// read all documents back
	readAndValidate(s.T(),
		s.database,
		s.collection,
		readFilter,
		nil,
		inputDocument)

	// DELETE keys 50 and 70
	deleteByFilter(s.T(), s.database, s.collection, map[string]interface{}{
		"filter": map[string]interface{}{
			"$or": []interface{}{
				map[string]interface{}{
					"pkey_int": 50,
				},
				map[string]interface{}{
					"pkey_int": 70,
				},
			},
		},
	}).Status(http.StatusOK).
		JSON().
		Object().
		Empty()

	readAndValidate(s.T(),
		s.database,
		s.collection,
		readFilter,
		nil,
		[]interface{}{
			map[string]interface{}{
				"pkey_int":     60,
				"string_value": "simple_insert60",
			},
		},
	)
}

func (s *DocumentSuite) TestRead_MultipleRows() {
	inputDocument := []interface{}{
		map[string]interface{}{
			"pkey_int":     210,
			"int_value":    1000,
			"string_value": "simple_insert110",
			"bool_value":   true,
			"double_value": 1000.000001,
			"bytes_value":  []byte(`"simple_insert110"`),
		},
		map[string]interface{}{
			"pkey_int":     220,
			"int_value":    2000,
			"string_value": "simple_insert120",
			"bool_value":   false,
			"double_value": 2000.22221,
			"bytes_value":  []byte(`"simple_insert120"`),
		},
		map[string]interface{}{
			"pkey_int":     230,
			"string_value": "simple_insert130",
			"bytes_value":  []byte(`"simple_insert130"`),
		},
	}

	// should always succeed with mustNotExists as false
	insertDocuments(s.T(), s.database, s.collection, inputDocument, false).
		Status(http.StatusOK)

	readFilter := map[string]interface{}{
		"$or": []interface{}{
			map[string]interface{}{
				"pkey_int": 210,
			},
			map[string]interface{}{
				"pkey_int": 220,
			},
			map[string]interface{}{
				"pkey_int": 230,
			},
		},
	}

	cases := []struct {
		fields       map[string]interface{}
		expDocuments []interface{}
	}{
		{
			map[string]interface{}{
				"int_value":    1,
				"string_value": 1,
				"bytes_value":  1,
			},
			[]interface{}{
				map[string]interface{}{
					"int_value":    1000,
					"string_value": "simple_insert110",
					"bytes_value":  []byte(`"simple_insert110"`),
				},
				map[string]interface{}{
					"int_value":    2000,
					"string_value": "simple_insert120",
					"bytes_value":  []byte(`"simple_insert120"`),
				},
				map[string]interface{}{
					"string_value": "simple_insert130",
					"bytes_value":  []byte(`"simple_insert130"`),
				},
			},
		}, {
			// bool is not present in the third document
			map[string]interface{}{
				"string_value": 1,
				"bool_value":   1,
			},
			[]interface{}{
				map[string]interface{}{
					"string_value": "simple_insert110",
					"bool_value":   true,
				},
				map[string]interface{}{
					"string_value": "simple_insert120",
					"bool_value":   false,
				},
				map[string]interface{}{
					"string_value": "simple_insert130",
				},
			},
		}, {
			// both are not present in the third document
			map[string]interface{}{
				"double_value": 1,
				"bool_value":   1,
			},
			[]interface{}{
				map[string]interface{}{
					"double_value": 1000.000001,
					"bool_value":   true,
				},
				map[string]interface{}{
					"double_value": 2000.22221,
					"bool_value":   false,
				},
				map[string]interface{}{},
			},
		},
	}
	for _, c := range cases {
		readAndValidate(s.T(),
			s.database,
			s.collection,
			readFilter,
			c.fields,
			c.expDocuments)
	}
}

func insertDocuments(t *testing.T, db string, collection string, documents []interface{}, mustNotExist bool) *httpexpect.Response {
	e := httpexpect.New(t, config.GetBaseURL())

	return e.POST(getDocumentURL(db, collection, "insert")).
		WithJSON(map[string]interface{}{
			"documents": documents,
			"options": map[string]interface{}{
				"must_not_exist": mustNotExist,
			},
		}).Expect()
}

func updateByFilter(t *testing.T, db string, collection string, filter map[string]interface{}, fields map[string]interface{}) *httpexpect.Response {
	var payload = make(map[string]interface{})
	for key, value := range filter {
		payload[key] = value
	}
	for key, value := range fields {
		payload[key] = value
	}
	e := httpexpect.New(t, config.GetBaseURL())
	return e.PUT(getDocumentURL(db, collection, "update")).
		WithJSON(payload).
		Expect()
}

func deleteByFilter(t *testing.T, db string, collection string, filter map[string]interface{}) *httpexpect.Response {
	e := httpexpect.New(t, config.GetBaseURL())
	return e.DELETE(getDocumentURL(db, collection, "delete")).
		WithJSON(filter).
		Expect()
}

func readByFilter(t *testing.T, db string, collection string, filter map[string]interface{}, fields map[string]interface{}) []map[string]json.RawMessage {
	var payload = make(map[string]interface{})
	payload["fields"] = fields
	payload["filter"] = filter

	e := httpexpect.New(t, config.GetBaseURL())
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

func readAndValidate(t *testing.T, db string, collection string, filter map[string]interface{}, fields map[string]interface{}, inputDocument []interface{}) {
	readResp := readByFilter(t, db, collection, filter, fields)
	require.Equal(t, len(inputDocument), len(readResp))
	for i := 0; i < len(inputDocument); i++ {
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(readResp[i]["result"], &doc))

		var actualDoc = []byte(doc["doc"])
		expDoc, err := json.Marshal(inputDocument[i])
		require.NoError(t, err)
		require.JSONEqf(t, string(expDoc), string(actualDoc), "exp '%s' actual '%s'", string(expDoc), string(actualDoc))
	}
}
