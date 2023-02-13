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
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

var testQuerySchema = Map{
	"schema": Map{
		"title":       testCollection,
		"description": "this schema is for integration tests",
		"properties": Map{
			"pkey_int": Map{
				"description": "primary key field",
				"type":        "integer",
			},
			"int_value": Map{
				"description": "simple int field",
				"type":        "integer",
			},
			"string_value": Map{
				"description": "simple string field and part of pkey",
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
			// "array_obj_value": Map{
			// 	"description": "array field",
			// 	"type":        "array",
			// 	"items": Map{
			// 		"type": "object",
			// 		"properties": Map{
			// 			"id": Map{
			// 				"type": "integer",
			// 			},
			// 			"product": Map{
			// 				"type": "string",
			// 			},
			// 		},
			// 	},
			// },
			// "object_value": Map{
			// 	"description": "object field",
			// 	"type":        "object",
			// 	"properties": Map{
			// 		"name": Map{
			// 			"type": "string",
			// 		},
			// 		"bignumber": Map{
			// 			"type": "integer",
			// 		},
			// 	},
			// },
		},
		"primary_key": []interface{}{"pkey_int", "string_value"},
	},
}

func setupQueryTests(t *testing.T) (string, string) {
	db := fmt.Sprintf("integration_%s", t.Name())
	deleteProject(t, db)
	createProject(t, db).Status(http.StatusOK)
	createCollection(t, db, testCollection, testQuerySchema).Status(http.StatusOK)

	return db, testCollection
}

func insertDocs(t *testing.T, db string, coll string, extra ...Doc) {
	inputDocument := []Doc{
		{
			"pkey_int":        1,
			"int_value":       10,
			"string_value":    "a",
			"bool_value":      true,
			"double_value":    10.01,
			"bytes_value":     []byte{1, 2, 3, 4},
			"uuid_value":      uuid.New().String(),
			"date_time_value": "2015-12-21T17:42:34Z",
		},
		{
			"pkey_int":        2,
			"int_value":       1,
			"string_value":    "G",
			"bool_value":      false,
			"double_value":    5.05,
			"bytes_value":     []byte{4, 4, 4},
			"uuid_value":      uuid.New().String(),
			"date_time_value": "2016-10-12T17:42:34Z",
		},
		{
			"pkey_int":        3,
			"int_value":       100,
			"string_value":    "B",
			"bool_value":      false,
			"double_value":    1000,
			"bytes_value":     []byte{3, 4, 4},
			"uuid_value":      uuid.New().String(),
			"date_time_value": "2013-11-01T17:42:34Z",
		},
		{
			"pkey_int":        4,
			"int_value":       5,
			"string_value":    "z",
			"bool_value":      true,
			"double_value":    25.05,
			"bytes_value":     []byte{4, 4, 4},
			"uuid_value":      uuid.New().String(),
			"date_time_value": "2020-10-12T17:42:34Z",
		},
		{
			"pkey_int":        30,
			"int_value":       30,
			"string_value":    "k",
			"bool_value":      false,
			"double_value":    5.05,
			"bytes_value":     []byte{4, 4, 4},
			"uuid_value":      uuid.New().String(),
			"date_time_value": "2014-10-12T17:42:34Z",
		},
	}

	inputDocument = append(inputDocument, extra...)

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
}

func getIds(docs []map[string]jsoniter.RawMessage) []int {
	var ids []int
	for _, docString := range docs {

		var result map[string]json.RawMessage
		var doc map[string]json.RawMessage

		json.Unmarshal(docString["result"], &result)
		json.Unmarshal(result["data"], &doc)

		var id int
		json.Unmarshal(doc["pkey_int"], &id)
		ids = append(ids, id)
	}

	return ids
}

func TestQuery_EQ(t *testing.T) {
	db, coll := setupTests(t)
	insertDocs(t, db, coll)
	defer cleanupTests(t, db)

	cases := []struct {
		filter Map
		ids    []int
	}{
		{
			Map{"int_value": 10},
			[]int{1},
		},
		{
			Map{"bool_value": false},
			[]int{2, 3, 30},
		},
		{
			Map{"bool_value": false, "int_value": 3},
			[]int(nil),
		},
		{
			Map{"bool_value": false, "int_value": 30},
			[]int{30},
		},
		{
			Map{
				"$and": []interface{}{
					Map{"string_value": Map{"$eq": "G"}},
					Map{"bool_value": false},
				},
			},
			[]int{2},
		},
		// {
		// 	Map{
		// 		"$or": []interface{}{
		// 			Map{"int_value": Map{"$eq": 10}},
		// 			Map{"int_value": 100},
		// 			Map{"double_value": 25.05},
		// 		},
		// 	},
		// []int{2},
		// },
	}

	for _, query := range cases {
		resp := readByFilter(t, db, coll, query.filter, nil, nil, nil)
		ids := getIds(resp)
		assert.Equal(t, len(ids), len(query.ids))
		assert.Equal(t, query.ids, ids, query.filter)
	}
}

func TestQuery_Range(t *testing.T) {
	db, coll := setupTests(t)
	insertDocs(t, db, coll)
	defer cleanupTests(t, db)

	cases := []struct {
		filter Map
		ids    []int
	}{
		{
			Map{"int_value": Map{"$gt": 0}},
			[]int{2, 4, 1, 30, 3},
		},
		{
			Map{"int_value": Map{"$lt": 30}},
			[]int{2, 4, 1},
		},
		{
			Map{
				"$and": []interface{}{
					Map{"int_value": Map{"$gte": 30}},
					Map{"int_value": Map{"$lte": 100}},
				},
			},
			[]int{30, 3},
		},
		{
			Map{"string_value": Map{"$gt": "B"}},
			[]int{2, 30, 4},
		},
		{
			Map{"string_value": Map{"$lt": "G"}},
			[]int{1, 3},
		},
		{
			Map{
				"$and": []interface{}{
					Map{"string_value": Map{"$gte": "G"}},
					Map{"string_value": Map{"$lt": "z"}},
				},
			},
			[]int{2, 30},
		},
		{
			Map{
				"$and": []interface{}{
					Map{"bool_value": Map{"$gte": true}},
					Map{"bool_value": Map{"$lte": true}},
				},
			},
			[]int{1, 4},
		},
		{
			Map{"double_value": Map{"$gt": 10}},
			[]int{1, 4, 3},
		},
		{
			Map{"double_value": Map{"$lt": 26}},
			[]int{2, 30, 1, 4},
		},
		{
			Map{
				"$and": []interface{}{
					Map{"double_value": Map{"$gte": 10.01}},
					Map{"double_value": Map{"$lt": 1000}},
				},
			},
			[]int{1, 4},
		},
		{
			Map{"date_time_value": Map{"$gt": "2015-12.22T17:42:34Z"}},
			[]int{2, 4},
		},
		{
			Map{"date_time_value": Map{"$lt": "2015-12.22T17:42:34Z"}},
			[]int{3, 30, 1},
		},
		{
			Map{"created_at": Map{"$gt": "2022-12.22T17:42:34Z"}},
			[]int{1, 2, 3, 4, 30},
		},
		{
			Map{
				"$and": []interface{}{
					Map{"date_time_value": Map{"$gte": "2013-11-01T17:42:34Z"}},
					Map{"date_time_value": Map{"$lt": "2015-12.22T17:42:34Z"}},
				},
			},
			[]int{3, 30, 1},
		},
	}

	for _, query := range cases {
		resp := readByFilter(t, db, coll, query.filter, nil, nil, nil)
		ids := getIds(resp)
		assert.Equal(t, len(query.ids), len(ids), query.filter)
		assert.Equal(t, query.ids, ids, query.filter)
	}
}

func TestQuery_RangeWithNull(t *testing.T) {
	db, coll := setupTests(t)
	insertDocs(t, db, coll, Doc{
		"pkey_int":        50,
		"int_value":       nil,
		"string_value":    nil,
		"bool_value":      nil,
		"double_value":    nil,
		"bytes_value":     nil,
		"uuid_value":      nil,
		"date_time_value": nil,
	})
	defer cleanupTests(t, db)

	cases := []struct {
		filter Map
		ids    []int
	}{
		{
			Map{"int_value": Map{"$eq": nil}},
			[]int{50},
		},
		{
			Map{"int_value": Map{"$gt": nil}},
			[]int{2, 4, 1, 30, 3},
		},
		{
			Map{"int_value": Map{"$gte": nil}},
			[]int{50, 2, 4, 1, 30, 3},
		},
		{
			Map{"int_value": Map{"$lt": 30}},
			[]int{50, 2, 4, 1},
		},
		{
			Map{"string_value": Map{"$gt": "B"}},
			[]int{2, 30, 4},
		},
		{
			Map{"string_value": Map{"$lt": "G"}},
			[]int{50, 1, 3},
		},
		{
			Map{
				"$and": []interface{}{
					Map{"string_value": Map{"$gte": "G"}},
					Map{"string_value": Map{"$lt": "z"}},
				},
			},
			[]int{2, 30},
		},
		{
			Map{
				"$and": []interface{}{
					Map{"bool_value": Map{"$gte": true}},
					Map{"bool_value": Map{"$lte": true}},
				},
			},
			[]int{1, 4},
		},
		{
			Map{"double_value": Map{"$gt": nil}},
			[]int{2, 30, 1, 4, 3},
		},
		{
			Map{"double_value": Map{"$gte": nil}},
			[]int{50, 2, 30, 1, 4, 3},
		},
		{
			Map{"double_value": Map{"$gt": 10}},
			[]int{1, 4, 3},
		},
		{
			Map{"double_value": Map{"$lt": 26}},
			[]int{50, 2, 30, 1, 4},
		},
		{
			Map{"date_time_value": Map{"$lt": "2015-12.22T17:42:34Z"}},
			[]int{50, 3, 30, 1},
		},
	}

	for _, query := range cases {
		resp := readByFilter(t, db, coll, query.filter, nil, nil, nil)
		ids := getIds(resp)
		assert.Equal(t, len(query.ids), len(ids), query.filter)
		assert.Equal(t, query.ids, ids, query.filter)
	}
}

func TestQuery_LongStrings(t *testing.T) {
	db, coll := setupTests(t)
	insertDocs(t, db, coll,
		Doc{
			"pkey_int":        50,
			"int_value":       nil,
			"string_value":    "Hi, this is a very long string that will be cut off at 64 bytes of length",
			"bool_value":      nil,
			"double_value":    nil,
			"bytes_value":     nil,
			"uuid_value":      nil,
			"date_time_value": nil,
		},
		Doc{
			"pkey_int":        60,
			"int_value":       nil,
			"string_value":    "Hi, this is a very long string that will be cut off at 64 bytes of length but it is different to the other",
			"bool_value":      nil,
			"double_value":    nil,
			"bytes_value":     nil,
			"uuid_value":      nil,
			"date_time_value": nil,
		},
		Doc{
			"pkey_int":        70,
			"int_value":       nil,
			"string_value":    "Hi, this is a very long string that will be cut off at 64 bytes of length and then has something different",
			"bool_value":      nil,
			"double_value":    nil,
			"bytes_value":     nil,
			"uuid_value":      nil,
			"date_time_value": nil,
		},
	)
	defer cleanupTests(t, db)

	cases := []struct {
		filter Map
		ids    []int
	}{
		{
			Map{"string_value": Map{"$eq": "Hi, this is a very long string that will be cut off at 64 bytes of length but it is different to the other"}},
			[]int{60},
		},
	}

	for _, query := range cases {
		resp := readByFilter(t, db, coll, query.filter, nil, nil, nil)
		ids := getIds(resp)
		assert.Equal(t, len(query.ids), len(ids), query.filter)
		assert.Equal(t, query.ids, ids, query.filter)
	}
}
