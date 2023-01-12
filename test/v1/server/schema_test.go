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

	"github.com/tigrisdata/tigris/server/config"
)

func TestSchemaMigration(t *testing.T) {
	if config.DefaultConfig.Search.WriteEnabled {
		t.Skip("TypeSense doesn't support incompatible schema changes")
	}

	if !config.DefaultConfig.Schema.AllowIncompatible {
		t.Skip("incompatible schema changes disabled")
	}

	coll := t.Name()

	var testSchemaV1 = Map{
		"schema": Map{
			"title":       coll,
			"description": "this schema is for " + coll + " tests",
			"properties": Map{
				"pkey_int": Map{
					"type": "integer",
				},
				"field1": Map{
					"type": "integer",
				},
				"field2": Map{
					"type": "string",
				},
				"field3": Map{
					"type": "string",
				},
			},
			"primary_key": []interface{}{"pkey_int"},
		},
	}

	// change field1 type to string and delete field3
	var testSchemaV2 = Map{
		"schema": Map{
			"title":       coll,
			"description": "this schema is for " + coll + " tests",
			"properties": Map{
				"pkey_int": Map{
					"type": "integer",
				},
				"field1": Map{
					"type": "string",
				},
				"field2": Map{
					"type": "string",
				},
			},
			"primary_key": []interface{}{"pkey_int"},
		},
	}

	// change field2 type to integer and re-add field3
	var testSchemaV3 = Map{
		"schema": Map{
			"title":       coll,
			"description": "this schema is for " + coll + " tests",
			"properties": Map{
				"pkey_int": Map{
					"type": "integer",
				},
				"field1": Map{
					"type": "string",
				},
				"field2": Map{
					"type": "integer",
				},
				"field3": Map{
					"type": "string",
				},
			},
			"primary_key": []interface{}{"pkey_int"},
		},
	}

	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	createCollection(t, db, coll, testSchemaV1).Status(http.StatusOK)

	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 1, "field1": 123, "field2": "567", "field3": "fld3_v1"}}, true).Status(http.StatusOK)

	createCollection(t, db, coll, testSchemaV2).Status(http.StatusOK)

	// fails because field1 is from previous schema version
	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 2, "field1": 1230, "field2": "5670"}}, true).Status(http.StatusBadRequest)
	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 2, "field1": "1230", "field2": "5670"}}, true).Status(http.StatusOK)

	createCollection(t, db, coll, testSchemaV3).Status(http.StatusOK)

	// fails because field2 is from previous schema version
	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 3, "field1": "12300", "field2": "sss56700"}}, true).Status(http.StatusBadRequest)
	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 3, "field1": "12300", "field2": 56700, "field3": "fld3_v3"}}, true).Status(http.StatusOK)

	readAndValidate(t, db, coll, Map{}, nil, []Doc{
		{"pkey_int": 1, "field1": "123", "field2": 567}, // field3 should not reappear so as it was deleted in schema V2, while the row was updated before
		{"pkey_int": 2, "field1": "1230", "field2": 5670},
		{"pkey_int": 3, "field1": "12300", "field2": 56700, "field3": "fld3_v3"}, // field3 appear because the row was updated after it was re-added in schema V3
	})

	// test update flow
	updateByFilter(t, db, coll,
		Map{
			"filter": Map{
				"$or": []Doc{
					{"pkey_int": 1},
					{"pkey_int": 2},
					{"pkey_int": 3},
				},
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"field3": "new_val",
				},
			},
		},
		nil,
	)

	readAndValidate(t, db, coll, Map{}, nil, []Doc{
		{"pkey_int": 1, "field1": "123", "field2": 567, "field3": "new_val"}, // field3 should not reappear so as it was deleted in schema V2, while the row was updated before
		{"pkey_int": 2, "field1": "1230", "field2": 5670, "field3": "new_val"},
		{"pkey_int": 3, "field1": "12300", "field2": 56700, "field3": "new_val"}, // field3 appear because the row was updated after it was re-added in schema V3
	})
}
