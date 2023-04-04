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

package schema

import (
	"os"
	"testing"

	ulog "github.com/tigrisdata/tigris/util/log"
)

var (
	typesTest = `{
        "title": "products",
        "properties": {
          "id": { "type": "integer", "format": "int32" },
          "name": { "type": "string" },
          "price": { "type": "number" },
          "int64": { "type": "integer", "format": "int64" },
          "bool": { "type": "boolean"},
          "byte1": { "type": "string", "format": "byte"},
          "time1": { "type": "string", "format": "date-time"},
          "uUID1": { "type": "string", "format": "uuid"},
          "arrInts": { "type": "array", "items" : { "type" : "integer" } },
          "int64WithDesc": { "type": "integer", "format": "int64", "description": "field description" },
          "twoDArr": { "type": "array", "items" : { "type": "array", "items" : { "type" : "integer" } } }
		}}`
	tagsTest = `{
        "title": "products",
        "primary_key": ["Key", "KeyGenIdx", "name_key", "name_gen_key"],
        "description": "type description",
        "required": ["max_len_str_req", "req_field", "time_f"],
        "properties": {
          "Gen": { "type": "integer", "format": "int32", "autoGenerate": true },
          "Key": { "type": "integer", "format": "int32"},
          "KeyGenIdx": { "type": "integer", "format": "int32", "autoGenerate": true },
          "name_key": { "type": "integer", "format": "int32" },
          "name_si": { "type": "string", "searchIndex": true },
          "name_sif": { "type": "integer", "format": "int32", "searchIndex": true, "facet": true },
          "name_sifs": { "type": "string", "format": "date-time", "searchIndex": true, "facet": true, "sort": true },
          "user_name": { "type": "integer", "format": "int32" },
          "name_gen": { "type": "integer", "format": "int32", "autoGenerate": true },
          "name_gen_key": { "type": "integer", "format": "int32", "autoGenerate": true },
          "def_val_int": { "type": "integer", "default": 32 },
          "def_val_str": { "type": "string", "default": "str1" },
          "def_val_str_q": { "type": "string", "default": "st'r1" },
          "def_val_date": { "type": "string", "format": "date-time", "default": "now()" },
          "def_val_date_const": { "type": "string", "format": "date-time", "default": "2022-12-01T21:21:21.409Z" },
          "def_val_uuid": { "type": "string", "format": "uuid", "default": "uuid()" },
          "def_val_cuid": { "type": "string", "default": "cuid()" },
          "max_len_str": { "type": "string", "maxLength" : 11 },
          "max_len_str_req": { "type": "string", "maxLength" : 11 },
          "req_field": { "type": "integer", "format": "int32"},
          "time_f": { "type": "string", "format": "date-time", "default": "now()", "updatedAt": true, "createdAt": true }
		}}`
	objectTest = `{
          "title": "products",
          "primary_key": ["Id"],
          "properties": {
            "subtype": {
              "type": "object",
              "description": "sub type description",
              "properties": {
                  "id2": { "type": "integer", "format": "int32"}
              }
            },
            "subArrays": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "field_3": { "type": "integer", "format": "int32"},
				  "subArrayNesteds": {
                    "type": "array",
                    "items": {
                      "type": "object",
                      "properties": { "field_arr": { "type": "integer", "format": "int32" } }
                    }
                  },
				  "subObjectNested": {
                    "type": "object",
                    "properties": { "field_3": { "type": "integer", "format": "int32" } }
                  },
				  "subObjectNestedReuseTypeByBody": {
                    "type": "object",
                    "properties": { "field_3": { "type": "integer", "format": "int32" } }
                  },
				  "subObjectNestedOne": {
                    "type": "object",
                    "properties": { 
                  		"field_31": { "type": "integer", "format": "int32"},
				  		"subObjectNested": {
                    		"type": "object",
                    		"properties": { "field_3": { "type": "integer", "format": "int32" } }
						}
					}
                  },
				  "subObjectNestedTwo": {
                    "type": "object",
                    "properties": { 
                  		"field_32": { "type": "integer", "format": "int32"},
				  		"subObjectNested": {
                    		"type": "object",
                    		"properties": { "field_4": { "type": "integer", "format": "int32" } }
						}
					}
                  },
				  "subObjectNestedThree": {
                    "type": "object",
                    "properties": { 
                  		"field_33": { "type": "integer", "format": "int32"},
				  		"subObjectNested": {
                    		"type": "object",
                    		"properties": { "field_4": { "type": "integer", "format": "int32" } }
						}
					}
                  }
                }
              }
		    }
          }
        }`

	noGoTagSchema = `{
        "title": "products",
        "properties": {
          "Name": { "type": "string" }
		}}`
)

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled"})
	os.Exit(m.Run())
}
