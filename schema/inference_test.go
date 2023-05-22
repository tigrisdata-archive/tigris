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

//nolint:funlen
package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	schema "github.com/tigrisdata/tigris/schema/lang"
)

func TestSchemaInference(t *testing.T) {
	cases := []struct {
		name string
		in   [][]byte
		exp  *schema.Schema
	}{
		{
			"types", [][]byte{
				[]byte(`{
	"str_field" : "str_value",
	"int_field" : 1,
	"float_field" : 1.1,
	"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
	"time_field" : "2022-11-04T16:17:23.967964263-07:00",
	"bool_field" : true,
	"binary_field": "cGVlay1hLWJvbwo=",
	"object" : {
		"str_field" : "str_value",
		"int_field" : 1,
		"float_field" : 1.1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"bool_field" : true,
		"binary_field": "cGVlay1hLWJvbwo="
	},
	"array" : [ {
		"str_field" : "str_value",
		"int_field" : 1,
		"float_field" : 1.1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"bool_field" : true,
		"binary_field": "cGVlay1hLWJvbwo="
	} ],
    "prim_array" : [ "str" ],
    "array_uuid" : [ "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" ]
}`),
			}, &schema.Schema{
				Name: "types",
				Fields: map[string]*schema.Field{
					"uuid_field":   {Type: schema.NewStringType(), Format: "uuid"},
					"time_field":   {Type: schema.NewStringType(), Format: "date-time"},
					"str_field":    {Type: schema.NewStringType()},
					"bool_field":   {Type: schema.NewBooleanType()},
					"float_field":  {Type: schema.NewFloatType()},
					"int_field":    {Type: schema.NewIntegerType()},
					"binary_field": {Type: schema.NewStringType(), Format: "byte"},
					"object": {Type: schema.NewObjectType(), Fields: map[string]*schema.Field{
						"uuid_field":   {Type: schema.NewStringType(), Format: "uuid"},
						"time_field":   {Type: schema.NewStringType(), Format: "date-time"},
						"str_field":    {Type: schema.NewStringType()},
						"bool_field":   {Type: schema.NewBooleanType()},
						"float_field":  {Type: schema.NewFloatType()},
						"int_field":    {Type: schema.NewIntegerType()},
						"binary_field": {Type: schema.NewStringType(), Format: "byte"},
					}},
					"array": {
						Type: schema.NewMultiType("array"),
						Items: &schema.Field{
							Type: schema.NewObjectType(),
							Fields: map[string]*schema.Field{
								"uuid_field":   {Type: schema.NewStringType(), Format: "uuid"},
								"time_field":   {Type: schema.NewStringType(), Format: "date-time"},
								"str_field":    {Type: schema.NewStringType()},
								"bool_field":   {Type: schema.NewBooleanType()},
								"float_field":  {Type: schema.NewFloatType()},
								"int_field":    {Type: schema.NewIntegerType()},
								"binary_field": {Type: schema.NewStringType(), Format: "byte"},
							},
						},
					},
					"prim_array": {
						Type: schema.NewMultiType("array"),
						Items: &schema.Field{
							Type: schema.NewStringType(),
						},
					},
					"array_uuid": {
						Type: schema.NewMultiType("array"),
						Items: &schema.Field{
							Type:   schema.NewStringType(),
							Format: jsonSpecFormatUUID,
						},
					},
				},
			},
		},

		{
			"empty_obj_arr", [][]byte{
				[]byte(`{
	"int_field" : 1,
	"empty_object" : {},
    "empty_array" : [],
	"object_with_empty_arr" : { "empty_array_emb": [] },
	"array_with_empty_obj" : [ {} ],
	"str_field" : "str_value"
}`),
			}, &schema.Schema{
				Name: "empty_obj_arr",
				Fields: map[string]*schema.Field{
					"int_field": {Type: schema.NewIntegerType()},
					"str_field": {Type: schema.NewStringType()},
				},
			},
		},

		{
			"broaden_type", [][]byte{
				[]byte(`{
	"int_field" : 1,
	"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
	"time_field" : "2022-11-04T16:17:23.967964263-07:00",
	"binary_field": "cGVlay1hLWJvbwo=",
	"object" : {
		"int_field" : 1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"binary_field": "cGVlay1hLWJvbwo="
	},
	"array" : [ {
		"int_field" : 1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"binary_field": "cGVlay1hLWJvbwo="
	} ]
}`),
				[]byte(`{
	"int_field" : 1.1,
	"uuid_field" : "str9cd3a2ea3d58e9d1",
	"time_field" : "20221104T16172396796426307:00",
	"binary_field": "not_base64",
	"object" : {
		"int_field" : 1.2,
		"uuid_field" : "str9cd3a2ea3d58e9d1111",
		"time_field" : "20221104T1617239679642630700111",
		"binary_field": "not_base6411111"
	},
	"array" : [ {
		"int_field" : 1.3,
		"uuid_field" : "nnnnn1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "ttttt2022-11-04T16:17:23.967964263-07:00",
		"binary_field": "notbase64"
	} ]
}`),
			}, &schema.Schema{
				Name: "broaden_type",
				Fields: map[string]*schema.Field{
					"uuid_field":   {Type: schema.NewStringType()},
					"time_field":   {Type: schema.NewStringType()},
					"int_field":    {Type: schema.NewFloatType()},
					"binary_field": {Type: schema.NewStringType()},
					"object": {Type: schema.NewObjectType(), Fields: map[string]*schema.Field{
						"uuid_field":   {Type: schema.NewStringType()},
						"time_field":   {Type: schema.NewStringType()},
						"int_field":    {Type: schema.NewFloatType()},
						"binary_field": {Type: schema.NewStringType()},
					}},
					"array": {
						Type: schema.NewMultiType("array"),
						Items: &schema.Field{
							Type: schema.NewObjectType(),
							Fields: map[string]*schema.Field{
								"uuid_field":   {Type: schema.NewStringType()},
								"time_field":   {Type: schema.NewStringType()},
								"int_field":    {Type: schema.NewFloatType()},
								"binary_field": {Type: schema.NewStringType()},
							},
						},
					},
				},
			},
		},
		{
			"broaden_array_type", [][]byte{
				[]byte(`{
	"array" : [ {
		"int_field" : 1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"binary_field": "cGVlay1hLWJvbwo="
	},
	 {
		"int_field" : 1.3,
		"uuid_field" : "nnnnn1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "ttttt2022-11-04T16:17:23.967964263-07:00",
		"binary_field": "notbase64"
	} ]
}`),
			}, &schema.Schema{
				Name: "broaden_array_type",
				Fields: map[string]*schema.Field{
					"array": {
						Type: schema.NewMultiType("array"),
						Items: &schema.Field{
							Type: schema.NewObjectType(),
							Fields: map[string]*schema.Field{
								"uuid_field":   {Type: schema.NewStringType()},
								"time_field":   {Type: schema.NewStringType()},
								"int_field":    {Type: schema.NewFloatType()},
								"binary_field": {Type: schema.NewStringType()},
							},
						},
					},
				},
			},
		},
		{
			"adding_fields", [][]byte{
				[]byte(`{
	"int_field" : 1,
	"object" : {
		"int_field" : 1
	},
	"array" : [ {
		"int_field" : 1
	} ]
}`),
				[]byte(`{
	"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
	"object" : {
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"
	},
	"array" : [ {
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"
	} ]
}`),
			}, &schema.Schema{
				Name: "adding_fields",
				Fields: map[string]*schema.Field{
					"uuid_field": {Type: schema.NewStringType(), Format: jsonSpecFormatUUID},
					"int_field":  {Type: schema.NewIntegerType()},
					"object": {Type: schema.NewObjectType(), Fields: map[string]*schema.Field{
						"uuid_field": {Type: schema.NewStringType(), Format: jsonSpecFormatUUID},
						"int_field":  {Type: schema.NewIntegerType()},
					}},
					"array": {
						Type: schema.NewMultiType("array"),
						Items: &schema.Field{
							Type: schema.NewObjectType(),
							Fields: map[string]*schema.Field{
								"uuid_field": {Type: schema.NewStringType(), Format: jsonSpecFormatUUID},
								"int_field":  {Type: schema.NewIntegerType()},
							},
						},
					},
				},
			},
		},
		{
			"adding_array_object_fields", [][]byte{
				[]byte(`{ "array" : [ { "int_field" : 1 }, { "int_field_two" : 1 } ] }`),
			}, &schema.Schema{
				Name: "adding_array_object_fields",
				Fields: map[string]*schema.Field{
					"array": {
						Type: schema.NewMultiType("array"),
						Items: &schema.Field{
							Type: schema.NewObjectType(),
							Fields: map[string]*schema.Field{
								"int_field":     {Type: schema.NewIntegerType()},
								"int_field_two": {Type: schema.NewIntegerType()},
							},
						},
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var sch schema.Schema
			err := Infer(&sch, c.name, c.in, nil, nil, 0)
			require.NoError(t, err)
			assert.Equal(t, c.exp, &sch)
		})
	}
}

func TestSchemaInferenceNegative(t *testing.T) {
	cases := []struct {
		name string
		in   [][]byte
		err  error
	}{
		{
			"incompatible_primitive",
			[][]byte{
				[]byte(`{ "incompatible_field" : 1 }`),
				[]byte(`{ "incompatible_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_prim_to_object",
			[][]byte{
				[]byte(`{ "incompatible_field" : 1 }`),
				[]byte(`{ "incompatible_field" : { "field1": "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" } }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_object_to_prim",
			[][]byte{
				[]byte(`{ "incompatible_field" : { "field1": "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" } }`),
				[]byte(`{ "incompatible_field" : 1 }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_array_to_prim",
			[][]byte{
				[]byte(`{ "incompatible_field" : ["1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"] }`),
				[]byte(`{ "incompatible_field" : 1 }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_prim_to_array",
			[][]byte{
				[]byte(`{ "incompatible_field" : 1 }`),
				[]byte(`{ "incompatible_field" : ["1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"] }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_array_mixed",
			[][]byte{
				[]byte(`{ "incompatible_field" : ["1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1", 1] }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_array",
			[][]byte{
				[]byte(`{ "incompatible_field" : [ 1 ] }`),
				[]byte(`{ "incompatible_field" : ["1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"] }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_array_object_mixed",
			[][]byte{
				[]byte(`{ "incompatible_field" : [ { "one" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" }, { "one" : 1 } ] }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_array_object",
			[][]byte{
				[]byte(`{ "incompatible_field" : [ { "one" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" } ] }`),
				[]byte(`{ "incompatible_field" : [ { "one" : 1 } ] }`),
			},
			ErrIncompatibleSchema,
		},
		{
			"incompatible_object",
			[][]byte{
				[]byte(`{ "incompatible_field" : { "one" : 1 } }`),
				[]byte(`{ "incompatible_field" : { "one" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" } }`),
			},
			ErrIncompatibleSchema,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var sch schema.Schema
			err := Infer(&sch, c.name, c.in, nil, nil, 0)
			assert.Equal(t, c.err, err)
		})
	}
}

func TestSchemaTags(t *testing.T) {
	cases := []struct {
		name         string
		in           [][]byte
		primaryKey   []string
		autoGenerate []string
		exp          *schema.Schema
	}{
		{
			"pk_autogenerate",
			[][]byte{
				[]byte(`{ "uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
"uuid_field1" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" }`),
			},
			[]string{"uuid_field"},
			[]string{"uuid_field1"},
			&schema.Schema{
				Name: "pk_autogenerate",
				Fields: map[string]*schema.Field{
					"uuid_field":  {Type: schema.NewStringType(), Format: "uuid"},
					"uuid_field1": {Type: schema.NewStringType(), Format: "uuid", AutoGenerate: true},
				},
				PrimaryKey: []string{"uuid_field"},
			},
		},
		{
			"pk_autogenerate_multi",
			[][]byte{
				[]byte(`{ "uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
"uuid_field1" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" }`),
			},
			[]string{"uuid_field", "uuid_field1"},
			[]string{"uuid_field1", "uuid_field"},
			&schema.Schema{
				Name: "pk_autogenerate_multi",
				Fields: map[string]*schema.Field{
					"uuid_field":  {Type: schema.NewStringType(), Format: "uuid", AutoGenerate: true},
					"uuid_field1": {Type: schema.NewStringType(), Format: "uuid", AutoGenerate: true},
				},
				PrimaryKey: []string{"uuid_field", "uuid_field1"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var sch schema.Schema
			err := Infer(&sch, c.name, c.in, c.primaryKey, c.autoGenerate, 0)
			require.NoError(t, err)
			assert.Equal(t, c.exp, &sch)
		})
	}
}
