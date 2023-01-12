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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	ljson "github.com/tigrisdata/tigris/lib/json"
)

var delta = &VersionDelta{
	Version: 2,
	Fields: []*VersionDeltaField{
		{KeyPath: []string{"to_change_type_top_level1"}, From: StringType, To: DoubleType},
		{KeyPath: []string{"to_remove_top_level1"}, From: StringType, To: UnknownType},
		{KeyPath: []string{"field3", "to_change_type_in_object_1"}, From: StringType, To: DoubleType},
		{KeyPath: []string{"field3", "to_remove_in_object_1"}, From: StringType, To: UnknownType},
		{KeyPath: []string{"field3", "field6", "to_change_type_in_nested_obj1"}, From: StringType, To: DoubleType},
		{KeyPath: []string{"field3", "field6", "to_remove_in_nested_obj1"}, From: StringType, To: UnknownType},
		{KeyPath: []string{"field3", "to_remove_nested_object"}, From: ObjectType, To: UnknownType},
		{KeyPath: []string{"field3", "to_change_primitive_to_object_nested1"}, From: StringType, To: ObjectType},
		{KeyPath: []string{"field3", "to_change_object_to_primitive_nested1"}, From: ObjectType, To: StringType},
		{KeyPath: []string{"to_remove_top_level_array1"}, From: ArrayType, To: UnknownType},
		{KeyPath: []string{"to_change_top_level_array1", ""}, From: StringType, To: DoubleType},
		{KeyPath: []string{"to_change_complex_2d_array1", "", "", "to_remove_in_2d_array1"}, From: StringType, To: UnknownType},
		{KeyPath: []string{"to_change_complex_2d_array1", "", "", "to_change_in_2d_array1"}, From: StringType, To: DoubleType},
		{KeyPath: []string{"to_change_complex_2d_array1", "", "", "field6", "to_remove_field7"}, From: StringType, To: UnknownType},
		{KeyPath: []string{"to_change_complex_2d_array1", "", "", "field6", "to_change_field8"}, From: StringType, To: DoubleType},
		{KeyPath: []string{"to_change_primitive_2d_array1", "", ""}, From: StringType, To: DoubleType},
		{KeyPath: []string{"to_change_array_to_primitive_top_level1"}, From: ArrayType, To: DoubleType},
		{KeyPath: []string{"to_change_primitive_to_array_top_level1"}, From: StringType, To: ArrayType},
		{KeyPath: []string{"to_change_primitive_to_object_top_level1"}, From: StringType, To: ObjectType},
		{KeyPath: []string{"to_change_3d_array_from_primitive_to_object", "", "", ""}, From: DoubleType, To: ObjectType},
		{KeyPath: []string{"to_change_3d_array_from_object_to_primitive", "", "", ""}, From: ObjectType, To: DoubleType},
		{KeyPath: []string{"to_reduce_dimensions_array1", "", ""}, From: ArrayType, To: DoubleType},
		{KeyPath: []string{"to_increase_dimensions_array1", "", ""}, From: DoubleType, To: ArrayType},
		{KeyPath: []string{"to_reduce_max_length_str"}, From: StringType, To: StringType, MaxLength: 4},
		{KeyPath: []string{"to_reduce_max_length_bytes"}, From: ByteType, To: ByteType, MaxLength: 4},
		{KeyPath: []string{"to_change_type_and_add_max_length"}, From: StringType, To: ByteType, MaxLength: 4},
		{KeyPath: []string{"to_change_string_to_bytes_and_truncate"}, From: StringType, To: ByteType, MaxLength: 4},
		{KeyPath: []string{"to_change_bytes_to_string_and_truncate"}, From: ByteType, To: StringType, MaxLength: 4},
	},
}

func TestBuildSchemaDelta(t *testing.T) {
	cases := []struct {
		name   string
		first  string
		second string
		delta  *VersionDelta
	}{
		{
			name: "same_schema",
			first: `{
			"title": "t1", "primary_key": ["id"],
			"properties": {
				"id": { "type": "integer" },
				"field1": { "type": "string" }
			}
		}`,
			second: `{
			"title": "t1", "primary_key": ["id"],
			"properties": {
				"id": { "type": "integer" },
				"field1": { "type": "string" }
			}
		}`,
			delta: &VersionDelta{Version: 2},
		},
		{
			name: "compatible",
			first: `{
			"title": "t1", "primary_key": ["id"],
			"properties": {
				"id": { "type": "integer" },
				"field1": { "type": "string" }
			}
		}`,
			second: `{
			"title": "t1", "primary_key": ["id"],
			"properties": {
				"id": { "type": "integer" },
				"field1": { "type": "string" },
				"field2": { "type": "string" },
				"field3": { "type": "object", "properties" : { "field4": { "type": "string" } } }
			}
		}`,
			delta: &VersionDelta{Version: 2},
		},
		{
			name: "incompatible",
			first: `{
			"title": "t1", "primary_key": ["id"],
			"properties": {
				"id": { "type": "integer" },
				"to_change_type_top_level1": { "type": "string" }, 
				"to_remove_top_level1": { "type": "string" },
				"field3": {
					"type": "object",
					"properties" : {
						"to_change_type_in_object_1": { "type": "string" },
						"to_remove_in_object_1": { "type": "string" },
						"field6": {
							"type": "object",
							"properties" : {
								"to_change_type_in_nested_obj1": { "type": "string" },
								"to_remove_in_nested_obj1": { "type": "string" },
								"field1": { "type": "string" }
							}
						},
						"to_remove_nested_object": { "type": "object",
							"properties" : {
								"field7": { "type": "string" }
							}
						},
						"to_change_primitive_to_object_nested1": { "type": "string" },
						"to_change_object_to_primitive_nested1": { "type": "object",
							"properties" : {
								"field7": { "type": "string" }
							}
						}
					}
				},
				"to_remove_top_level_array1": {
					"type": "array",
					"items" : {
						"type" : "string"
					}
				},
				"to_change_top_level_array1": {
					"type": "array",
					"items" : {
						"type" : "string"
					}
				},
				"to_change_complex_2d_array1": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "object",
							"properties" : {
								"to_remove_in_2d_array1": { "type": "string" },
								"to_change_in_2d_array1": { "type": "string" },
								"field6": {
									"type": "object",
									"properties" : {
										"to_remove_field7": { "type": "string" },
										"to_change_field8": { "type": "string" },
										"field1": { "type": "string" }
									}
								}
							}
						}
					}
				},
				"to_change_primitive_2d_array1": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "string"
						}
					}
				},
				"to_change_array_to_primitive_top_level1": {
					"type": "array",
					"items" : {
						"type" : "string"
					}
				},
				"to_change_primitive_to_array_top_level1": { "type": "string" },
				"to_change_primitive_to_object_top_level1": { "type": "string" },
				"to_change_3d_array_from_primitive_to_object": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "array",
							"items" : {
								"type" : "number"
							}
						}
					}
				},
				"to_change_3d_array_from_object_to_primitive": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "array",
							"items" : {
								"type": "object",
								"properties" : {
									"field1": { "type": "string" }
								}
							}
						}
					}
				},
				"to_reduce_dimensions_array1": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "array",
							"items" : {
								"type" : "number"
							}
						}
					}
				},
				"to_increase_dimensions_array1": {
					"type" : "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "number"
						}
					}
				},
				"to_reduce_max_length_str": { "type": "string", "maxLength" : 5 },
				"to_reduce_max_length_bytes": { "type": "string", "format" : "byte", "maxLength" : 5 },
				"to_change_type_and_add_max_length": { "type": "string" },
				"to_change_string_to_bytes_and_truncate": { "type": "string", "maxLength" : 5 },
				"to_change_bytes_to_string_and_truncate": { "type": "string", "format" : "byte", "maxLength" : 5 }
			}
		}`,
			second: `{
			"title": "t1", "primary_key": ["id"],
			"properties": {
				"id": { "type": "integer" },
				"to_change_type_top_level1": { "type": "number" }, 
				"field3": {
					"type": "object",
					"properties" : {
						"to_change_type_in_object_1": { "type": "number" },
						"field6": {
							"type": "object",
							"properties" : {
								"to_change_type_in_nested_obj1": { "type": "number" },
								"field1": { "type": "string" }
							}
						},
						"to_change_primitive_to_object_nested1": { "type": "object",
							"properties" : {
								"field77": { "type": "string" }
							}
						},
						"to_change_object_to_primitive_nested1": { "type": "string" }
					}
				},
				"to_change_top_level_array1": {
					"type": "array",
					"items" : {
						"type" : "number"
					}
				},
				"to_change_complex_2d_array1": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "object",
							"properties" : {
								"to_change_in_2d_array1": { "type": "number" },
								"field6": { "type": "object",
									"properties" : {
										"to_change_field8": { "type": "number" },
										"field1": { "type": "string" }
									}
								}
							}
						}
					}
				},
				"to_change_primitive_2d_array1": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "number"
						}
					}
				},
				"to_change_array_to_primitive_top_level1": { "type": "number" },
				"to_change_primitive_to_array_top_level1": { 
					"type": "array",
					"items" : {
						"type" : "number"
					}
				},
				"to_change_primitive_to_object_top_level1": {
					"type": "object",
					"properties" : {
						"field_1": { "type": "number" }
					}
				},
				"to_change_3d_array_from_primitive_to_object": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "array",
							"items" : {
								"type": "object",
								"properties" : {
									"field1": { "type": "string" }
								}
							}
						}
					}
				},
				"to_change_3d_array_from_object_to_primitive": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "array",
							"items" : {
								"type" : "number"
							}
						}
					}
				},
				"to_reduce_dimensions_array1": {
					"type" : "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "number"
						}
					}
				},
				"to_increase_dimensions_array1": {
					"type": "array",
					"items" : {
						"type" : "array",
						"items" : {
							"type" : "array",
							"items" : {
								"type" : "number"
							}
						}
					}
				},
				"to_reduce_max_length_str": { "type": "string", "maxLength" : 4 },
				"to_reduce_max_length_bytes": { "type": "string", "format" : "byte", "maxLength" : 4 },
				"to_change_type_and_add_max_length": { "type": "string", "format" : "byte", "maxLength": 4 },
				"to_change_string_to_bytes_and_truncate": { "type": "string", "format" : "byte", "maxLength" : 4 },
				"to_change_bytes_to_string_and_truncate": { "type": "string", "maxLength" : 4 }
			}
		}`,
			delta: delta,
		},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			d, err := buildSchemaDelta(Version{1, []byte(v.first)}, Version{2, []byte(v.second)})
			require.NoError(t, err)
			for k, f := range v.delta.Fields {
				require.Equal(t, f, d.Fields[k])
			}
			require.Equal(t, len(v.delta.Fields), len(d.Fields))
		})
	}
}

func TestApplySchemaDeltas(t *testing.T) {
	cases := []struct {
		name   string
		before string
		after  string
		deltas []*VersionDelta
	}{
		{
			name:   "incompatible",
			deltas: []*VersionDelta{delta},
			before: `{
				"id": 1,
				"to_change_type_top_level1": "1.1", 
				"to_remove_top_level1": "1.01",
				"field3": {
					"to_change_type_in_object_1": "1.2",
					"to_remove_in_object_1": "1.02",
					"field6": {
						"to_change_type_in_nested_obj1": "1.3",
						"to_remove_in_nested_obj1": "1.03",
						"field1": "val_field1"
					},
					"to_remove_nested_object": { "field7": "val_field7" },
					"to_change_primitive_to_object_nested1": "val_111",
					"to_change_object_to_primitive_nested1": {
						"field7": "val_field7"
					}
				},
				"to_remove_top_level_array1": ["1", "2"],
				"to_change_top_level_array1": ["3", "4"],
				"to_change_complex_2d_array1":
					[
					  [ {
							"to_remove_in_2d_array1": "2.1",
							"to_change_in_2d_array1": "2.2",
							"field6": { "to_remove_field7": "2.3", "to_change_field8": "2.4", "field1": "2.5" }
						},
					 	{
							"to_remove_in_2d_array1": "3.1",
							"to_change_in_2d_array1": "3.2",
							"field6": { "to_remove_field7": "4.3", "to_change_field8": "4.4", "field1": "4.5" }
						} ],
					  [ {
							"to_remove_in_2d_array1": "5.1",
							"to_change_in_2d_array1": "5.2",
							"field6": { "to_remove_field7": "6.3", "to_change_field8": "6.4", "field1": "6.5" }
						},
					 	{
							"to_remove_in_2d_array1": "7.1",
							"to_change_in_2d_array1": "7.2",
							"field6": { "to_remove_field7": "8.3", "to_change_field8": "8.4", "field1": "8.5" }
						} ]
					],
				"to_change_primitive_2d_array1": [ ["9.1", "9.2"], ["10.1", "10.2"] ],
				"to_change_array_to_primitive_top_level1": [ "11.1", "11.2" ],
				"to_change_primitive_to_array_top_level1": "12.1",
				"to_change_primitive_to_object_top_level1": "13.1",
				"to_reduce_dimensions_array1": [ [14.1, 14.2], [15.1, 15.2]],
				"to_change_3d_array_from_primitive_to_object": [ [ [ 25.1, 25.2 ], [ 26.1, 26.2 ] ], [ [ 27.1, 27.2 ], [ 28.1, 28.2 ] ] ],
				"to_change_3d_array_from_object_to_primitive":  [ [ [ { "field1" : "f1" }, { "field1" : "f2" } ], [{ "field1" : "f3" }, { "field1" : "f4" }] ], [ [{ "field1" : "f5" }, { "field1" : "f6" }], [{ "field1" : "f7" }, { "field1" : "f8" }] ] ],
				"to_increase_dimensions_array1": [ 16.1, 16.2 ],
				"to_reduce_max_length_str": "12345", 
				"to_reduce_max_length_bytes": "MTIzNDU=", 
				"to_change_type_and_add_max_length": "12345",
				"to_change_string_to_bytes_and_truncate": "12345",
				"to_change_bytes_to_string_and_truncate": "12345" 
			}`,

			after: `{
				"id": 1,
				"to_change_type_top_level1": 1.1, 
				"field3": {
					"to_change_type_in_object_1": 1.2,
					"field6": {
						"to_change_type_in_nested_obj1": 1.3,
						"field1": "val_field1"
					}
				},
				"to_change_top_level_array1": [3, 4],
				"to_change_complex_2d_array1":
					[
					  [ {
							"to_change_in_2d_array1": 2.2,
							"field6": { "to_change_field8": 2.4, "field1": "2.5" }
						},
					 	{
							"to_change_in_2d_array1": 3.2,
							"field6": { "to_change_field8": 4.4, "field1": "4.5" }
						} ],
					  [ {
							"to_change_in_2d_array1": 5.2,
							"field6": { "to_change_field8": 6.4, "field1": "6.5" }
						},
					 	{
							"to_change_in_2d_array1": 7.2,
							"field6": { "to_change_field8": 8.4, "field1": "8.5" }
						} ]
					],
				"to_change_primitive_2d_array1": [ [9.1, 9.2], [10.1, 10.2] ],
				"to_reduce_max_length_str": "1234",
				"to_reduce_max_length_bytes": "MTIzNA==",
				"to_change_type_and_add_max_length": "MTIzNA==",
				"to_change_string_to_bytes_and_truncate": "MTIzNA=="
			}`,
		},
		{
			name: "removed_then_added",
			deltas: []*VersionDelta{
				{
					Version: 2,
					Fields: []*VersionDeltaField{
						{KeyPath: []string{"field1"}, From: StringType, To: UnknownType},
					},
				},
				{
					Version: 3,
					Fields: []*VersionDeltaField{
						{KeyPath: []string{"field1"}, From: UnknownType, To: DoubleType},
					},
				},
			},
			before: `{"field1" : "3.4", "field2" : "8"}`,
			after:  `{"field2": "8"}`,
		},
		{
			name: "multiple_conversions",
			deltas: []*VersionDelta{
				{
					Version: 2,
					Fields: []*VersionDeltaField{
						{KeyPath: []string{"field1"}, From: StringType, To: DoubleType},
					},
				},
				{
					Version: 3,
					Fields: []*VersionDeltaField{
						{KeyPath: []string{"field1"}, From: DoubleType, To: StringType},
					},
				},
			},
			before: `{"field1" : "3.4", "field2" : "8"}`,
			after:  `{"field1" : "3.4", "field2" : "8"}`,
		},
		{
			name: "multiple_conversions_non_convertible",
			deltas: []*VersionDelta{
				{
					Version: 2,
					Fields: []*VersionDeltaField{
						{KeyPath: []string{"field1"}, From: StringType, To: BoolType},
					},
				},
				{
					Version: 3,
					Fields: []*VersionDeltaField{
						{KeyPath: []string{"field2"}, From: BoolType, To: StringType},
					},
				},
			},
			before: `{"field1" : "str1", "field2" : "8"}`,
			after:  `{"field2" : "8"}`,
		},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			doc, err := ljson.Decode([]byte(v.before))
			require.NoError(t, err)

			for _, dlt := range v.deltas {
				applySchemaDelta(doc, dlt)
			}

			b, err := ljson.Encode(doc)
			require.NoError(t, err)

			require.JSONEq(t, v.after, string(b))
		})
	}
}

func TestTypeConversionRules(t *testing.T) {
	t.Run("from_bool", func(t *testing.T) {
		cases := []struct {
			to   FieldType
			from interface{}
			exp  interface{}
		}{
			{UnknownType, true, nil},
			{NullType, true, nil},
			{BoolType, true, true},
			{Int32Type, true, 1},
			{Int64Type, true, 1},
			{DoubleType, true, 1},
			{StringType, true, "true"},
			{ByteType, true, []byte{0x1}},
			{UUIDType, true, nil},
			{DateTimeType, true, nil},
			{ArrayType, true, nil},
			{ObjectType, true, nil},

			{UnknownType, false, nil},
			{NullType, false, nil},
			{BoolType, false, false},
			{Int32Type, false, 0},
			{Int64Type, false, 0},
			{DoubleType, false, 0},
			{StringType, false, "false"},
			{ByteType, false, []byte{0x0}},
			{UUIDType, false, nil},
			{DateTimeType, false, nil},
			{ArrayType, false, nil},
			{ObjectType, false, nil},
		}

		for _, c := range cases {
			res := convertType(&VersionDeltaField{From: BoolType, To: c.to}, c.from)
			require.Equal(t, c.exp, res)
		}
	})

	t.Run("from_bytes", func(t *testing.T) {
		cases := []struct {
			to   FieldType
			from interface{}
			exp  interface{}
		}{
			{UnknownType, "YWFhYQ==", nil},
			{NullType, "YWFhYQ==", nil},
			{BoolType, "YWFhYQ==", nil},
			{BoolType, "AQ==", true},
			{BoolType, "AA==", false},
			{Int32Type, "YWFh", nil},
			{Int32Type, "AAAAEg==", int64(0x12)},
			{Int64Type, "YWFhYQ==", nil},
			{Int64Type, "BwAAAAAAABI=", int64(0x0700000000000012)},
			{DoubleType, "YWFhYQ==", nil},
			{DoubleType, "P/MzMzMzMzM=", 1.2},
			{StringType, "YWFhYQ==", "aaaa"},
			{ByteType, "YWFhYQ==", []byte("aaaa")},
			{UUIDType, "YWFhYQ==", nil},
			{UUIDType, "xtnfAasgSEaBSjNZz0LGaA==", "c6d9df01-ab20-4846-814a-3359cf42c668"},
			{DateTimeType, "YWFhYQ==", nil},
			{DateTimeType, "Fz36VmzwIp4=", "2023-01-26T21:51:25.55350083Z"},
			{ArrayType, "YWFhYQ==", nil},
			{ObjectType, "YWFhYQ==", nil},
		}

		//nolint:dupword
		/*
			Example how to generate base64 encoded binary values for: date-time and uuid
			u := uuid.New()
			dst := make([]byte, 32, 32)
			src := make([]byte, 8, 8) // = u[:]
			d := time.Now()
			binary.BigEndian.PutUint64(src, uint64(d.UnixNano()))
			base64.StdEncoding.Encode(dst, src)
			fmt.Printf("date-time bytes: %s source=%d %s\n", string(dst), d.UnixNano(), d.Format(time.RFC3339Nano))
			base64.StdEncoding.Encode(dst, u[:])
			fmt.Printf("uuid bytes: %s source=%s\n", string(dst), u.String())

			// float64
			dst := make([]byte, 32, 32)
			dd := []byte{0x3f, 0xf3, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33}
			base64.StdEncoding.Encode(dst, dd)
			fmt.Printf("%s\n", dst)
		*/

		for _, c := range cases {
			t.Run("to_"+FieldNames[c.to], func(t *testing.T) {
				res := convertType(&VersionDeltaField{From: ByteType, To: c.to}, c.from)
				require.Equal(t, c.exp, res)
			})
		}
	})

	t.Run("from_double", func(t *testing.T) {
		cases := []struct {
			to   FieldType
			from interface{}
			exp  interface{}
		}{
			{UnknownType, json.Number("1.2"), nil},
			{NullType, json.Number("1.2"), nil},
			{BoolType, json.Number("1.2"), true},
			{Int32Type, json.Number("1.2"), int64(1)},
			{Int64Type, json.Number("1.2"), int64(1)},
			{DoubleType, json.Number("1.2"), 1.2},
			{StringType, json.Number("1.2"), "1.2"},
			{ByteType, json.Number("1.2"), []byte{0x3f, 0xf3, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33}},
			{UUIDType, json.Number("1.2"), nil},
			{DateTimeType, json.Number("1.2"), nil},
			{ArrayType, json.Number("1.2"), nil},
			{ObjectType, json.Number("1.2"), nil},

			{UnknownType, 1.2, nil},
			{NullType, 1.2, nil},
			{BoolType, 1.2, true},
			{Int32Type, 1.2, int64(1)},
			{Int64Type, 1.2, int64(1)},
			{DoubleType, 1.2, 1.2},
			{StringType, 1.2, "1.2"},
			{ByteType, 1.2, []byte{0x3f, 0xf3, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33}},
			{UUIDType, 1.2, nil},
			{DateTimeType, 1.2, nil},
			{ArrayType, 1.2, nil},
			{ObjectType, 1.2, nil},
		}

		for _, c := range cases {
			t.Run("to_"+FieldNames[c.to], func(t *testing.T) {
				res := convertType(&VersionDeltaField{From: DoubleType, To: c.to}, c.from)
				require.Equal(t, c.exp, res)
			})
		}
	})

	t.Run("from_string", func(t *testing.T) {
		cases := []struct {
			to   FieldType
			from interface{}
			exp  interface{}
		}{
			{UnknownType, "str1", nil},
			{NullType, "str1", nil},
			{BoolType, "true", true},
			{Int32Type, "123", int64(123)},
			{Int64Type, "567", int64(567)},
			{DoubleType, "1.2", 1.2},
			{StringType, "str1", "str1"},
			{ByteType, "str1", []byte("str1")},
			{UUIDType, "9e896593-0065-4416-8e6e-b92400fd1246", "9e896593-0065-4416-8e6e-b92400fd1246"},
			{UUIDType, "non-uuid-in-str", nil},
			{DateTimeType, "2023-01-24T16:37:29.463235689-08:00", "2023-01-24T16:37:29.463235689-08:00"},
			{ArrayType, "str1", nil},
			{ObjectType, "str1", nil},
		}
		for _, c := range cases {
			t.Run("to_"+FieldNames[c.to], func(t *testing.T) {
				res := convertType(&VersionDeltaField{From: StringType, To: c.to}, c.from)
				require.Equal(t, c.exp, res)
			})
		}
	})

	t.Run("from_int64", func(t *testing.T) {
		cases := []struct {
			to   FieldType
			from interface{}
			exp  interface{}
		}{
			{UnknownType, json.Number("123"), nil},
			{NullType, json.Number("123"), nil},
			{BoolType, json.Number("123"), true},
			{Int32Type, json.Number("123"), int64(123)},
			{Int64Type, json.Number("123"), int64(123)},
			{DoubleType, json.Number("123"), 123.0},
			{StringType, json.Number("123"), "123"},
			{ByteType, json.Number("123"), []byte{0, 0, 0, 0, 0, 0, 0, 0x7b}},
			{UUIDType, json.Number("123"), nil},
			{DateTimeType, json.Number("123"), nil},
			{ArrayType, json.Number("123"), nil},
			{ObjectType, json.Number("123"), nil},

			{UnknownType, int64(123), nil},
			{NullType, int64(123), nil},
			{BoolType, int64(123), true},
			{Int32Type, int64(123), int64(123)},
			{Int64Type, int64(123), int64(123)},
			{DoubleType, int64(123), 123.0},
			{StringType, int64(123), "123"},
			{ByteType, int64(123), []byte{0, 0, 0, 0, 0, 0, 0, 0x7b}},
			{UUIDType, int64(123), nil},
			{DateTimeType, int64(123), nil},
			{ArrayType, int64(123), nil},
			{ObjectType, int64(123), nil},
		}

		for _, c := range cases {
			t.Run("to_"+FieldNames[c.to], func(t *testing.T) {
				res := convertType(&VersionDeltaField{From: Int64Type, To: c.to}, c.from)
				require.Equal(t, c.exp, res)
			})
		}
	})
}
