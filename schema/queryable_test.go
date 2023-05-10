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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildQueryableFields(t *testing.T) {
	fields := []*Field{
		{FieldName: "A1", DataType: Int32Type},
		{FieldName: "A2", DataType: Int64Type},
		{FieldName: "A3", DataType: DoubleType},
		{FieldName: "A4", DataType: BoolType},
		{FieldName: "B1", DataType: StringType},
		{FieldName: "B2", DataType: UUIDType},
		{FieldName: "B3", DataType: DateTimeType},
		{FieldName: "B4", DataType: ByteType},
		{FieldName: "C", DataType: ObjectType, Fields: []*Field{
			{FieldName: "E", DataType: Int64Type},
			{FieldName: "F", DataType: StringType},
			{
				FieldName: "G", DataType: ObjectType, Fields: []*Field{
					{FieldName: "H", DataType: StringType},
					{FieldName: "I", DataType: ObjectType},
				},
			},
		}},
		{FieldName: "D", DataType: ObjectType},
		{FieldName: "E", DataType: ArrayType},
		{FieldName: "F", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: Int32Type}}},
		{FieldName: "F1", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: Int64Type}}},
		{FieldName: "F2", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: DoubleType}}},
		{FieldName: "F3", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: BoolType}}},
		{FieldName: "F4", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: StringType}}},
		{FieldName: "F5", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: UUIDType}}},
		{FieldName: "F6", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: DateTimeType}}},
		{FieldName: "F7", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: ArrayType}}},
		{FieldName: "F8", DataType: ArrayType, Fields: []*Field{{FieldName: "A", DataType: ObjectType}}},
	}

	queryable := NewQueryableFieldsBuilder().BuildQueryableFields(fields, nil, true)
	expTypes := []string{"int32", "int64", "float", "bool", "string", "string", "int64", "string", "int64", "string", "string", "object", "object", "string", "int32[]", "int64[]", "float[]", "bool[]", "string[]", "string[]", "string[]", "string", "object[]", "int64", "int64"}
	expFields := []string{"A1", "A2", "A3", "A4", "B1", "B2", "B3", "B4", "C.E", "C.F", "C.G.H", "C.G.I", "D", "E", "F", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "_tigris_created_at", "_tigris_updated_at"}
	for i, q := range queryable {
		require.Equal(t, expFields[i], q.FieldName)
		require.Equal(t, expTypes[i], q.SearchType)
	}
}

func TestIncludeMetadata(t *testing.T) {
	fields := []*Field{
		{FieldName: "A1", DataType: Int32Type},
		{FieldName: "A2", DataType: Int64Type},
	}

	queryable := NewQueryableFieldsBuilder().BuildQueryableFields(fields, nil, true)
	expTypes := []string{"int32", "int64", "int64", "int64"}
	expFields := []string{"A1", "A2", "_tigris_created_at", "_tigris_updated_at"}
	for i, q := range queryable {
		require.Equal(t, expFields[i], q.FieldName)
		require.Equal(t, expTypes[i], q.SearchType)
	}

	queryable = NewQueryableFieldsBuilder().BuildQueryableFields(fields, nil, false)
	expTypes = []string{"int32", "int64"}
	expFields = []string{"A1", "A2"}
	for i, q := range queryable {
		require.Equal(t, expFields[i], q.FieldName)
		require.Equal(t, expTypes[i], q.SearchType)
	}
}
