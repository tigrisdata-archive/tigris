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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
)

var boolTrue = true

func TestFieldBuilder_Build(t *testing.T) {
	t.Run("test convert json to internal types", func(t *testing.T) {
		require.Equal(t, Int64Type, ToFieldType("integer", "", ""))
		require.Equal(t, StringType, ToFieldType("string", "", ""))
		require.Equal(t, ByteType, ToFieldType("string", jsonSpecEncodingB64, ""))
		require.Equal(t, UUIDType, ToFieldType("string", "", jsonSpecFormatUUID))
		require.Equal(t, DateTimeType, ToFieldType("string", "", jsonSpecFormatDateTime))
		require.Equal(t, UnknownType, ToFieldType("string", "random", ""))
	})
	t.Run("test supported types", func(t *testing.T) {
		cases := []struct {
			builder  *FieldBuilder
			expError error
		}{
			{
				builder:  &FieldBuilder{FieldName: "test", Type: "boolean"},
				expError: nil,
			},
			{
				builder:  &FieldBuilder{FieldName: "test", Type: "bool"},
				expError: errors.InvalidArgument("unsupported type detected 'bool'"),
			},
			{
				builder:  &FieldBuilder{FieldName: "test", Type: "string", Format: "uuid"},
				expError: nil,
			},
			{
				builder:  &FieldBuilder{FieldName: "test", Type: "number"},
				expError: nil,
			},
			{
				builder:  &FieldBuilder{FieldName: "test", Type: "number", Primary: &boolTrue},
				expError: errors.InvalidArgument("unsupported primary key type detected 'number'"),
			},
			{
				builder:  &FieldBuilder{FieldName: "test", Type: "integer", Primary: &boolTrue},
				expError: nil,
			},
			{
				builder:  &FieldBuilder{FieldName: "_test", Type: "integer"},
				expError: nil,
			},
		}
		for _, c := range cases {
			_, err := c.builder.Build(false)
			require.Equal(t, c.expError, err)
		}
	})
	t.Run("test supported properties", func(t *testing.T) {
		cases := []struct {
			propertySchema []byte
			expError       error
		}{
			{
				[]byte(`{"type": "boolean"}`),
				nil,
			},
			{
				[]byte(`{"unique": true}`),
				errors.InvalidArgument("unsupported property found 'unique'"),
			},
			{
				[]byte(`{"max_length": 100}`),
				errors.InvalidArgument("unsupported property found 'max_length'"),
			},
			{
				[]byte(`{"maxLength": 100}`),
				nil,
			},
			{
				[]byte(`{"sort": true}`),
				nil,
			},
		}
		for _, c := range cases {
			var f FieldBuilder
			if c.expError != nil {
				require.Equal(t, c.expError, f.Validate(c.propertySchema))
			} else {
				require.NoError(t, f.Validate(c.propertySchema))
			}
		}
	})
	t.Run("test reserved fields", func(t *testing.T) {
		builder := &FieldBuilder{FieldName: "created_at", Type: "date-time"}
		_, err := builder.Build(false)
		require.Error(t, err)
	})

	t.Run("test invalid field name pattern", func(t *testing.T) {
		invalidFieldNames := []string{"0id", "0ID", "("}
		for _, invalidFieldName := range invalidFieldNames {
			_, err := (&FieldBuilder{FieldName: invalidFieldName, Type: "string"}).Build(false) // one time builder, thrown away after test concluded
			require.Equal(t, err,
				errors.InvalidArgument(
					"Invalid collection field name, field name can only contain [a-zA-Z0-9_$] and it can only start"+
						" with [a-zA-Z_$] for fieldName = '%s'",
					invalidFieldName))
		}
	})

	t.Run("test valid field name pattern", func(t *testing.T) {
		validFieldNames := []string{"a1", "$a1", "$_a1", "$_", "A1", "Z1"}
		for _, validFieldName := range validFieldNames {
			_, err := (&FieldBuilder{FieldName: validFieldName, Type: "string"}).Build(false) // one time builder, thrown away after test concluded
			require.NoError(t, err)
		}
	})
}

func TestIndexableFieldsAreChecked(t *testing.T) {
	cases := []struct {
		schema []byte
		expErr error
	}{
		{
			// cannot set a byte field as indexable
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true}, "b": {"type": "string", "format":"byte", "index": true}}}`),
			errors.InvalidArgument("'b' has been configured to be indexed but it is not a supported indexable type. Only top level non-byte fields can be indexed."),
		},
		{
			// cannot index an array
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true}, "arr": {"type": "array", "items":{"type": "string"}, "index": true}}}`),
			errors.InvalidArgument("'arr' has been configured to be indexed but it is not a supported indexable type. Only top level non-byte fields can be indexed."),
		},
		{
			// cannot index an object
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true}, "obj": {"type": "object", "index": true, "properties":{"name": {"type": "string"}}}}}`),
			errors.InvalidArgument("'obj' has been configured to be indexed but it is not a supported indexable type. Only top level non-byte fields can be indexed."),
		},
		{
			// cannot index a subfield on an object
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true}, "obj": {"type": "object", "properties":{"name": {"type": "string", "index": true}}}}}`),
			errors.InvalidArgument("Cannot index nested field 'name'"),
		},
		{
			// cannot index a subfield on an array
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true},"arr": {"type": "array", "items":{"type": "string", "index": true}}}}`),
			errors.InvalidArgument("Cannot index nested field in array"),
		},
	}

	for _, c := range cases {
		_, err := Build("t1", c.schema)
		require.Equal(t, c.expErr, err)
	}
}

func TestQueryableField_ShouldPack(t *testing.T) {
	// reserved fields should never be packed
	for _, f := range ReservedFields {
		t.Run(fmt.Sprintf("%s is reserved and should not be packed", f), func(t *testing.T) {
			q := &QueryableField{FieldName: f}
			require.False(t, q.ShouldPack())
		})
	}

	for _, f := range [...]FieldType{ArrayType, DateTimeType} {
		t.Run(fmt.Sprintf("%s should be packed", FieldNames[f]), func(t *testing.T) {
			q := &QueryableField{FieldName: "myField", DataType: f}
			require.True(t, q.ShouldPack())
		})
	}

	shouldNotPack := [...]FieldType{
		UnknownType,
		NullType,
		BoolType,
		Int32Type,
		Int64Type,
		DoubleType,
		StringType,
		ByteType,
		ObjectType,
	}
	for _, f := range shouldNotPack {
		t.Run(fmt.Sprintf("%s should not be packed", FieldNames[f]), func(t *testing.T) {
			q := &QueryableField{FieldName: "myField", DataType: f}
			require.False(t, q.ShouldPack())
		})
	}
}

func TestQueryableField_IsReserved(t *testing.T) {
	// this should reflect schema.Reserved fields array
	for _, f := range ReservedFields {
		t.Run(fmt.Sprintf("%s is reserved", f), func(t *testing.T) {
			q := &QueryableField{FieldName: f}
			require.True(t, q.IsReserved())
		})
	}
}
