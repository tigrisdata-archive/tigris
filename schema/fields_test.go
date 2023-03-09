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
				builder:  &FieldBuilder{FieldName: "test", Type: "integer", Primary: &boolTrue},
				expError: nil,
			},
			{
				builder:  &FieldBuilder{FieldName: "_test", Type: "integer"},
				expError: nil,
			},
		}
		for _, c := range cases {
			require.Equal(t, c.expError, ValidateFieldBuilder(*c.builder))
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
			if c.expError != nil {
				require.Equal(t, c.expError, ValidateSupportedProperties(c.propertySchema))
			} else {
				require.NoError(t, ValidateSupportedProperties(c.propertySchema))
			}
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
			errors.InvalidArgument("Cannot enable index on field 'b' of type 'byte'. Only top level non-byte fields can be indexed."),
		},
		{
			// cannot index an array
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true}, "arr": {"type": "array", "items":{"type": "string"}, "index": true}}}`),
			errors.InvalidArgument("Cannot enable index on field 'arr' of type 'array'. Only top level non-byte fields can be indexed."),
		},
		{
			// cannot index an object
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true}, "obj": {"type": "object", "index": true, "properties":{"name": {"type": "string"}}}}}`),
			errors.InvalidArgument("Cannot enable index on object 'obj' or object fields"),
		},
		{
			// cannot index a subfield on an object
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true}, "obj_fail": {"type": "object", "properties":{"name": {"type": "string", "index": true}}}}}`),
			errors.InvalidArgument("Cannot enable index on nested field 'name'"),
		},
		{
			// cannot index a subfield on an array
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "index": true},"arr": {"type": "array", "items":{"type": "string"}, "index": true}}}`),
			errors.InvalidArgument("Cannot enable index on field 'arr' of type 'array'. Only top level non-byte fields can be indexed."),
		},
	}

	for _, c := range cases {
		_, err := NewFactoryBuilder(true).Build("t1", c.schema)
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
