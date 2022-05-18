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

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

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
				expError: api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported type detected 'bool'"),
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
				expError: api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported primary key type detected 'number'"),
			},
			{
				builder:  &FieldBuilder{FieldName: "test", Type: "integer", Primary: &boolTrue},
				expError: nil,
			},
		}
		for _, c := range cases {
			_, err := c.builder.Build()
			require.Equal(t, c.expError, err)
		}
	})
	t.Run("test_supported properties", func(t *testing.T) {
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
				api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported property found 'unique'"),
			},
			{
				[]byte(`{"max_length": 100}`),
				api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported property found 'max_length'"),
			},
			{
				[]byte(`{"maxLength": 100}`),
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
}
