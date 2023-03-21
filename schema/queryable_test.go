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
		{FieldName: "A", DataType: Int64Type},
		{FieldName: "B", DataType: StringType},
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
	}

	queryable := NewQueryableFieldsBuilder().BuildQueryableFields(fields, nil)
	expFields := []string{"A", "B", "C.E", "C.F", "C.G.H", "C.G.I", "D", "_tigris_created_at", "_tigris_updated_at"}
	for i, q := range queryable {
		require.Equal(t, expFields[i], q.FieldName)
	}
}
