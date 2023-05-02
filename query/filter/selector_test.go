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

package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/value"
)

func TestSelector(t *testing.T) {
	doc := []byte(`    {
      "a": 1,
      "tenant": "A",
      "createdAt": "2023-04-23T18:45:37Z",
      "c": {
        "a": 10,
        "b": "nested object under c",
        "c": {
          "a": "foo",
          "b": { "name": "this is free flow object"},
          "c": [{ "a": "car"}, { "a": "bike"}],
          "d": ["PARIS", "LONDON", "ENGLAND"]
        },
        "d": ["SANTA CLARA","SAN JOSE"],
        "e": [{ "a": "football"}, { "a": "basketball"}]
      },
      "d": {"agent": "free flow object top level"},
      "e": [{"random": "array of free flow object"}],
      "f": [{"a": "array of object with a field"}],
      "g": ["NEW YORK","MIAMI"],
      "h": 1.1
    }`)

	cases := []struct {
		parent   *schema.QueryableField
		field    *schema.QueryableField
		matcher  ValueMatcher
		expMatch bool
	}{
		{
			nil,
			&schema.QueryableField{FieldName: "c.d", DataType: schema.ArrayType},
			NewEqualityMatcher(value.NewStringValue("SAN JOSE", nil)),
			true,
		},
		{
			nil,
			&schema.QueryableField{FieldName: "c.c.d", DataType: schema.ArrayType},
			NewEqualityMatcher(value.NewStringValue("LONDON", nil)),
			true,
		},
		{
			nil,
			&schema.QueryableField{FieldName: "c.c.d", DataType: schema.ArrayType},
			NewEqualityMatcher(value.NewStringValue("ONDON", nil)),
			false,
		},
		{
			nil,
			&schema.QueryableField{FieldName: "g", DataType: schema.ArrayType},
			NewEqualityMatcher(value.NewStringValue("MIAMI", nil)),
			true,
		},
		{
			&schema.QueryableField{FieldName: "c.c.b", DataType: schema.ObjectType},
			&schema.QueryableField{FieldName: "c.c.b.name", DataType: schema.StringType, UnFlattenName: "name"},
			NewEqualityMatcher(value.NewStringValue("this is free flow object", nil)),
			true,
		},
		{
			&schema.QueryableField{FieldName: "d", DataType: schema.ObjectType},
			&schema.QueryableField{FieldName: "d.agent", DataType: schema.StringType, UnFlattenName: "agent"},
			NewEqualityMatcher(value.NewStringValue("free flow object top level", nil)),
			true,
		},
		{
			&schema.QueryableField{FieldName: "e", DataType: schema.ArrayType},
			&schema.QueryableField{FieldName: "e.random", DataType: schema.StringType, UnFlattenName: "random"},
			NewEqualityMatcher(value.NewStringValue("array of free flow object", nil)),
			true,
		},
		{
			&schema.QueryableField{FieldName: "f", DataType: schema.ArrayType},
			&schema.QueryableField{FieldName: "f.a", DataType: schema.StringType, UnFlattenName: "a"},
			NewEqualityMatcher(value.NewStringValue("array of object with a field", nil)),
			true,
		},
	}
	for _, c := range cases {
		s := NewSelector(c.parent, c.field, c.matcher, nil)
		require.Equal(t, c.expMatch, s.Matches(doc, nil))
	}
}
