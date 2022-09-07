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

func TestApplySchemaRules(t *testing.T) {
	cases := []struct {
		existing []byte
		incoming []byte
		expErr   error
	}{
		{
			// field added
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}, "b": { "type": "string", "format": "byte"}},"primary_key": ["id"]}`),
			nil,
		}, {
			// field removed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}},"primary_key": ["id"]}`),
			ErrMissingField,
		}, {
			// primary key added
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}, "b": { "type": "string", "format": "byte"}},"primary_key": ["id", "s"]}`),
			api.Errorf(api.Code_INVALID_ARGUMENT, "number of index fields changed"),
		}, {
			// primary key order changed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id", "s"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["s", "id"]}`),
			api.Errorf(api.Code_INVALID_ARGUMENT, "index fields modified expected \"id\", found \"s\""),
		}, {
			// type changed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "format":"byte"}},"primary_key": ["id"]}`),
			api.Errorf(api.Code_INVALID_ARGUMENT, "data type mismatch for field \"s\""),
		}, {
			// primary key missing in existing collection, in update it is present, this shouldn't be an error if it is id
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			nil,
		}, {
			// primary key missing in existing collection, in update it is present, but it is not id
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "id1": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id1"]}`),
			api.Errorf(api.Code_INVALID_ARGUMENT, "index fields modified expected \"id\", found \"id1\""),
		},
	}
	for _, c := range cases {
		f1, err := Build("t1", c.existing)
		require.NoError(t, err)
		f2, err := Build("t1", c.incoming)
		require.NoError(t, err)

		existingC := NewDefaultCollection(f1.Name, 1, 1, f1.CollectionType, f1.Fields, f1.Indexes, f1.Schema, "f")
		err = ApplySchemaRules(existingC, f2)
		require.Equal(t, c.expErr, err)
	}
}
