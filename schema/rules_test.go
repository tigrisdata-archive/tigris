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
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
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
		},
		{
			// field removed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}},"primary_key": ["id"]}`),
			ErrMissingField,
		},
		{
			// primary key added
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}, "b": { "type": "string", "format": "byte"}},"primary_key": ["id", "s"]}`),
			errors.InvalidArgument("number of index fields changed"),
		},
		{
			// primary key order changed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id", "s"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["s", "id"]}`),
			errors.InvalidArgument("index fields modified expected \"id\", found \"s\""),
		},
		{
			// type changed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "format":"byte"}},"primary_key": ["id"]}`),
			errors.InvalidArgument("data type mismatch for field \"s\""),
		},
		{
			// primary key missing in existing collection, in update it is present, this shouldn't be an error if it is id
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			nil,
		},
		{
			// primary key missing in existing collection, in update it is present, but it is not id
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "id1": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id1"]}`),
			errors.InvalidArgument("index fields modified expected \"id\", found \"id1\""),
		},
		{
			// reducing max length
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "maxLength" : 100 }},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "maxLength" : 99 }},"primary_key": ["id"]}`),
			errors.InvalidArgument("reducing length of an existing field is not allowed \"s\""),
		},
	}

	config.DefaultConfig.Schema.AllowIncompatible = false

	for _, c := range cases {
		f1, err := Build("t1", c.existing)
		require.NoError(t, err)
		f2, err := Build("t1", c.incoming)
		require.NoError(t, err)

		existingC, err := NewDefaultCollection(1, 1, f1, nil, nil)
		require.NoError(t, err)

		err = ApplySchemaRules(existingC, f2)
		require.Equal(t, c.expErr, err)
	}
}

func TestApplySchemaRulesIncompatible(t *testing.T) {
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
		},
		{
			// field removed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}},"primary_key": ["id"]}`),
			nil,
		},
		{
			// primary key added
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}, "b": { "type": "string", "format": "byte"}},"primary_key": ["id", "s"]}`),
			errors.InvalidArgument("number of index fields changed"),
		},
		{
			// primary key order changed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id", "s"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["s", "id"]}`),
			errors.InvalidArgument("index fields modified expected \"id\", found \"s\""),
		},
		{
			// type changed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "format":"byte"}},"primary_key": ["id"]}`),
			nil,
		},
		{
			// primary key missing in existing collection, in update it is present, this shouldn't be an error if it is id
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			nil,
		},
		{
			// primary key missing in existing collection, in update it is present, but it is not id
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "id1": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id1"]}`),
			errors.InvalidArgument("index fields modified expected \"id\", found \"id1\""),
		},
	}

	config.DefaultConfig.Schema.AllowIncompatible = true

	for _, c := range cases {
		f1, err := Build("t1", c.existing)
		require.NoError(t, err)
		f2, err := Build("t1", c.incoming)
		require.NoError(t, err)

		existingC, err := NewDefaultCollection(1, 1, f1, nil, nil)
		require.NoError(t, err)

		err = ApplySchemaRules(existingC, f2)
		require.Equal(t, c.expErr, err)
	}
}

func TestApplyIndexSchemaRules(t *testing.T) {
	cases := []struct {
		existing []byte
		incoming []byte
		expErr   error
	}{
		{
			// field added
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}, "b": { "type": "string", "format": "byte"}}}`),
			nil,
		},
		{
			// field removed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}},"primary_key": ["id"]}`),
			nil,
		},
		{
			// type changed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "format":"byte"}}}`),
			errors.InvalidArgument("data type mismatch for field 's'"),
		},
		{
			// source changed
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}, "source": {"type": "external"}}`),
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}, "source": {"type": "tigris", "collection": "foo"}}`),
			errors.InvalidArgument("changing index source type is not allowed from: 'external', to: 'tigris'"),
		},
		{
			// collection changed
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}, "source": {"type": "tigris", "collection": "foo"}}`),
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}, "source": {"type": "tigris", "collection": "bar"}}`),
			errors.InvalidArgument("changing index source collection is not allowed from: 'foo', to: 'bar'"),
		},
		{
			// branch changed
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}, "source": {"type": "tigris", "collection": "foo", "branch": "featA"}}`),
			[]byte(`{"title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}, "source": {"type": "tigris", "collection": "foo", "branch": "featB"}}`),
			errors.InvalidArgument("changing index source database branch is not allowed from: 'featA', to: 'featB'"),
		},
	}

	for _, c := range cases {
		f1, err := BuildSearch("t1", c.existing)
		require.NoError(t, err)
		f2, err := BuildSearch("t1", c.incoming)
		require.NoError(t, err)

		existingC := NewSearchIndex(1, "1.1.t1", f1, nil)

		err = ApplySearchIndexSchemaRules(existingC, f2)
		require.Equal(t, c.expErr, err)
	}
}
