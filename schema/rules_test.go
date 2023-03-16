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
		name     string
		existing []byte
		incoming []byte
		expErr   error
	}{
		{
			"field added",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}, "b": { "type": "string", "format": "byte"}},"primary_key": ["id"]}`),
			nil,
		},
		{
			"field removed",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}},"primary_key": ["id"]}`),
			ErrMissingField,
		},
		{
			"nested field removed",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "object", "properties" : { "nested1" : { "type" : "integer" }, "nested2" : { "type" : "integer" } }}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "object", "properties" : { "nested1" : { "type" : "integer" } }}},"primary_key": ["id"]}`),
			ErrMissingField,
		},
		{
			"nested field type change",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "object", "properties" : { "nested1" : { "type" : "integer" }, "nested2" : { "type" : "integer" } }}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "object", "properties" : { "nested1" : { "type" : "integer" }, "nested2" : { "type" : "string" } }}},"primary_key": ["id"]}`),
			errors.InvalidArgument("data type mismatch for field \"nested2\""),
		},
		{
			"primary key added",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}, "b": { "type": "string", "format": "byte"}},"primary_key": ["id", "s"]}`),
			errors.InvalidArgument("number of index fields changed"),
		},
		{
			"primary key order changed",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id", "s"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["s", "id"]}`),
			errors.InvalidArgument("index fields modified expected \"id\", found \"s\""),
		},
		{
			"type changed",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "format":"byte"}},"primary_key": ["id"]}`),
			errors.InvalidArgument("data type mismatch for field \"s\""),
		},
		{
			"primary key missing in existing collection, in update it is present, this shouldn't be an error if it is id",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			nil,
		},
		{
			"primary key missing in existing collection, in update it is present, but it is not id",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "id1": { "type": "integer"}, "s": { "type": "string"}},"primary_key": ["id1"]}`),
			errors.InvalidArgument("index fields modified expected \"id\", found \"id1\""),
		},
		{
			"reducing max length",
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "maxLength" : 100 }},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "integer"}, "s": { "type": "string", "maxLength" : 99 }},"primary_key": ["id"]}`),
			errors.InvalidArgument("reducing length of an existing field is not allowed \"s\""),
		},
	}

	config.DefaultConfig.Schema.AllowIncompatible = false

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f1, err := NewFactoryBuilder(true).Build("t1", c.existing)
			require.NoError(t, err)
			f2, err := NewFactoryBuilder(true).Build("t1", c.incoming)
			require.NoError(t, err)

			existingC, err := NewDefaultCollection(1, 1, f1, nil, nil)
			require.NoError(t, err)

			err = ApplyBackwardCompatibilityRules(existingC, f2)
			require.Equal(t, c.expErr, err)
		})
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
		f1, err := NewFactoryBuilder(true).Build("t1", c.existing)
		require.NoError(t, err)
		f2, err := NewFactoryBuilder(true).Build("t1", c.incoming)
		require.NoError(t, err)

		existingC, err := NewDefaultCollection(1, 1, f1, nil, nil)
		require.NoError(t, err)

		err = ApplyBackwardCompatibilityRules(existingC, f2)
		require.Equal(t, c.expErr, err)
	}
}

func TestApplySearchIndexSchemaRules(t *testing.T) {
	cases := []struct {
		existing []byte
		incoming []byte
		expErr   error
	}{
		{
			// field added
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "string"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "string"}, "s": { "type": "string"}, "b": { "type": "string"}}}`),
			nil,
		},
		{
			// field removed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "string"}, "s": { "type": "string"}},"primary_key": ["id"]}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "string"}},"primary_key": ["id"]}`),
			nil,
		},
		{
			// type changed
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "string"}, "s": { "type": "string"}}}`),
			[]byte(`{ "title": "t1", "properties": { "id": { "type": "string"}, "s": { "type": "string", "format":"date-time"}}}`),
			errors.InvalidArgument("data type mismatch for field 's'"),
		},
	}

	for _, c := range cases {
		f1, err := NewFactoryBuilder(true).BuildSearch("t1", c.existing)
		require.NoError(t, err)
		f2, err := NewFactoryBuilder(true).BuildSearch("t1", c.incoming)
		require.NoError(t, err)

		existingC := NewSearchIndex(1, "1.1.t1", f1, nil)

		err = ApplySearchIndexBackwardCompatibilityRules(existingC, f2)
		require.Equal(t, c.expErr, err)
	}
}

func TestAttributesOnFields(t *testing.T) {
	t.Run("test reserved fields", func(t *testing.T) {
		ptrTrue := true
		f := &Field{FieldName: "test", DataType: DoubleType, PrimaryKeyField: &ptrTrue}
		require.Equal(t, errors.InvalidArgument("unsupported primary key type detected 'double'"), ValidateFieldAttributes(false, f))
	})
	t.Run("test reserved fields", func(t *testing.T) {
		f := &Field{FieldName: "_tigris_created_at", DataType: DateTimeType}
		require.Error(t, ValidateFieldAttributes(false, f))
	})
	t.Run("test invalid field name pattern", func(t *testing.T) {
		invalidFieldNames := []string{"0id", "0ID", "("}
		for _, invalidFieldName := range invalidFieldNames {
			err := ValidateFieldAttributes(false, &Field{FieldName: invalidFieldName, DataType: StringType})
			require.Equal(t, err,
				errors.InvalidArgument(
					"Invalid collection field name, field name can only contain [a-zA-Z0-9_$] and it can only start"+
						" with [a-zA-Z_$] for fieldName = '%s'",
					invalidFieldName))
		}
	})
}

func TestSearchAttributesOnFields(t *testing.T) {
	cases := []struct {
		schema      []byte
		expErrorMsg string
	}{
		{
			[]byte(`{"title":"test","properties":{"id":{"type":"string","searchIndex":true,"facet":true,"sort":true}}}`),
			"",
		}, {
			[]byte(`{"title":"test","properties":{"name":{"type":"string","searchIndex":true,"facet":true,"sort":true}}}`),
			"",
		}, {
			[]byte(`{"title":"test","properties":{"name":{"type":"string","index":true,"facet":true,"sort":true}}}`),
			"Enable search index first to use faceting or sorting on field 'name'",
		}, {
			[]byte(`{"title":"test","properties":{"arr":{"type":"array","items":{"type":"string"},"searchIndex":true,"facet":true}}}`),
			"",
		}, {
			[]byte(`{"title":"test","properties":{"arr":{"type":"array","items":{"type":"string"},"searchIndex":true,"facet":true,"sort":true}}}`),
			"Cannot enable sorting on field 'arr' of type 'array'",
		}, {
			[]byte(`{"title":"test","properties":{"obj":{"type":"object","searchIndex":true,"facet":true,"sort":true}}}`),
			"Cannot have sort or facet attribute on an object 'obj'",
		}, {
			[]byte(`{"title":"test","properties":{"obj":{"type":"object","properties":{"id":{"type":"integer","searchIndex":true}}}}}`),
			"",
		}, {
			[]byte(`{"title":"test","properties":{"obj":{"type":"object","properties":{"id":{"type":"integer","searchIndex":true}, "nested": {"type":"object","properties":{"str":{"type":"string","searchIndex":true}}}}}}}`),
			"",
		}, {
			[]byte(`{"title":"test","properties":{"obj_fail":{"type":"object","properties":{"nested_arr":{"type":"array","items":{"type":"string"},"searchIndex":true,"facet":true},"nested_arr_obj":{"type":"array","items":{"type":"object","properties":{"n_id":{"type":"integer"}}}}}}}}`),
			"",
		}, {
			[]byte(`{"title":"test","properties":{"obj_last":{"type":"object","properties":{"nested_arr_obj":{"type":"array","items":{"type":"object","properties":{"n_id":{"type":"integer","searchIndex":true}}}}}}}}`),
			"Cannot enable index or search on an array of objects",
		},
	}
	for _, c := range cases {
		_, err := NewFactoryBuilder(true).Build("test", c.schema)
		if len(c.expErrorMsg) > 0 {
			require.Contains(t, err.Error(), c.expErrorMsg)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestIndexAttributeOnFields(t *testing.T) {
	cases := []struct {
		schema      []byte
		expErrorMsg string
	}{
		{
			[]byte(`{"title":"test","properties":{"id":{"type":"string","index":true}}}`),
			"",
		}, {
			[]byte(`{"title":"test","properties":{"name":{"type":"string","index":true}}}`),
			"",
		}, {
			[]byte(`{"title":"test","properties":{"arr":{"type":"array","items":{"type":"string"},"index":true}}}`),
			"Cannot enable index on field 'arr' of type 'array'",
		}, {
			[]byte(`{"title":"test","properties":{"obj":{"type":"object","index":true}}}`),
			"Cannot enable index on object 'obj' or object fields",
		}, {
			[]byte(`{"title":"test","properties":{"obj":{"type":"object","properties":{"nested_arr":{"type":"array","items":{"type":"string"},"index":true},"nested_arr_obj":{"type":"array","items":{"type":"object","properties":{"n_id":{"type":"integer"}}}}}}}}`),
			"Cannot enable index on nested field 'nested_arr'",
		}, {
			[]byte(`{"title":"test","properties":{"obj_last":{"type":"object","properties":{"nested_arr_obj":{"type":"array","items":{"type":"object","properties":{"n_id":{"type":"integer","index":true}}}}}}}}`),
			"Cannot enable index on nested field 'n_id'",
		},
	}
	for _, c := range cases {
		_, err := NewFactoryBuilder(true).Build("test", c.schema)
		if len(c.expErrorMsg) > 0 {
			require.Contains(t, err.Error(), c.expErrorMsg)
		} else {
			require.NoError(t, err)
		}
	}
}
