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

package search

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/schema"
)

func TestSearchBuilder(t *testing.T) {
	js := []byte(`{"a": 4, "$and": [{"int_value":1}, {"string_value1": "shoe"}]}`)
	f := filter.NewFactory([]*schema.QueryableField{
		schema.NewQueryableField("a", schema.Int64Type, schema.UnknownType, nil),
		schema.NewQueryableField("int_value", schema.Int64Type, schema.UnknownType, nil),
		schema.NewQueryableField("string_value1", schema.StringType, schema.UnknownType, nil),
	}, nil)
	wrappedF, err := f.WrappedFilter(js)
	require.NoError(t, err)

	b := NewBuilder()
	q := b.Filter(wrappedF).Query("test").Build()
	require.Equal(t, []string{"a:=4&&int_value:=1&&string_value1:=shoe"}, q.ToSearchFilter())
	require.Equal(t, "test", q.Q)
}

func TestQuery_ToSortFields(t *testing.T) {
	t.Run("with nil sort order", func(t *testing.T) {
		q := NewBuilder().SortOrder(nil).Build()
		sortBy := q.ToSortFields()
		assert.NotNil(t, sortBy)
		assert.Empty(t, sortBy)
	})

	t.Run("with empty sort order", func(t *testing.T) {
		q := NewBuilder().SortOrder(&sort.Ordering{}).Build()
		sortBy := q.ToSortFields()
		assert.NotNil(t, sortBy)
		assert.Empty(t, sortBy)
	})

	t.Run("with 1 sort order", func(t *testing.T) {
		ordering := &sort.Ordering{
			{Name: "field_1", Ascending: true, MissingValuesFirst: false},
		}
		q := NewBuilder().SortOrder(ordering).Build()
		sortBy := q.ToSortFields()
		assert.Equal(t, "field_1(missing_values: last):asc", sortBy)
	})

	t.Run("with 3 sort orders", func(t *testing.T) {
		ordering := &sort.Ordering{
			{Name: "field_1", Ascending: true},
			{Name: "parent.field_2"},
			{Name: "first.second.field_3", Ascending: true, MissingValuesFirst: true},
		}

		expected := `field_1(missing_values: last):asc,parent.field_2(missing_values: last):desc,first.second.field_3(missing_values: first):asc`

		q := NewBuilder().SortOrder(ordering).Build()
		sortBy := q.ToSortFields()
		assert.Equal(t, expected, sortBy)
	})
}
