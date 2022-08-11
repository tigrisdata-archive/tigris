package search

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshalSort(t *testing.T) {
	t.Run("Unmarshal 3 sort orders", func(t *testing.T) {
		rawInput := []byte(`[{"field_1":"$asc"},{"field_2":"$desc"},{"field_3":"$asc"}]`)
		sort, err := UnmarshalSort(rawInput)

		assert.NoError(t, err)
		assert.NotNil(t, sort)
		assert.Len(t, *sort, 3)

		expected := []SortField{
			{
				Name:               "field_1",
				Ascending:          true,
				MissingValuesFirst: false,
			},
			{
				Name:               "field_2",
				Ascending:          false,
				MissingValuesFirst: false,
			},
			{
				Name:               "field_3",
				Ascending:          true,
				MissingValuesFirst: false,
			},
		}
		// validate ordering, and values
		assert.Exactly(t, expected, *sort)
	})

	t.Run("with empty array", func(t *testing.T) {
		sort, err := UnmarshalSort([]byte(`[]`))
		assert.NoError(t, err)
		assert.NotNil(t, sort)
		assert.Empty(t, sort)
	})

	t.Run("with nil input", func(t *testing.T) {
		sort, err := UnmarshalSort(nil)
		assert.NoError(t, err)
		assert.Nil(t, sort)
	})

	t.Run("with empty input", func(t *testing.T) {
		sort, err := UnmarshalSort([]byte(``))
		assert.NoError(t, err)
		assert.Nil(t, sort)
	})

	t.Run("with 1 order", func(t *testing.T) {
		sort, err := UnmarshalSort([]byte(`[{"field_1":"$desc"}]`))
		assert.NoError(t, err)
		assert.NotNil(t, sort)
		assert.Len(t, *sort, 1)

		order := (*sort)[0]
		assert.Equal(t, order.Name, "field_1")
		assert.False(t, order.Ascending)
		assert.False(t, order.MissingValuesFirst)
	})

	t.Run("with multiple fields in single order input", func(t *testing.T) {
		sort, err := UnmarshalSort([]byte(`[{"field_1":"$desc","field_2":"$asc"}]`))
		assert.NoError(t, err)
		assert.NotNil(t, sort)
		assert.Len(t, *sort, 1)

		order := (*sort)[0]
		assert.Equal(t, order.Name, "field_2")
		assert.True(t, order.Ascending)
		assert.False(t, order.MissingValuesFirst)
	})

	t.Run("with invalid sort order", func(t *testing.T) {
		sort, err := UnmarshalSort([]byte(`[{"field_1":"desc"}]`))
		assert.ErrorContains(t, err, "Sort order can only be `$asc` or `$desc`")
		assert.Nil(t, sort)
	})

	t.Run("Unmarshal 4 sort orders", func(t *testing.T) {
		rawInput := []byte(`[{"field_1":"$asc"},{"field_2":"$desc"},{"field_3":"$asc"},{"field_4":"$asc"}]`)
		sort, err := UnmarshalSort(rawInput)
		assert.ErrorContains(t, err, "Sorting can support up to `3` fields")
		assert.Nil(t, sort)
	})

	t.Run("with invalid array object", func(t *testing.T) {
		sort, err := UnmarshalSort([]byte(`["field_1"]`))
		assert.ErrorContains(t, err, "Invalid value for `sort`")
		assert.Nil(t, sort)
	})

	t.Run("with object instead of array", func(t *testing.T) {
		sort, err := UnmarshalSort([]byte(`{"field_1":"$asc"}`))
		assert.ErrorContains(t, err, "Invalid value for `sort`")
		assert.Nil(t, sort)
	})
}
