package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
)

func TestSearchQueryRunner_getFieldSelection(t *testing.T) {
	t.Run("only include fields are provided", func(t *testing.T) {
		collFields := []*schema.Field{
			{FieldName: "field_1"},
			{FieldName: "field_2"},
		}

		runner := &SearchQueryRunner{
			req: &api.SearchRequest{
				IncludeFields: []string{"field_1", "field_2"},
			},
		}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, err)
		assert.NotNil(t, factory)
		assert.Empty(t, factory.Exclude)
		assert.Len(t, factory.Include, 2)
		assert.Contains(t, factory.Include, "field_1")
		assert.Contains(t, factory.Include, "field_2")
	})

	t.Run("only exclude fields are provided", func(t *testing.T) {
		collFields := []*schema.Field{
			{FieldName: "field_1"},
			{FieldName: "field_2"},
		}

		runner := &SearchQueryRunner{
			req: &api.SearchRequest{
				ExcludeFields: []string{"field_1", "field_2"},
			},
		}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, err)
		assert.NotNil(t, factory)
		assert.Empty(t, factory.Include)
		assert.Len(t, factory.Exclude, 2)
		assert.Contains(t, factory.Exclude, "field_1")
		assert.Contains(t, factory.Exclude, "field_2")
	})

	t.Run("no fields to include or exclude", func(t *testing.T) {
		collFields := []*schema.Field{
			{FieldName: "field_1"},
			{FieldName: "field_2"},
		}
		runner := &SearchQueryRunner{req: &api.SearchRequest{}}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, err)
		assert.Nil(t, factory)
	})

	t.Run("no schema fields are defined", func(t *testing.T) {
		var collFields []*schema.Field
		runner := &SearchQueryRunner{req: &api.SearchRequest{}}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, err)
		assert.Nil(t, factory)
	})

	t.Run("selection fields are not in schema", func(t *testing.T) {
		collFields := []*schema.Field{
			{FieldName: "field_1"},
			{FieldName: "field_2"},
		}

		runner := &SearchQueryRunner{
			req: &api.SearchRequest{
				ExcludeFields: []string{"field_2", "field_3"},
			},
		}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, factory)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "`field_3` is not a schema field")
	})
}
