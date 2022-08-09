package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
)

func TestSearchQueryRunner_getFacetFields(t *testing.T) {
	collFields := []*schema.QueryableField{
		{FieldName: "field_1", Faceted: true, DataType: schema.StringType},
		{FieldName: "parent.field_2", Faceted: true, DataType: schema.StringType},
		{FieldName: "field_3", Faceted: false, DataType: schema.StringType},
		{FieldName: "field_4", Faceted: true, DataType: schema.Int32Type},
	}
	runner := &SearchQueryRunner{req: &api.SearchRequest{}}

	t.Run("no facet field param in input", func(t *testing.T) {
		runner.req.Facet = nil
		facets, err := runner.getFacetFields(collFields)
		assert.NoError(t, err)
		assert.NotNil(t, facets)
		assert.Empty(t, facets.Fields)
	})

	t.Run("no queryable field in collection", func(t *testing.T) {
		var collFields []*schema.QueryableField
		runner.req.Facet = []byte(`{"field_1":{"size":10}}`)
		facets, err := runner.getFacetFields(collFields)
		assert.ErrorContains(t, err, "`field_1` is not a schema field")
		assert.NotNil(t, facets)
		assert.Empty(t, facets.Fields)
	})

	t.Run("requested facet field is not faceted in collection", func(t *testing.T) {
		runner.req.Facet = []byte(`{"parent.field_2":{"size":10},"field_3":{"size":10}}`)
		facets, err := runner.getFacetFields(collFields)
		assert.ErrorContains(t, err, "only supported for numeric and text fields")
		assert.NotNil(t, facets)
		assert.Empty(t, facets.Fields)
	})

	t.Run("requested facet fields are not in collection", func(t *testing.T) {
		runner.req.Facet = []byte(`{"field_1":{"size":10},"field_5":{"size":10}}`)
		facets, err := runner.getFacetFields(collFields)
		assert.ErrorContains(t, err, "`field_5` is not a schema field")
		assert.NotNil(t, facets)
		assert.Empty(t, facets.Fields)
	})

	t.Run("valid facet fields requested", func(t *testing.T) {
		runner.req.Facet = []byte(`{"field_1":{"size":10},"parent.field_2":{"size":10}}`)
		facets, err := runner.getFacetFields(collFields)
		assert.NoError(t, err)
		assert.Len(t, facets.Fields, 2)
		for _, ff := range facets.Fields {
			assert.Contains(t, []string{"field_1", "parent.field_2"}, ff.Name)
			assert.Equal(t, ff.Size, 10)
		}
	})
}

func TestSearchQueryRunner_getFieldSelection(t *testing.T) {
	collFields := []*schema.QueryableField{
		{FieldName: "field_1"},
		{FieldName: "parent.field_2"},
	}

	t.Run("only include fields are provided", func(t *testing.T) {
		runner := &SearchQueryRunner{
			req: &api.SearchRequest{
				IncludeFields: []string{"field_1", "parent.field_2"},
			},
		}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, err)
		assert.NotNil(t, factory)
		assert.Empty(t, factory.Exclude)
		assert.Len(t, factory.Include, 2)
		assert.Contains(t, factory.Include, "field_1")
		assert.Contains(t, factory.Include, "parent.field_2")
	})

	t.Run("only exclude fields are provided", func(t *testing.T) {
		runner := &SearchQueryRunner{
			req: &api.SearchRequest{
				ExcludeFields: []string{"field_1", "parent.field_2"},
			},
		}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, err)
		assert.NotNil(t, factory)
		assert.Empty(t, factory.Include)
		assert.Len(t, factory.Exclude, 2)
		assert.Contains(t, factory.Exclude, "field_1")
		assert.Contains(t, factory.Exclude, "parent.field_2")
	})

	t.Run("no fields to include or exclude", func(t *testing.T) {
		runner := &SearchQueryRunner{req: &api.SearchRequest{}}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, err)
		assert.Nil(t, factory)
	})

	t.Run("no schema fields are defined", func(t *testing.T) {
		var collFields []*schema.QueryableField
		runner := &SearchQueryRunner{req: &api.SearchRequest{}}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, err)
		assert.Nil(t, factory)
	})

	t.Run("selection fields are not in schema", func(t *testing.T) {
		runner := &SearchQueryRunner{
			req: &api.SearchRequest{
				ExcludeFields: []string{"field_1", "field_3"},
			},
		}

		factory, err := runner.getFieldSelection(collFields)

		assert.Nil(t, factory)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "`field_3` is not a schema field")
	})
}
