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

package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/schema"
)

func TestSearchQueryRunner_getFacetFields(t *testing.T) {
	collection := &schema.DefaultCollection{
		QueryableFields: []*schema.QueryableField{
			schema.NewQueryableField("field_1", schema.StringType, schema.UnknownType, nil, nil),
			schema.NewQueryableField("parent.field_2", schema.StringType, schema.UnknownType, nil, nil),
			schema.NewQueryableField("field_3", schema.ByteType, schema.UnknownType, nil, nil),
			schema.NewQueryableField("field_4", schema.StringType, schema.UnknownType, nil, nil),
		},
	}
	runner := &SearchQueryRunner{req: &api.SearchRequest{}}

	t.Run("no facet field param in input", func(t *testing.T) {
		runner.req.Facet = nil
		facets, err := runner.getFacetFields(collection)
		assert.NoError(t, err)
		assert.NotNil(t, facets)
		assert.Empty(t, facets.Fields)
	})

	t.Run("no queryable field in collection", func(t *testing.T) {
		var collFields []*schema.QueryableField
		collection := &schema.DefaultCollection{QueryableFields: collFields}
		runner.req.Facet = []byte(`{"field_1":{"size":10}}`)
		facets, err := runner.getFacetFields(collection)
		assert.ErrorContains(t, err, "`field_1` is not present in collection")
		assert.NotNil(t, facets)
		assert.Empty(t, facets.Fields)
	})

	t.Run("requested facet field is not faceted in collection", func(t *testing.T) {
		runner.req.Facet = []byte(`{"parent.field_2":{"size":10},"field_3":{"size":10}}`)
		facets, err := runner.getFacetFields(collection)
		assert.ErrorContains(t, err, "only supported for numeric and text fields")
		assert.NotNil(t, facets)
		assert.Empty(t, facets.Fields)
	})

	t.Run("requested facet fields are not in collection", func(t *testing.T) {
		runner.req.Facet = []byte(`{"field_1":{"size":10},"field_5":{"size":10}}`)
		facets, err := runner.getFacetFields(collection)
		assert.ErrorContains(t, err, "`field_5` is not present in collection")
		assert.NotNil(t, facets)
		assert.Empty(t, facets.Fields)
	})

	t.Run("valid facet fields requested", func(t *testing.T) {
		runner.req.Facet = []byte(`{"field_1":{"size":10},"parent.field_2":{"size":10}}`)
		facets, err := runner.getFacetFields(collection)
		assert.NoError(t, err)
		assert.Len(t, facets.Fields, 2)
		for _, ff := range facets.Fields {
			assert.Contains(t, []string{"field_1", "parent.field_2"}, ff.Name)
			assert.Equal(t, ff.Size, 10)
		}
	})
}

func TestSearchQueryRunner_getFieldSelection(t *testing.T) {
	collection := &schema.DefaultCollection{
		QueryableFields: []*schema.QueryableField{
			schema.NewQueryableField("field_1", schema.StringType, schema.UnknownType, nil, nil),
			schema.NewQueryableField("parent.field_2", schema.StringType, schema.UnknownType, nil, nil),
		},
	}

	t.Run("only include fields are provided", func(t *testing.T) {
		runner := &SearchQueryRunner{
			req: &api.SearchRequest{
				IncludeFields: []string{"field_1", "parent.field_2"},
			},
		}

		factory, err := runner.getFieldSelection(collection)

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

		factory, err := runner.getFieldSelection(collection)

		assert.Nil(t, err)
		assert.NotNil(t, factory)
		assert.Empty(t, factory.Include)
		assert.Len(t, factory.Exclude, 2)
		assert.Contains(t, factory.Exclude, "field_1")
		assert.Contains(t, factory.Exclude, "parent.field_2")
	})

	t.Run("no fields to include or exclude", func(t *testing.T) {
		runner := &SearchQueryRunner{req: &api.SearchRequest{}}

		factory, err := runner.getFieldSelection(collection)

		assert.Nil(t, err)
		assert.Nil(t, factory)
	})

	t.Run("no schema fields are defined", func(t *testing.T) {
		collection := &schema.DefaultCollection{}
		runner := &SearchQueryRunner{req: &api.SearchRequest{}}

		factory, err := runner.getFieldSelection(collection)

		assert.Nil(t, err)
		assert.Nil(t, factory)
	})

	t.Run("selection fields are not in schema", func(t *testing.T) {
		runner := &SearchQueryRunner{
			req: &api.SearchRequest{
				ExcludeFields: []string{"field_1", "field_3"},
			},
		}

		factory, err := runner.getFieldSelection(collection)

		assert.Nil(t, factory)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "`field_3` is not present in collection")
	})
}

func TestSearchQueryRunner_getSortOrdering(t *testing.T) {
	collection := &schema.DefaultCollection{
		QueryableFields: []*schema.QueryableField{
			schema.NewQueryableField("field_1", schema.StringType, schema.UnknownType, nil, nil),
			schema.NewQueryableField("parent.field_2", schema.StringType, schema.UnknownType, nil, nil),
			schema.NewQueryableField("field_3", schema.ByteType, schema.UnknownType, nil, nil),
		},
	}
	collection.QueryableFields[0].Sortable = true
	collection.QueryableFields[1].Sortable = true

	runner := &SearchQueryRunner{req: &api.SearchRequest{}}

	t.Run("no sort param in input", func(t *testing.T) {
		runner.req.Sort = nil
		ordering, err := runner.getSortOrdering(collection, runner.req.Sort)
		assert.NoError(t, err)
		assert.Nil(t, ordering)
	})

	t.Run("no queryable field in collection", func(t *testing.T) {
		collection := &schema.DefaultCollection{}
		runner.req.Sort = []byte(`[{"field_1":"$asc"}]`)
		sortOrder, err := runner.getSortOrdering(collection, runner.req.Sort)
		assert.ErrorContains(t, err, "`field_1` is not present in collection")
		assert.Nil(t, sortOrder)
	})

	t.Run("requested sort field is not sortable in collection", func(t *testing.T) {
		runner.req.Sort = []byte(`[{"field_1":"$desc"},{"field_3":"$asc"}]`)
		sortOrder, err := runner.getSortOrdering(collection, runner.req.Sort)
		assert.ErrorContains(t, err, "Cannot sort on `field_3` field")
		assert.Nil(t, sortOrder)
	})

	t.Run("valid sort fields requested", func(t *testing.T) {
		runner.req.Sort = []byte(`[{"field_1":"$desc"},{"parent.field_2":"$asc"}]`)
		sortOrder, err := runner.getSortOrdering(collection, runner.req.Sort)
		assert.NoError(t, err)
		assert.NotNil(t, sortOrder)
		expected := &sort.Ordering{
			{Name: "field_1", Ascending: false, MissingValuesFirst: false},
			{Name: "parent.field_2", Ascending: true, MissingValuesFirst: false},
		}
		assert.Exactly(t, expected, sortOrder)
	})

	t.Run("Invalid sort input", func(t *testing.T) {
		runner.req.Sort = []byte(`[{"field_1":"descending"}]`)
		sort, err := runner.getSortOrdering(collection, runner.req.Sort)
		assert.ErrorContains(t, err, "Sort order can only be `$asc` or `$desc`")
		assert.Nil(t, sort)
	})
}
