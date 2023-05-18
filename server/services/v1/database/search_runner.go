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

package database

import (
	"context"
	"math"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
)

// SearchQueryRunner is a runner used for Queries that are reads and needs to return result in streaming fashion.
type SearchQueryRunner struct {
	*BaseQueryRunner

	req          *api.SearchRequest
	streaming    SearchStreaming
	queryMetrics *metrics.SearchQueryMetrics
}

// ReadOnly on search query runner is implemented as search queries do not need to be inside a transaction; in fact,
// there is no need to start any transaction for search queries as they are simply forwarded to the indexing store.
func (runner *SearchQueryRunner) ReadOnly(ctx context.Context, tenant *metadata.Tenant) (Response, context.Context, error) {
	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)
	if reqStatus != nil && reqStatusExists {
		reqStatus.SetCollectionSearchType()
	}

	db, err := runner.getDatabase(ctx, nil, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return Response{}, ctx, err
	}

	wrappedF, err := filter.NewFactory(collection.QueryableFields, value.NewCollationFrom(runner.req.Collation)).WrappedFilter(runner.req.Filter)
	if err != nil {
		return Response{}, ctx, err
	}
	if config.DefaultConfig.Search.LogFilter {
		log.Error().
			Str("tenant", tenant.Name()).
			Str("project", db.Name()).
			Str("collection", collection.Name).
			Str("filter", string(runner.req.Filter)).
			Msg("collection search filters")
	}

	searchFields, err := runner.getSearchFields(collection)
	if err != nil {
		return Response{}, ctx, err
	}

	facets, err := runner.getFacetFields(collection)
	if err != nil {
		return Response{}, ctx, err
	}

	if len(facets.Fields) == 0 {
		runner.queryMetrics.SetSearchType("search_all")
	} else {
		runner.queryMetrics.SetSearchType("faceted")
	}

	fieldSelection, err := runner.getFieldSelection(collection)
	if err != nil {
		return Response{}, ctx, err
	}

	sortOrder, err := runner.getSearchOrdering(collection, runner.req.Sort)
	if err != nil {
		return Response{}, ctx, err
	}

	if sortOrder != nil {
		runner.queryMetrics.SetSort(true)
	} else {
		runner.queryMetrics.SetSort(false)
	}

	vecSearch, err := runner.getVectorSearch(collection)
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	pageSize := int(runner.req.PageSize)
	if pageSize == 0 {
		pageSize = defaultPerPage
	}
	var totalPages *int32

	searchQ := qsearch.NewBuilder().
		Query(runner.req.Q).
		SearchFields(searchFields).
		Facets(facets).
		PageSize(pageSize).
		Filter(wrappedF).
		ReadFields(fieldSelection).
		SortOrder(sortOrder).
		VectorSearch(vecSearch).
		Build()
	if searchQ.IsQAndVectorBoth() {
		return Response{}, ctx, errors.InvalidArgument("Currently either full text or vector search is supported")
	}

	searchReader := NewSearchReader(ctx, runner.searchStore, collection, searchQ)
	var iterator *FilterableSearchIterator
	if runner.req.Page != 0 {
		iterator = searchReader.SinglePageIterator(ctx, collection, wrappedF, runner.req.Page)
	} else {
		iterator = searchReader.Iterator(ctx, collection, wrappedF)
	}
	if err != nil {
		return Response{}, ctx, err
	}

	pageNo := int32(defaultPageNo)
	if runner.req.Page > 0 {
		pageNo = runner.req.Page
	}
	for {
		resp := &api.SearchResponse{}
		var row Row
		for iterator.Next(&row) {
			if searchQ.ReadFields != nil {
				// apply field selection
				newValue, err := searchQ.ReadFields.Apply(row.Data.RawData)
				if ulog.E(err) {
					return Response{}, ctx, err
				}
				row.Data.RawData = newValue
			}

			resp.Hits = append(resp.Hits, &api.SearchHit{
				Data: row.Data.RawData,
				Metadata: &api.SearchHitMeta{
					CreatedAt: row.Data.CreateToProtoTS(),
					UpdatedAt: row.Data.UpdatedToProtoTS(),
				},
			})

			if len(resp.Hits) == pageSize {
				break
			}
		}

		resp.Facets = iterator.getFacets()
		if totalPages == nil {
			tp := int32(math.Ceil(float64(iterator.getTotalFound()) / float64(pageSize)))
			totalPages = &tp
		}

		resp.Meta = &api.SearchMetadata{
			Found:      iterator.getTotalFound(),
			TotalPages: *totalPages,
			Page: &api.Page{
				Current: pageNo,
				Size:    int32(searchQ.PageSize),
			},
		}
		// if no hits, got error, send only error
		// if no hits, no error, at least one response and break
		// if some hits, got an error, send current hits and then error (will be zero hits next time)
		// if some hits, no error, continue to send response
		if len(resp.Hits) == 0 {
			if iterator.Interrupted() != nil {
				return Response{}, ctx, iterator.Interrupted()
			}
			if pageNo > defaultPageNo && pageNo > runner.req.Page {
				break
			}
		}

		if err := runner.streaming.Send(resp); err != nil {
			return Response{}, ctx, err
		}

		pageNo++
	}

	return Response{}, ctx, nil
}

func (runner *SearchQueryRunner) getSearchFields(coll *schema.DefaultCollection) ([]string, error) {
	searchFields := runner.req.SearchFields
	if len(searchFields) == 0 {
		// this is to include all searchable fields if not present in the query
		for _, cf := range coll.GetQueryableFields() {
			if cf.DataType == schema.StringType && cf.SearchIndexed {
				searchFields = append(searchFields, cf.InMemoryName())
			}
		}
	} else {
		for i, sf := range searchFields {
			cf, err := coll.GetQueryableField(sf)
			if err != nil {
				return nil, err
			}
			if !cf.SearchIndexed {
				return nil, errors.InvalidArgument("`%s` is not a searchable field. Enable search indexing on this field", sf)
			}
			if cf.InMemoryName() != cf.Name() {
				searchFields[i] = cf.InMemoryName()
			}
		}
	}
	return searchFields, nil
}

func (runner *SearchQueryRunner) getFacetFields(coll *schema.DefaultCollection) (qsearch.Facets, error) {
	facets, err := qsearch.UnmarshalFacet(runner.req.Facet)
	if err != nil {
		return qsearch.Facets{}, err
	}

	for i, ff := range facets.Fields {
		cf, err := coll.GetQueryableField(ff.Name)
		if err != nil {
			return qsearch.Facets{}, err
		}
		if !cf.Faceted {
			return qsearch.Facets{}, errors.InvalidArgument(
				"Cannot generate facets for `%s`. Enable faceting on this field", ff.Name)
		}
		if cf.InMemoryName() != cf.Name() {
			facets.Fields[i].Name = cf.InMemoryName()
		}
	}

	return facets, nil
}

func (runner *SearchQueryRunner) getFieldSelection(coll *schema.DefaultCollection) (*read.FieldFactory, error) {
	var selectionFields []string

	// Only one of include/exclude. Honor inclusion over exclusion
	//nolint:gocritic
	if len(runner.req.IncludeFields) > 0 {
		selectionFields = runner.req.IncludeFields
	} else if len(runner.req.ExcludeFields) > 0 {
		selectionFields = runner.req.ExcludeFields
	} else {
		return nil, nil
	}

	factory := &read.FieldFactory{
		Include: map[string]read.Field{},
		Exclude: map[string]read.Field{},
	}

	for _, sf := range selectionFields {
		cf, err := coll.GetQueryableField(sf)
		if err != nil {
			return nil, err
		}

		factory.AddField(&read.SimpleField{
			Name: cf.Name(),
			Incl: len(runner.req.IncludeFields) > 0,
		})
	}

	return factory, nil
}

func (runner *SearchQueryRunner) getVectorSearch(coll *schema.DefaultCollection) (qsearch.VectorSearch, error) {
	vectorSearch, err := qsearch.UnmarshalVectorSearch(runner.req.Vector)
	if err != nil {
		return vectorSearch, err
	}
	if len(vectorSearch.VectorF) == 0 {
		return vectorSearch, nil
	}

	f, err := coll.GetQueryableField(vectorSearch.VectorF)
	if err != nil {
		return qsearch.VectorSearch{}, err
	}
	if f.DataType != schema.VectorType {
		return qsearch.VectorSearch{}, errors.InvalidArgument("Cannot perform vector search on non-vector type, field `%s` is not a vector", f.FieldName)
	}
	if f.Dimensions != nil && *f.Dimensions != len(vectorSearch.VectorV) {
		return qsearch.VectorSearch{}, errors.InvalidArgument("query vector is not same size as dimensions, expected size: %d", *f.Dimensions)
	}

	return vectorSearch, nil
}
