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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"

	jsoniter "github.com/json-iterator/go"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/server/metrics"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/typesense/typesense-go/typesense"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

type storeImpl struct {
	client *typesense.Client
}

type storeImplWithMetrics struct {
	s Store
}

const StreamContentType = "application/x-json-stream"

func (m *storeImplWithMetrics) measure(ctx context.Context, name string, f func(ctx context.Context) error) {
	// Low level measurement wrapper that is called by the measure functions on the appropriate receiver
	measurement := metrics.NewMeasurement("tigris.search", name, metrics.SearchSpanType, metrics.GetSearchTags(name))
	ctx = measurement.StartTracing(ctx, true)
	err := f(ctx)
	if err == nil {
		// Request was ok
		measurement.CountOkForScope(metrics.SearchOkCount, measurement.GetSearchOkTags())
		_ = measurement.FinishTracing(ctx)
		measurement.RecordDuration(metrics.SearchRespTime, measurement.GetSearchOkTags())
		return
	}
	// Request had error
	measurement.CountErrorForScope(metrics.SearchErrorCount, measurement.GetSearchErrorTags(err))
	_ = measurement.FinishWithError(ctx, "search", err)
	measurement.RecordDuration(metrics.SearchErrorRespTime, measurement.GetSearchErrorTags(err))
}

func (m *storeImplWithMetrics) AllCollections(ctx context.Context) (resp map[string]*tsApi.CollectionResponse, err error) {
	m.measure(ctx, "AllCollections", func(ctx context.Context) error {
		resp, err = m.s.AllCollections(ctx)
		return err
	})
	return
}

func (m *storeImplWithMetrics) DescribeCollection(ctx context.Context, name string) (resp *tsApi.CollectionResponse, err error) {
	m.measure(ctx, "DescribeCollection", func(ctx context.Context) error {
		resp, err = m.s.DescribeCollection(ctx, name)
		return err
	})
	return
}

func (m *storeImplWithMetrics) CreateCollection(ctx context.Context, schema *tsApi.CollectionSchema) (err error) {
	m.measure(ctx, "CreateCollection", func(ctx context.Context) error {
		err = m.s.CreateCollection(ctx, schema)
		return err
	})
	return
}

func (m *storeImplWithMetrics) UpdateCollection(ctx context.Context, name string, schema *tsApi.CollectionUpdateSchema) (err error) {
	m.measure(ctx, "UpdateCollection", func(ctx context.Context) error {
		err = m.s.UpdateCollection(ctx, name, schema)
		return err
	})
	return
}

func (m *storeImplWithMetrics) DropCollection(ctx context.Context, table string) (err error) {
	m.measure(ctx, "DropCollection", func(ctx context.Context) error {
		err = m.s.DropCollection(ctx, table)
		return err
	})
	return
}

func (m *storeImplWithMetrics) IndexDocuments(ctx context.Context, table string, documents io.Reader, options IndexDocumentsOptions) (err error) {
	m.measure(ctx, "IndexDocuments", func(ctx context.Context) error {
		err = m.s.IndexDocuments(ctx, table, documents, options)
		return err
	})
	return
}

func (m *storeImplWithMetrics) DeleteDocuments(ctx context.Context, table string, key string) (err error) {
	m.measure(ctx, "DeleteDocuments", func(ctx context.Context) error {
		err = m.s.DeleteDocuments(ctx, table, key)
		return err
	})
	return
}

func (m *storeImplWithMetrics) Search(ctx context.Context, table string, query *qsearch.Query, pageNo int) (result []tsApi.SearchResult, err error) {
	m.measure(ctx, "Search", func(ctx context.Context) error {
		result, err = m.s.Search(ctx, table, query, pageNo)
		return err
	})
	return
}

type IndexDocumentsOptions struct {
	Action    string
	BatchSize int
}

func (s *storeImpl) convertToInternalError(err error) error {
	if e, ok := err.(*typesense.HTTPError); ok {
		switch e.Status {
		case http.StatusConflict:
			return ErrDuplicateEntity
		case http.StatusNotFound:
			return ErrNotFound
		}
		return NewSearchError(e.Status, ErrCodeUnhandled, e.Error())
	}

	if e, ok := err.(*json.UnmarshalTypeError); ok {
		ulog.E(e)
		return NewSearchError(http.StatusInternalServerError, ErrCodeUnhandled, "Search read failed")
	}

	return err
}

func (s *storeImpl) DeleteDocuments(_ context.Context, table string, key string) error {
	_, err := s.client.Collection(table).Document(key).Delete()
	return s.convertToInternalError(err)
}

func (s *storeImpl) IndexDocuments(_ context.Context, table string, reader io.Reader, options IndexDocumentsOptions) (err error) {
	var closer io.ReadCloser
	closer, err = s.client.Collection(table).Documents().ImportJsonl(reader, &tsApi.ImportDocumentsParams{
		Action:    &options.Action,
		BatchSize: &options.BatchSize,
	})
	if err != nil {
		return err
	}
	defer func() { ulog.E(closer.Close()) }()

	type resp struct {
		Code     int
		Document string
		Error    string
		Success  bool
	}
	if closer != nil {
		var r resp
		res, err := io.ReadAll(closer)
		if err != nil {
			return err
		}
		if err = jsoniter.Unmarshal(res, &r); err != nil {
			return err
		}
		if len(r.Error) > 0 {
			if err = fmt.Errorf(r.Error); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storeImpl) getBaseSearchParam(query *qsearch.Query, pageNo int) tsApi.MultiSearchParameters {
	baseParam := tsApi.MultiSearchParameters{
		Q:       &query.Q,
		Page:    &pageNo,
		PerPage: &query.PageSize,
	}
	if fields := query.ToSearchFields(); len(fields) > 0 {
		baseParam.QueryBy = &fields
	}
	if facets := query.ToSearchFacets(); len(facets) > 0 {
		baseParam.FacetBy = &facets
		if size := query.ToSearchFacetSize(); size > 0 {
			baseParam.MaxFacetValues = &size
		}
	}
	if sortBy := query.ToSortFields(); len(sortBy) > 0 {
		baseParam.SortBy = &sortBy
	}

	return baseParam
}

func (s *storeImpl) Search(_ context.Context, table string, query *qsearch.Query, pageNo int) ([]tsApi.SearchResult, error) {
	var params []tsApi.MultiSearchCollectionParameters
	searchFilter := query.ToSearchFilter()
	if len(searchFilter) > 0 {
		for i := 0; i < len(searchFilter); i++ {
			// ToDo: check all places
			param := s.getBaseSearchParam(query, pageNo)
			param.FilterBy = &searchFilter[i]
			params = append(params, tsApi.MultiSearchCollectionParameters{
				Collection:            table,
				MultiSearchParameters: param,
			})
		}
	} else {
		params = append(params, tsApi.MultiSearchCollectionParameters{
			Collection:            table,
			MultiSearchParameters: s.getBaseSearchParam(query, pageNo),
		})
	}

	res, err := s.client.MultiSearch.PerformWithContentType(&tsApi.MultiSearchParams{}, tsApi.MultiSearchSearchesParameter{
		Searches: params,
	}, StreamContentType)
	if err != nil {
		return nil, s.convertToInternalError(err)
	}

	log.Info().Str("res", string(res.Body)).Msg("search response")
	reader := bytes.NewReader(res.Body)
	decoder := jsoniter.NewDecoder(reader)
	decoder.UseNumber()

	var dest tsApi.MultiSearchResult
	if err := decoder.Decode(&dest); err != nil {
		return nil, s.convertToInternalError(err)
	}

	return dest.Results, nil
}

func (s *storeImpl) AllCollections(_ context.Context) (map[string]*tsApi.CollectionResponse, error) {
	resp, err := s.client.Collections().Retrieve()
	if err != nil {
		return nil, s.convertToInternalError(err)
	}

	respMap := make(map[string]*tsApi.CollectionResponse)
	for _, r := range resp {
		respMap[r.Name] = r
	}
	return respMap, nil
}

func (s *storeImpl) DescribeCollection(_ context.Context, name string) (*tsApi.CollectionResponse, error) {
	resp, err := s.client.Collection(name).Retrieve()
	if err != nil {
		return nil, s.convertToInternalError(err)
	}
	return resp, nil
}

func (s *storeImpl) CreateCollection(_ context.Context, schema *tsApi.CollectionSchema) error {
	_, err := s.client.Collections().Create(schema)
	return s.convertToInternalError(err)
}

func (s *storeImpl) UpdateCollection(_ context.Context, name string, schema *tsApi.CollectionUpdateSchema) error {
	_, err := s.client.Collection(name).Update(schema)
	return s.convertToInternalError(err)
}

func (s *storeImpl) DropCollection(_ context.Context, table string) error {
	_, err := s.client.Collection(table).Delete()
	return s.convertToInternalError(err)
}
