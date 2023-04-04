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

package search

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/query/filter"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/typesense-go/typesense"
	tsApi "github.com/tigrisdata/typesense-go/typesense/api"
)

var maxCandidates = 100

type IndexResp struct {
	Code     int
	Document string
	Error    string
	Success  bool
}

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
	_ = measurement.FinishWithError(ctx, err)
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

func (m *storeImplWithMetrics) IndexDocuments(ctx context.Context, table string, documents io.Reader, options IndexDocumentsOptions) (resp []IndexResp, err error) {
	m.measure(ctx, "IndexDocuments", func(ctx context.Context) error {
		resp, err = m.s.IndexDocuments(ctx, table, documents, options)
		return err
	})
	return
}

func (m *storeImplWithMetrics) DeleteDocument(ctx context.Context, table string, key string) (err error) {
	m.measure(ctx, "DeleteDocument", func(ctx context.Context) error {
		err = m.s.DeleteDocument(ctx, table, key)
		return err
	})
	return
}

func (m *storeImplWithMetrics) DeleteDocuments(ctx context.Context, table string, filter *filter.WrappedFilter) (count int, err error) {
	m.measure(ctx, "DeleteDocuments", func(ctx context.Context) error {
		count, err = m.s.DeleteDocuments(ctx, table, filter)
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

func (m *storeImplWithMetrics) GetDocuments(ctx context.Context, table string, ids []string) (result *tsApi.SearchResult, err error) {
	m.measure(ctx, "Get", func(ctx context.Context) error {
		result, err = m.s.GetDocuments(ctx, table, ids)
		return err
	})
	return
}

func (m *storeImplWithMetrics) CreateDocument(ctx context.Context, table string, doc map[string]any) (err error) {
	m.measure(ctx, "Create", func(ctx context.Context) error {
		err = m.s.CreateDocument(ctx, table, doc)
		return err
	})
	return
}

type IndexDocumentsOptions struct {
	Action    IndexAction
	BatchSize int
}

func (s *storeImpl) convertToInternalError(err error) error {
	if e, ok := err.(*typesense.HTTPError); ok {
		msgMap, decErr := util.JSONToMap(e.Body)
		if decErr != nil {
			return NewSearchError(e.Status, ErrCodeUnhandled, string(e.Body))
		}
		return NewSearchError(e.Status, ErrCodeUnhandled, msgMap["message"].(string))
	}

	if e, ok := err.(*json.UnmarshalTypeError); ok {
		ulog.E(e)
		return NewSearchError(http.StatusInternalServerError, ErrCodeUnhandled, "Search read failed")
	}

	return err
}

func (s *storeImpl) DeleteDocument(_ context.Context, table string, key string) error {
	_, err := s.client.Collection(table).Document(key).Delete()
	return s.convertToInternalError(err)
}

func (s *storeImpl) DeleteDocuments(_ context.Context, table string, filter *filter.WrappedFilter) (int, error) {
	f := filter.SearchFilter()
	params := &tsApi.DeleteDocumentsParams{
		FilterBy: &f,
	}
	count, err := s.client.Collection(table).Documents().Delete(params)
	return count, err
}

func (s *storeImpl) CreateDocument(_ context.Context, table string, doc map[string]any) error {
	_, err := s.client.Collection(table).Documents().Create(doc)
	return s.convertToInternalError(err)
}

func (s *storeImpl) IndexDocuments(_ context.Context, table string, reader io.Reader, options IndexDocumentsOptions) ([]IndexResp, error) {
	var err error
	var closer io.ReadCloser
	action := string(options.Action)
	closer, err = s.client.Collection(table).Documents().ImportJsonl(reader, &tsApi.ImportDocumentsParams{
		Action:    &action,
		BatchSize: &options.BatchSize,
	})
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var responses []IndexResp
	decoder := jsoniter.NewDecoder(closer)
	for decoder.More() {
		var single IndexResp
		if err := decoder.Decode(&single); err != nil {
			return nil, err
		}

		responses = append(responses, single)
	}

	return responses, nil
}

func (s *storeImpl) getBaseSearchParam(table string, query *qsearch.Query, pageNo int) tsApi.MultiSearchCollectionParameters {
	baseParam := tsApi.MultiSearchCollectionParameters{
		Q:          &query.Q,
		Collection: table,
		Page:       &pageNo,
		PerPage:    &query.PageSize,
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
	if groupBy := query.ToSearchGroupBy(); len(groupBy) > 0 {
		baseParam.GroupBy = &groupBy
	}
	if searchFilter := query.WrappedF.SearchFilter(); len(searchFilter) > 1 {
		baseParam.FilterBy = &searchFilter
	}
	if vector := query.ToSearchVector(); len(vector) > 0 {
		baseParam.VectorQuery = &vector
	}

	return baseParam
}

func (s *storeImpl) Search(_ context.Context, table string, query *qsearch.Query, pageNo int) ([]tsApi.SearchResult, error) {
	var params []tsApi.MultiSearchCollectionParameters
	params = append(params, s.getBaseSearchParam(table, query, pageNo))

	res, err := s.client.MultiSearch.PerformWithContentType(&tsApi.MultiSearchParams{
		MaxCandidates: &maxCandidates,
	}, tsApi.MultiSearchSearchesParameter{
		Searches: params,
	}, StreamContentType)
	if err != nil {
		return nil, s.convertToInternalError(err)
	}

	reader := bytes.NewReader(res.Body)
	decoder := jsoniter.NewDecoder(reader)
	decoder.UseNumber()

	var dest tsApi.MultiSearchResult
	if err := decoder.Decode(&dest); err != nil {
		return nil, s.convertToInternalError(err)
	}
	for _, each := range dest.Results {
		if each.Hits == nil && each.GroupedHits == nil {
			type errResult struct {
				Code    int    `json:"code"`
				Message string `json:"error"`
			}

			type errorsResult struct {
				Res []errResult `json:"results"`
			}

			var errorsRes errorsResult
			if err = jsoniter.Unmarshal(res.Body, &errorsRes); err == nil && len(errorsRes.Res) > 0 {
				return nil, NewSearchError(errorsRes.Res[0].Code, ErrCodeUnhandled, errorsRes.Res[0].Message)
			}
		}
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
	ptrTrue := true
	schema.EnableNestedFields = &ptrTrue

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

func (s *storeImpl) GetDocuments(ctx context.Context, table string, ids []string) (*tsApi.SearchResult, error) {
	filterBy := "id: ["
	for i, id := range ids {
		if i != 0 {
			filterBy += ","
		}
		filterBy += id
	}
	filterBy += "]"

	res, err := s.client.Collection(table).Documents().Search(&tsApi.SearchCollectionParams{
		Q:        "*",
		FilterBy: &filterBy,
	})

	return res, err
}
