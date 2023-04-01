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
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/query/filter"
	qsearch "github.com/tigrisdata/tigris/query/search"
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

const StreamContentType = "application/x-json-stream"

type IndexDocumentsOptions struct {
	Action    IndexAction
	BatchSize int
}

func (*storeImpl) convertToInternalError(err error) error {
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

func (*storeImpl) getBaseSearchParam(table string, query *qsearch.Query, pageNo int) tsApi.MultiSearchCollectionParameters {
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
		log.Error().Err(err).Interface("query", query).Msg("search error")
		return nil, s.convertToInternalError(err)
	}

	reader := bytes.NewReader(res.Body)
	decoder := jsoniter.NewDecoder(reader)
	decoder.UseNumber()

	var dest tsApi.MultiSearchResult
	if err := decoder.Decode(&dest); err != nil {
		log.Error().Err(err).Interface("query", query).Msg("search error")
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
				log.Error().Err(err).Interface("query", query).Msg("search error")
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

func (s *storeImpl) GetDocuments(_ context.Context, table string, ids []string) (*tsApi.SearchResult, error) {
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
