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
	"context"
	"fmt"
	"io"
	"net/http"

	jsoniter "github.com/json-iterator/go"
	qsearch "github.com/tigrisdata/tigris/query/search"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/typesense/typesense-go/typesense"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

type storeImpl struct {
	client *typesense.Client
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
	var baseParam = tsApi.MultiSearchParameters{
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

	return baseParam
}

func (s *storeImpl) Search(_ context.Context, table string, query *qsearch.Query, pageNo int) ([]tsApi.SearchResult, error) {
	var params []tsApi.MultiSearchCollectionParameters
	searchFilter := query.ToSearchFilter()
	if len(searchFilter) > 0 {
		for i := 0; i < len(searchFilter); i++ {
			//ToDo: check all places
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

	res, err := s.client.MultiSearch.Perform(&tsApi.MultiSearchParams{}, tsApi.MultiSearchSearchesParameter{
		Searches: params,
	})
	if err != nil {
		return nil, err
	}

	return res.Results, nil
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
