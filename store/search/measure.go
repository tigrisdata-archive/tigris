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
	"context"
	"fmt"
	"io"
	"net"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/query/filter"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/typesense-go/typesense"
	tsApi "github.com/tigrisdata/typesense-go/typesense/api"
)

type storeImplWithMetrics struct {
	s Store
}

func NewStoreWithMetrics(config *config.SearchConfig) (Store, error) {
	client := typesense.NewClient(
		typesense.WithServer(fmt.Sprintf("http://%s", net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port)))),
		typesense.WithAPIKey(config.AuthKey))
	log.Info().Str("host", config.Host).Int16("port", config.Port).Msg("initialized search store")
	return &storeImplWithMetrics{
		&storeImpl{
			client: client,
		},
	}, nil
}

func (m *storeImplWithMetrics) measure(ctx context.Context, name string, f func(ctx context.Context) error) {
	// Low level measurement wrapper that is called by the measure functions on the appropriate receiver
	measurement := metrics.NewMeasurement("tigris.search", name, metrics.SearchSpanType, metrics.GetSearchTags(name))
	ctx = measurement.StartTracing(ctx, true)
	err := f(ctx)
	if err != nil {
		// Request had error
		measurement.CountErrorForScope(metrics.SearchErrorCount, measurement.GetSearchErrorTags(err))
		_ = measurement.FinishWithError(ctx, err)
		measurement.RecordDuration(metrics.SearchErrorRespTime, measurement.GetSearchErrorTags(err))
	} else {
		// Request was ok
		measurement.CountOkForScope(metrics.SearchOkCount, measurement.GetSearchOkTags())
		_ = measurement.FinishTracing(ctx)
		measurement.RecordDuration(metrics.SearchRespTime, measurement.GetSearchOkTags())
	}
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
	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)
	m.measure(ctx, "CreateCollection", func(ctx context.Context) error {
		err = m.s.CreateCollection(ctx, schema)
		return err
	})
	if reqStatus != nil && reqStatusExists {
		reqStatus.AddSearchCreateIndexUnit()
	}
	return
}

func (m *storeImplWithMetrics) UpdateCollection(ctx context.Context, name string, schema *tsApi.CollectionUpdateSchema) (err error) {
	// TODO: measure the bytes written in global status
	m.measure(ctx, "UpdateCollection", func(ctx context.Context) error {
		err = m.s.UpdateCollection(ctx, name, schema)
		return err
	})
	return
}

func (m *storeImplWithMetrics) DropCollection(ctx context.Context, table string) (err error) {
	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)
	m.measure(ctx, "DropCollection", func(ctx context.Context) error {
		err = m.s.DropCollection(ctx, table)
		return err
	})
	if reqStatus != nil && reqStatusExists {
		reqStatus.AddSearchDropIndexUnit()
	}
	return
}

func (m *storeImplWithMetrics) IndexDocuments(ctx context.Context, table string, documents io.Reader, options IndexDocumentsOptions) (resp []IndexResp, err error) {
	// TODO: count the bytes written in global status
	m.measure(ctx, "IndexDocuments", func(ctx context.Context) error {
		resp, err = m.s.IndexDocuments(ctx, table, documents, options)
		return err
	})
	return
}

func (m *storeImplWithMetrics) DeleteDocument(ctx context.Context, table string, key string) (err error) {
	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)
	m.measure(ctx, "DeleteDocument", func(ctx context.Context) error {
		err = m.s.DeleteDocument(ctx, table, key)
		return err
	})
	if reqStatus != nil && reqStatusExists {
		reqStatus.AddSearchDeleteDocumentUnit(1)
	}
	return
}

func (m *storeImplWithMetrics) DeleteDocuments(ctx context.Context, table string, filter *filter.WrappedFilter) (count int, err error) {
	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)
	m.measure(ctx, "DeleteDocuments", func(ctx context.Context) error {
		count, err = m.s.DeleteDocuments(ctx, table, filter)
		return err
	})
	if reqStatus != nil && reqStatusExists {
		reqStatus.AddSearchDeleteDocumentUnit(count)
	}
	return
}

func (m *storeImplWithMetrics) Search(ctx context.Context, table string, query *qsearch.Query, pageNo int) (result []tsApi.SearchResult, err error) {
	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)
	m.measure(ctx, "Search", func(ctx context.Context) error {
		result, err = m.s.Search(ctx, table, query, pageNo)
		return err
	})
	if reqStatus != nil && reqStatusExists {
		if reqStatus.IsCollectionSearch() {
			reqStatus.AddCollectionSearchUnit()
		}
		if reqStatus.IsApiSearch() {
			reqStatus.AddSearchUnit()
		}
	}
	return
}

func (m *storeImplWithMetrics) GetDocuments(ctx context.Context, table string, ids []string) (result *tsApi.SearchResult, err error) {
	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)
	m.measure(ctx, "Get", func(ctx context.Context) error {
		result, err = m.s.GetDocuments(ctx, table, ids)
		return err
	})
	if reqStatus != nil && reqStatusExists {
		if reqStatus.IsCollectionSearch() {
			reqStatus.AddCollectionSearchUnit()
		}
		if reqStatus.IsApiSearch() {
			reqStatus.AddSearchUnit()
		}
	}
	return
}

func (m *storeImplWithMetrics) CreateDocument(ctx context.Context, table string, doc map[string]any) (err error) {
	// TODO: measure the bytes written in global status
	m.measure(ctx, "Create", func(ctx context.Context) error {
		err = m.s.CreateDocument(ctx, table, doc)
		return err
	})
	return
}
