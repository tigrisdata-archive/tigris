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
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/query/filter"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/typesense/typesense-go/typesense"
	tsApi "github.com/typesense/typesense-go/typesense/api"
	"github.com/typesense/typesense-go/typesense/api/circuit"
)

const (
	defaultConnectionTimeout  = 5 * time.Second
	defaultCircuitBreakerName = "searchClient"
)

type IndexAction string

var (
	Create  IndexAction = "create"
	Replace IndexAction = "upsert"
	Update  IndexAction = "update"
)

type Store interface {
	// AllCollections is to describe all search indexes.
	AllCollections(ctx context.Context) (map[string]*internal.SearchIndexResponse, error)
	// DescribeCollection is to describe a search index.
	DescribeCollection(ctx context.Context, name string) (*internal.SearchIndexResponse, error)
	// CreateCollection is to create a search index.
	CreateCollection(ctx context.Context, schema *internal.SearchIndexSchema) error
	// UpdateCollection is to update the search index.
	UpdateCollection(ctx context.Context, name string, schema *internal.SearchIndexSchema) error
	// DropCollection is to drop the search index.
	DropCollection(ctx context.Context, table string) error
	// CreateDocument is to create and index a single document
	CreateDocument(_ context.Context, table string, doc map[string]any) error
	// IndexDocuments is to index batch of documents. It expects index action to decide whether it needs to create/upsert/update documents.
	IndexDocuments(ctx context.Context, table string, documents io.Reader, options IndexDocumentsOptions) ([]IndexDocumentResp, error)
	// DeleteDocument is deleting a single document using id.
	DeleteDocument(ctx context.Context, table string, key string) error
	// DeleteDocuments is to delete multiple documents using filter.
	DeleteDocuments(ctx context.Context, table string, filter *filter.WrappedFilter) (int, error)
	// Search is to search using Query.
	Search(ctx context.Context, table string, query *qsearch.Query, pageNo int) ([]tsApi.SearchResult, error)
	// GetDocuments is to get a single or multiple documents by id.
	GetDocuments(ctx context.Context, table string, ids []string) (*tsApi.SearchResult, error)
}

func NewStoreWithMetrics(config *config.SearchConfig) (Store, error) {
	client, err := NewStore(config)
	if err != nil {
		return nil, err
	}

	return &storeImplWithMetrics{
		s: client,
	}, nil
}

func NewStore(config *config.SearchConfig) (Store, error) {
	cb := circuit.NewGoBreaker(
		circuit.WithGoBreakerName(defaultCircuitBreakerName),
		circuit.WithGoBreakerMaxRequests(circuit.DefaultGoBreakerMaxRequests),
		circuit.WithGoBreakerInterval(circuit.DefaultGoBreakerInterval),
		circuit.WithGoBreakerTimeout(circuit.DefaultGoBreakerTimeout),
		circuit.WithGoBreakerReadyToTrip(circuit.DefaultReadyToTrip),
	)
	httpClient := circuit.NewHTTPClient(
		circuit.WithHTTPRequestDoer(&http.Client{
			Timeout: defaultConnectionTimeout,
		}),
		circuit.WithCircuitBreaker(cb),
	)

	url := fmt.Sprintf("http://%s", net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port)))
	tsAPIClient, _ := tsApi.NewClientWithResponses(url,
		tsApi.WithAPIKey(config.AuthKey),
		tsApi.WithHTTPClient(httpClient))

	client := typesense.NewClient(
		typesense.WithServer(url),
		typesense.WithAPIKey(config.AuthKey))

	log.Info().Str("host", config.Host).Int16("port", config.Port).Msg("initialized search store")
	return &storeImpl{
		apiClient: tsAPIClient,
		client:    client,
	}, nil
}

type NoopStore struct{}

func (n *NoopStore) AllCollections(context.Context) (map[string]*internal.SearchIndexResponse, error) {
	return nil, nil
}

func (n *NoopStore) DescribeCollection(context.Context, string) (*internal.SearchIndexResponse, error) {
	return &internal.SearchIndexResponse{}, nil
}
func (n *NoopStore) CreateCollection(context.Context, *internal.SearchIndexSchema) error { return nil }
func (n *NoopStore) UpdateCollection(context.Context, string, *internal.SearchIndexSchema) error {
	return nil
}
func (n *NoopStore) DropCollection(context.Context, string) error { return nil }
func (n *NoopStore) IndexDocuments(context.Context, string, io.Reader, IndexDocumentsOptions) ([]IndexDocumentResp, error) {
	return nil, nil
}
func (n *NoopStore) DeleteDocument(context.Context, string, string) error { return nil }
func (n *NoopStore) DeleteDocuments(context.Context, string, *filter.WrappedFilter) (int, error) {
	return 0, nil
}

func (n *NoopStore) Search(context.Context, string, *qsearch.Query, int) ([]tsApi.SearchResult, error) {
	return nil, nil
}

func (n *NoopStore) GetDocuments(_ context.Context, _ string, _ []string) (*tsApi.SearchResult, error) {
	return nil, nil
}

func (n *NoopStore) CreateDocument(_ context.Context, _ string, _ map[string]any) error {
	return nil
}
