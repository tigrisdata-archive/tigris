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
	"github.com/tigrisdata/typesense-go/typesense"
	tsApi "github.com/tigrisdata/typesense-go/typesense/api"
)

type IndexAction string

var (
	Create  IndexAction = "create"
	Replace IndexAction = "upsert"
	Update  IndexAction = "update"
)

type Store interface {
	// AllCollections is to describe all search indexes.
	AllCollections(ctx context.Context) (map[string]*tsApi.CollectionResponse, error)
	// DescribeCollection is to describe a search index.
	DescribeCollection(ctx context.Context, name string) (*tsApi.CollectionResponse, error)
	// CreateCollection is to create a search index.
	CreateCollection(ctx context.Context, schema *tsApi.CollectionSchema) error
	// UpdateCollection is to update the search index.
	UpdateCollection(ctx context.Context, name string, schema *tsApi.CollectionUpdateSchema) error
	// DropCollection is to drop the search index.
	DropCollection(ctx context.Context, table string) error
	// CreateDocument is to create and index a single document
	CreateDocument(_ context.Context, table string, doc map[string]any) error
	// IndexDocuments is to index batch of documents. It expects index action to decide whether it needs to create/upsert/update documents.
	IndexDocuments(ctx context.Context, table string, documents io.Reader, options IndexDocumentsOptions) ([]IndexResp, error) // bytes written - bytes from io.Reader
	// DeleteDocument is deleting a single document using id.
	DeleteDocument(ctx context.Context, table string, key string) error
	// DeleteDocuments is to delete multiple documents using filter.
	DeleteDocuments(ctx context.Context, table string, filter *filter.WrappedFilter) (int, error)
	// Search is to search using Query.
	Search(ctx context.Context, table string, query *qsearch.Query, pageNo int) ([]tsApi.SearchResult, error)
	// GetDocuments is to get a single or multiple documents by id.
	GetDocuments(ctx context.Context, table string, ids []string) (*tsApi.SearchResult, error)
}

func NewStore(config *config.SearchConfig) (Store, error) {
	client := typesense.NewClient(
		typesense.WithServer(fmt.Sprintf("http://%s", net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port)))),
		typesense.WithAPIKey(config.AuthKey))
	log.Info().Str("host", config.Host).Int16("port", config.Port).Msg("initialized search store")
	return &storeImpl{
		client: client,
	}, nil
}

type NoopStore struct{}

func (n *NoopStore) AllCollections(context.Context) (map[string]*tsApi.CollectionResponse, error) {
	return nil, nil
}

func (n *NoopStore) DescribeCollection(context.Context, string) (*tsApi.CollectionResponse, error) {
	return &tsApi.CollectionResponse{}, nil
}
func (n *NoopStore) CreateCollection(context.Context, *tsApi.CollectionSchema) error { return nil }
func (n *NoopStore) UpdateCollection(context.Context, string, *tsApi.CollectionUpdateSchema) error {
	return nil
}
func (n *NoopStore) DropCollection(context.Context, string) error { return nil }
func (n *NoopStore) IndexDocuments(context.Context, string, io.Reader, IndexDocumentsOptions) ([]IndexResp, error) {
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
