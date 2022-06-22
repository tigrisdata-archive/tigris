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

	"github.com/rs/zerolog/log"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/typesense/typesense-go/typesense"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

type Store interface {
	CreateCollection(ctx context.Context, schema *tsApi.CollectionSchema) error
	UpdateCollection(ctx context.Context, name string, schema *tsApi.CollectionUpdateSchema) error
	DropCollection(ctx context.Context, table string) error
	IndexDocuments(ctx context.Context, table string, documents io.Reader, options IndexDocumentsOptions) error
	DeleteDocuments(ctx context.Context, table string, key string) error
	Search(ctx context.Context, table string, query *qsearch.Query, pageNo int) ([]tsApi.SearchResult, error)
}

func NewStore(config *config.SearchConfig) (Store, error) {
	client := typesense.NewClient(
		typesense.WithServer(fmt.Sprintf("http://%s:%d", config.Host, config.Port)),
		typesense.WithAPIKey(config.AuthKey))
	log.Info().Str("host", config.Host).Int16("port", config.Port).Msg("initialized search store")
	return &storeImpl{
		client: client,
	}, nil
}

type NoopStore struct{}

func (n *NoopStore) CreateCollection(context.Context, *tsApi.CollectionSchema) error { return nil }
func (n *NoopStore) UpdateCollection(context.Context, string, *tsApi.CollectionUpdateSchema) error {
	return nil
}
func (n *NoopStore) DropCollection(context.Context, string) error { return nil }
func (n *NoopStore) IndexDocuments(context.Context, string, io.Reader, IndexDocumentsOptions) error {
	return nil
}
func (n *NoopStore) DeleteDocuments(context.Context, string, string) error { return nil }
func (n *NoopStore) Search(context.Context, string, *qsearch.Query, int) ([]tsApi.SearchResult, error) {
	return nil, nil
}
