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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/cdc"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/search"
)

// QueryRunnerFactory is responsible for creating query runners for different queries.
type QueryRunnerFactory struct {
	txMgr       *transaction.Manager
	encoder     metadata.Encoder
	cdcMgr      *cdc.Manager
	searchStore search.Store
}

// NewQueryRunnerFactory returns QueryRunnerFactory object.
func NewQueryRunnerFactory(txMgr *transaction.Manager, cdcMgr *cdc.Manager, searchStore search.Store) *QueryRunnerFactory {
	return &QueryRunnerFactory{
		txMgr:       txMgr,
		encoder:     metadata.NewEncoder(),
		cdcMgr:      cdcMgr,
		searchStore: searchStore,
	}
}

func (f *QueryRunnerFactory) GetImportQueryRunner(r *api.ImportRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *ImportQueryRunner {
	return &ImportQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetInsertQueryRunner(r *api.InsertRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *InsertQueryRunner {
	return &InsertQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetReplaceQueryRunner(r *api.ReplaceRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *ReplaceQueryRunner {
	return &ReplaceQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetUpdateQueryRunner(r *api.UpdateRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *UpdateQueryRunner {
	return &UpdateQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetDeleteQueryRunner(r *api.DeleteRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *DeleteQueryRunner {
	return &DeleteQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

// GetStreamingQueryRunner returns StreamingQueryRunner.
func (f *QueryRunnerFactory) GetStreamingQueryRunner(r *api.ReadRequest, streaming Streaming, qm *metrics.StreamingQueryMetrics, accessToken *types.AccessToken) *StreamingQueryRunner {
	return &StreamingQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		streaming:       streaming,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetExplainQueryRunner(r *api.ReadRequest, _ *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *ExplainQueryRunner {
	return &ExplainQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
	}
}

// GetSearchQueryRunner for executing Search.
func (f *QueryRunnerFactory) GetSearchQueryRunner(r *api.SearchRequest, streaming SearchStreaming, qm *metrics.SearchQueryMetrics, accessToken *types.AccessToken) *SearchQueryRunner {
	return &SearchQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		streaming:       streaming,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetCollectionQueryRunner(accessToken *types.AccessToken) *CollectionQueryRunner {
	return &CollectionQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
	}
}

func (f *QueryRunnerFactory) GetProjectQueryRunner(accessToken *types.AccessToken) *ProjectQueryRunner {
	return &ProjectQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
	}
}

func (f *QueryRunnerFactory) GetBranchQueryRunner(accessToken *types.AccessToken) *BranchQueryRunner {
	return &BranchQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
	}
}

func (f *QueryRunnerFactory) GetIndexRunner(req *api.IndexCollectionRequest, queryMetrics *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *IndexerRunner {
	return &IndexerRunner{
		req:             req,
		queryMetrics:    queryMetrics,
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
	}
}
