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
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/buger/jsonparser"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
)

type IndexerRunner struct {
	*BaseQueryRunner

	req          *api.BuildCollectionIndexRequest
	queryMetrics *metrics.WriteQueryMetrics
}

func (runner *IndexerRunner) ReadOnly(ctx context.Context, tenant *metadata.Tenant) (Response, context.Context, error) {
	tx, err := runner.txMgr.StartTx(ctx)
	if err != nil {
		return Response{}, ctx, err
	}

	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant, runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	if err = tx.Commit(ctx); err != nil {
		return Response{}, ctx, err
	}

	indexer := NewSecondaryIndexer(coll, false)

	for _, index := range coll.SecondaryIndexes.All {
		if index.State == schema.INDEX_WRITE_MODE {
			index.State = schema.INDEX_WRITE_MODE_BUILDING
		}
	}
	if err = runner.updateCollectionState(ctx, tenant, db, coll.Name, coll.SecondaryIndexes.All); err != nil {
		return Response{}, ctx, err
	}

	if err = indexer.BuildCollection(ctx, runner.txMgr, nil); err != nil {
		log.Err(err).Msgf("Failed to index collection \"%s\" for db \"%s\"", coll.Name, db.DbName())
		return Response{}, ctx, err
	}

	for _, index := range coll.SecondaryIndexes.All {
		index.State = schema.INDEX_ACTIVE
	}

	if err = runner.updateCollectionState(ctx, tenant, db, coll.Name, coll.SecondaryIndexes.All); ulog.E(err) {
		return Response{}, ctx, err
	}

	runner.queryMetrics.SetWriteType("build_index")
	metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	return Response{
		Response: &api.BuildCollectionIndexResponse{
			Indexes: runner.indexToCollectionIndex(coll.SecondaryIndexes.All),
		},
	}, ctx, nil
}

func (runner *IndexerRunner) updateCollectionState(ctx context.Context, tenant *metadata.Tenant, db *metadata.Database, collName string, indexes []*schema.Index) error {
	tx, err := runner.txMgr.StartTx(ctx)
	if err != nil {
		return err
	}

	if err = tenant.UpdateCollectionIndexes(ctx, tx, db, collName, indexes); ulog.E(err) {
		return err
	}

	if err = tx.Commit(ctx); ulog.E(err) {
		return err
	}

	return nil
}

type SearchIndexerRunner struct {
	*BaseQueryRunner

	ctx            context.Context
	req            *api.BuildCollectionSearchIndexRequest
	queryMetrics   *metrics.WriteQueryMetrics
	collection     *schema.DefaultCollection
	sigDone        chan struct{}
	close          bool
	rowsWritten    int64
	since          time.Time
	logSince       time.Time
	ProgressUpdate func(context.Context) error
}

func (runner *SearchIndexerRunner) ReadOnly(ctx context.Context, tenant *metadata.Tenant) (Response, context.Context, error) {
	start := time.Now()
	tx, err := runner.txMgr.StartTx(ctx)
	if err != nil {
		return Response{}, ctx, err
	}
	if _, runner.collection, err = runner.getDBAndCollection(
		ctx,
		tx,
		tenant,
		runner.req.GetProject(),
		runner.req.GetCollection(),
		runner.req.GetBranch(),
	); err != nil {
		return Response{}, ctx, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Response{}, ctx, err
	}

	if _, err = runner.searchStore.DescribeCollection(ctx, runner.collection.ImplicitSearchIndex.StoreIndexName()); err != nil {
		if search.IsErrNotFound(err) {
			// this will be as part of options
			err = runner.searchStore.CreateCollection(ctx, runner.collection.ImplicitSearchIndex.StoreSchema)
		}

		if err != nil {
			return Response{}, ctx, err
		}
	}

	runner.sigDone = make(chan struct{}, 1)
	runner.ctx = ctx
	runner.since = time.Now()
	runner.logSince = time.Now()
	s := NewStreamer(ctx, tenant, runner.BaseQueryRunner, runner)
	s.Stream(createReadReq(runner.req))

	runner.wait()

	log.Info().Msgf("Total written '%d' rows in time '%v' for collection '%s' index '%s'",
		runner.rowsWritten, time.Since(start), runner.collection.Name, runner.collection.ImplicitSearchIndex.StoreIndexName())

	return Response{
		Response: &api.BuildCollectionSearchIndexResponse{
			Status: OkStatus,
		},
	}, nil, nil
}

func (runner *SearchIndexerRunner) wait() {
	select {
	case <-runner.ctx.Done():
	case <-runner.sigDone:
	}

	runner.close = true
}

func (runner *SearchIndexerRunner) done() {
	close(runner.sigDone)
}

func (runner *SearchIndexerRunner) consume(r *api.ReadResponse) error {
	if runner.close {
		return errors.Aborted("consumer exited")
	}
	index := runner.collection.PrimaryKey
	indexParts := make([]any, len(index.Fields)+1)
	indexParts[0] = index.Name
	for i, f := range index.Fields {
		jsonVal, dtp, _, err := jsonparser.Get(r.Data, f.FieldName)
		if err != nil || dtp == jsonparser.NotExist {
			return errors.Internal("unable to build index '%s' '%v'", err, dtp)
		}

		v, err := value.NewValue(f.Type(), jsonVal)
		if err != nil {
			return errors.Internal("unable to build index '%s' '%v'", err, dtp)
		}
		indexParts[i+1] = v.AsInterface()
	}

	id, err := CreateSearchKey(kv.BuildKey(indexParts...))
	if err != nil {
		return errors.Internal("unable to build search key '%v'", err)
	}

	searchData, err := PackSearchFields(runner.ctx, internal.NewTableData(r.Data), runner.collection, id)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(searchData)
	var resp []search.IndexResp
	if resp, err = runner.searchStore.IndexDocuments(runner.ctx, runner.collection.ImplicitSearchIndex.StoreIndexName(), reader, search.IndexDocumentsOptions{
		Action:    search.Create,
		BatchSize: 1,
	}); err != nil {
		return err
	}
	if len(resp) == 1 && !resp[0].Success {
		if resp[0].Code != http.StatusConflict {
			// ignore the conflicts
			return search.NewSearchError(resp[0].Code, search.ErrCodeUnhandled, resp[0].Error)
		}
	}

	runner.rowsWritten++
	if time.Since(runner.logSince) > 60*time.Second {
		log.Info().Msgf("Written '%d' rows in time '%v' for collection '%s' index '%s'",
			runner.rowsWritten, time.Since(runner.since), runner.collection.Name, runner.collection.ImplicitSearchIndex.StoreIndexName())
		runner.logSince = time.Now()

		if runner.ProgressUpdate != nil {
			if err = runner.ProgressUpdate(runner.ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

func createReadReq(req *api.BuildCollectionSearchIndexRequest) *api.ReadRequest {
	return &api.ReadRequest{
		Project:    req.Project,
		Collection: req.Collection,
		Branch:     req.Branch,
	}
}
