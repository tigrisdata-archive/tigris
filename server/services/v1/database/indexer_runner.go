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
	"context"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type IndexerRunner struct {
	*BaseQueryRunner

	req          *api.IndexCollectionRequest
	queryMetrics *metrics.WriteQueryMetrics
}

func (runner *IndexerRunner) ReadOnly(ctx context.Context, tenant *metadata.Tenant) (Response, context.Context, error) {
	tx, err := runner.txMgr.StartTx(ctx)
	if err != nil {
		return Response{}, ctx, err
	}

	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant,
		runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	if err = tx.Commit(ctx); err != nil {
		return Response{}, ctx, err
	}

	indexer := NewSecondaryIndexer(coll)

	if err = indexer.BuildCollection(ctx, runner.txMgr); err != nil {
		log.Err(err).Msgf("Failed to index collection \"%s\" for db \"%s\"", coll.Name, db.DbName())
		return Response{}, ctx, err
	}

	for _, index := range coll.SecondaryIndexes.All {
		index.State = schema.INDEX_ACTIVE
	}

	tx, err = runner.txMgr.StartTx(ctx)
	if err != nil {
		return Response{}, ctx, err
	}

	if err = tenant.UpdateCollectionIndexes(ctx, tx, db, coll.Name, coll.SecondaryIndexes.All); ulog.E(err) {
		return Response{}, ctx, err
	}

	if err = tx.Commit(ctx); ulog.E(err) {
		return Response{}, ctx, err
	}

	runner.queryMetrics.SetWriteType("replace")
	metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	return Response{
		Response: &api.IndexCollectionResponse{
			Indexes: runner.indexToCollectionIndex(coll.SecondaryIndexes.All),
		},
	}, ctx, nil
}
