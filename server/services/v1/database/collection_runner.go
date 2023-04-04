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

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

type CollectionQueryRunner struct {
	*BaseQueryRunner

	dropReq           *api.DropCollectionRequest
	listReq           *api.ListCollectionsRequest
	createOrUpdateReq *api.CreateOrUpdateCollectionRequest
	describeReq       *api.DescribeCollectionRequest
}

func (runner *CollectionQueryRunner) SetCreateOrUpdateCollectionReq(create *api.CreateOrUpdateCollectionRequest) {
	runner.createOrUpdateReq = create
}

func (runner *CollectionQueryRunner) SetDropCollectionReq(drop *api.DropCollectionRequest) {
	runner.dropReq = drop
}

func (runner *CollectionQueryRunner) SetListCollectionReq(list *api.ListCollectionsRequest) {
	runner.listReq = list
}

func (runner *CollectionQueryRunner) SetDescribeCollectionReq(describe *api.DescribeCollectionRequest) {
	runner.describeReq = describe
}

func (runner *CollectionQueryRunner) drop(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, tx, tenant, runner.dropReq.GetProject(), runner.dropReq.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	if tx.Context().GetStagedDatabase() == nil {
		// do not modify the actual database object yet, just work on the clone
		db = db.Clone()
		tx.Context().StageDatabase(db)
	}

	collection, err := runner.getCollection(db, runner.dropReq.GetCollection())
	if err != nil {
		return Response{}, ctx, err
	}

	project, _ := tenant.GetProject(runner.dropReq.GetProject())
	searchIndexes := collection.SearchIndexes
	// Drop Collection will also drop the implicit search index.
	if err = tenant.DropCollection(ctx, tx, db, runner.dropReq.GetCollection()); err != nil {
		return Response{}, ctx, err
	}

	if config.DefaultConfig.Search.WriteEnabled {
		for _, searchIndex := range searchIndexes {
			// Delete all the indexes that are created by the user and is tied to this collection.
			if err = tenant.DeleteSearchIndex(ctx, tx, project, searchIndex.Name); err != nil {
				return Response{}, ctx, err
			}
			countDDLDropUnit(ctx)
		}
	}

	countDDLDropUnit(ctx)

	return Response{Status: DroppedStatus}, ctx, nil
}

func (runner *CollectionQueryRunner) createOrUpdate(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	var collectionExists bool
	req := runner.createOrUpdateReq

	db, err := runner.getDatabase(ctx, tx, tenant, req.GetProject(), req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	if db.GetCollection(req.GetCollection()) == nil {
		collectionExists = true
	}

	if !collectionExists && req.OnlyCreate {
		// check if onlyCreate is set and if set then return an error if collection already exist
		return Response{}, ctx, errors.AlreadyExists("collection already exist")
	}

	schFactory, err := schema.NewFactoryBuilder(true).Build(req.GetCollection(), req.GetSchema())
	if err != nil {
		return Response{}, ctx, err
	}

	if tx.Context().GetStagedDatabase() == nil {
		// do not modify the actual database object yet, just work on the clone
		db = db.Clone()
		tx.Context().StageDatabase(db)
	}

	if err = tenant.CreateCollection(ctx, tx, db, schFactory); err != nil {
		if err == kv.ErrDuplicateKey {
			// this simply means, concurrently CreateCollection is called,
			return Response{}, ctx, errors.Aborted("concurrent createReq collection request, aborting")
		}

		if collectionExists {
			countDDLCreateUnit(ctx)
		} else {
			countDDLUpdateUnit(ctx, true)
		}
		return Response{}, ctx, err
	}
	if collectionExists {
		countDDLCreateUnit(ctx)
	} else {
		countDDLUpdateUnit(ctx, true)
	}
	return Response{Status: CreatedStatus}, ctx, nil
}

func (runner *CollectionQueryRunner) list(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, tx, tenant, runner.listReq.GetProject(), runner.listReq.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	collectionList := db.ListCollection()
	collections := make([]*api.CollectionInfo, len(collectionList))
	for i, c := range collectionList {
		collections[i] = &api.CollectionInfo{
			Collection: c.GetName(),
		}
	}

	return Response{
		Response: &api.ListCollectionsResponse{
			Collections: collections,
		},
	}, ctx, nil
}

func (runner *CollectionQueryRunner) describe(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	req := runner.describeReq
	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant,
		req.GetProject(), req.GetCollection(), req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	size, err := tenant.CollectionSize(ctx, db, coll)
	if err != nil {
		return Response{}, ctx, err
	}

	tenantName := tenant.GetNamespace().Metadata().Name

	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		namespace = "unknown"
	}

	metrics.UpdateCollectionSizeMetrics(namespace, tenantName, db.DbName(), db.BranchName(), coll.GetName(), size)
	// remove indexing version from the schema before returning the response
	sch := schema.RemoveIndexingVersion(coll.Schema)

	// Generate schema in the requested language format
	if runner.describeReq.SchemaFormat != "" {
		sch, err = schema.Generate(sch, runner.describeReq.SchemaFormat)
		if err != nil {
			return Response{}, ctx, err
		}
	}

	indexes := make([]*api.CollectionIndex, len(coll.SecondaryIndexes.All))
	for i, index := range coll.SecondaryIndexes.All {
		fields := make([]*api.Field, len(index.Fields))
		for j, field := range index.Fields {
			fields[j] = &api.Field{
				Name: field.FieldName,
			}
		}
		indexes[i] = &api.CollectionIndex{
			Name:   index.Name,
			State:  index.StateString(),
			Fields: fields,
		}
	}

	return Response{
		Response: &api.DescribeCollectionResponse{
			Collection: coll.Name,
			Metadata:   &api.CollectionMetadata{},
			Schema:     sch,
			Size:       size,
			Indexes:    indexes,
		},
	}, ctx, nil
}

func (runner *CollectionQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	switch {
	case runner.dropReq != nil:
		return runner.drop(ctx, tx, tenant)
	case runner.createOrUpdateReq != nil:
		return runner.createOrUpdate(ctx, tx, tenant)
	case runner.listReq != nil:
		return runner.list(ctx, tx, tenant)
	case runner.describeReq != nil:
		return runner.describe(ctx, tx, tenant)
	}

	return Response{}, ctx, errors.Unknown("unknown request path")
}
