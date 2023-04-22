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
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/auth"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

type ProjectQueryRunner struct {
	*BaseQueryRunner

	deleteReq   *api.DeleteProjectRequest
	createReq   *api.CreateProjectRequest
	listReq     *api.ListProjectsRequest
	describeReq *api.DescribeDatabaseRequest
}

func (runner *ProjectQueryRunner) SetCreateProjectReq(create *api.CreateProjectRequest) {
	runner.createReq = create
}

func (runner *ProjectQueryRunner) SetDeleteProjectReq(d *api.DeleteProjectRequest) {
	runner.deleteReq = d
}

func (runner *ProjectQueryRunner) SetListProjectsReq(list *api.ListProjectsRequest) {
	runner.listReq = list
}

func (runner *ProjectQueryRunner) SetDescribeDatabaseReq(describe *api.DescribeDatabaseRequest) {
	runner.describeReq = describe
}

func (runner *ProjectQueryRunner) create(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	projMetadata, err := createProjectMetadata(ctx)
	if err != nil {
		return Response{}, ctx, err
	}

	err = tenant.CreateProject(ctx, tx, runner.createReq.GetProject(), projMetadata)
	if err == kv.ErrDuplicateKey {
		return Response{}, ctx, errors.AlreadyExists("project already exist")
	}

	if err != nil {
		return Response{}, ctx, err
	}

	return Response{Status: CreatedStatus}, ctx, nil
}

func (runner *ProjectQueryRunner) delete(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	exist, err := tenant.DeleteProject(ctx, tx, runner.deleteReq.GetProject())
	if err != nil {
		return Response{}, ctx, err
	}

	if !exist {
		return Response{}, ctx, errors.NotFound("project doesn't exist '%s'", runner.deleteReq.GetProject())
	}

	return Response{Status: DroppedStatus}, ctx, nil
}

func (runner *ProjectQueryRunner) list(ctx context.Context, _ transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	// listReq projects need not include any branches
	projectList := tenant.ListProjects(ctx)
	projects := make([]*api.ProjectInfo, len(projectList))
	for i, l := range projectList {
		projects[i] = &api.ProjectInfo{
			Project: l,
		}
	}

	return Response{
		Response: &api.ListProjectsResponse{
			Projects: projects,
		},
	}, ctx, nil
}

func (runner *ProjectQueryRunner) describe(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, tx, tenant, runner.describeReq.GetProject(), runner.describeReq.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		namespace = "unknown"
	}
	tenantName := tenant.GetNamespace().Metadata().Name

	collectionList := db.ListCollection()
	collections := make([]*api.CollectionDescription, len(collectionList))
	for i, c := range collectionList {
		size, err := tenant.CollectionSize(ctx, db, c)
		if err != nil {
			return Response{}, ctx, err
		}

		metrics.UpdateCollectionSizeMetrics(namespace, tenantName, db.DbName(), db.BranchName(), c.GetName(), size.StoredBytes)

		// remove indexing version from the schema before returning the response
		sch := schema.RemoveIndexingVersion(c.Schema)

		// Generate schema in the requested language format
		if runner.describeReq.SchemaFormat != "" {
			sch, err = schema.Generate(sch, runner.describeReq.SchemaFormat)
			if err != nil {
				return Response{}, ctx, err
			}
		}

		collections[i] = &api.CollectionDescription{
			Collection: c.GetName(),
			Metadata:   &api.CollectionMetadata{},
			Schema:     sch,
			Size:       size.StoredBytes,
		}
	}

	size, err := tenant.DatabaseSize(ctx, db)
	if err != nil {
		return Response{}, ctx, err
	}

	metrics.UpdateDbSizeMetrics(namespace, tenantName, db.DbName(), db.BranchName(), size.StoredBytes)

	return Response{
		Response: &api.DescribeDatabaseResponse{
			Metadata:    &api.DatabaseMetadata{},
			Collections: collections,
			Size:        size.StoredBytes,
			Branches:    tenant.ListDatabaseBranches(runner.describeReq.GetProject()),
		},
	}, ctx, nil
}

func (runner *ProjectQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	switch {
	case runner.deleteReq != nil:
		return runner.delete(ctx, tx, tenant)
	case runner.createReq != nil:
		return runner.create(ctx, tx, tenant)
	case runner.listReq != nil:
		return runner.list(ctx, tx, tenant)
	case runner.describeReq != nil:
		return runner.describe(ctx, tx, tenant)
	}

	return Response{}, ctx, errors.Unknown("unknown request path")
}

type BranchQueryRunner struct {
	*BaseQueryRunner

	createBranch *api.CreateBranchRequest
	deleteBranch *api.DeleteBranchRequest
	listBranch   *api.ListBranchesRequest
}

func (runner *BranchQueryRunner) SetCreateBranchReq(create *api.CreateBranchRequest) {
	runner.createBranch = create
}

func (runner *BranchQueryRunner) SetDeleteBranchReq(deleteBranch *api.DeleteBranchRequest) {
	runner.deleteBranch = deleteBranch
}

func (runner *BranchQueryRunner) SetListBranchReq(listBranch *api.ListBranchesRequest) {
	runner.listBranch = listBranch
}

func (runner *BranchQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	switch {
	case runner.createBranch != nil:
		dbBranch := metadata.NewDatabaseNameWithBranch(runner.createBranch.GetProject(), runner.createBranch.GetBranch())
		err := tenant.CreateBranch(ctx, tx, runner.createBranch.GetProject(), dbBranch)
		if err != nil {
			return Response{}, ctx, createApiError(err)
		}

		countDDLCreateUnit(ctx)

		return Response{
			Response: &api.CreateBranchResponse{
				Status: CreatedStatus,
			},
		}, ctx, nil
	case runner.deleteBranch != nil:
		dbBranch := metadata.NewDatabaseNameWithBranch(runner.deleteBranch.GetProject(), runner.deleteBranch.GetBranch())
		err := tenant.DeleteBranch(ctx, tx, runner.deleteBranch.GetProject(), dbBranch)
		if err != nil {
			return Response{}, ctx, createApiError(err)
		}

		countDDLDropUnit(ctx)

		return Response{
			Response: &api.DeleteBranchResponse{
				Status: DeletedStatus,
			},
		}, ctx, nil
	case runner.listBranch != nil:
		branchList := tenant.ListDatabaseBranches(runner.listBranch.GetProject())
		branches := make([]*api.BranchInfo, len(branchList))
		for i, b := range branchList {
			branches[i] = &api.BranchInfo{
				Branch: b,
			}
		}
		return Response{
			Response: &api.ListBranchesResponse{
				Branches: branches,
			},
		}, ctx, nil
	}

	return Response{}, ctx, errors.Unknown("unknown request path")
}

func createProjectMetadata(ctx context.Context) (*metadata.ProjectMetadata, error) {
	currentSub, err := auth.GetCurrentSub(ctx)
	if err != nil && config.DefaultConfig.Auth.Enabled {
		return nil, errors.Internal("Failed to createReq database metadata")
	}
	return &metadata.ProjectMetadata{
		ID:        0, // it will be set to right value later on
		Creator:   currentSub,
		CreatedAt: time.Now().Unix(),
	}, nil
}
