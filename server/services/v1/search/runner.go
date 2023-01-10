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

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/database"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/server/types"
)

type Runner interface {
	Run(ctx context.Context, tenant *metadata.Tenant) (Response, error)
}

type TxRunner interface {
	Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, error)
}

type RunnerFactory struct{}

// NewRunnerFactory returns RunnerFactory object.
func NewRunnerFactory() *RunnerFactory {
	return &RunnerFactory{}
}

func (f *RunnerFactory) GetIndexRunner(accessToken *types.AccessToken) *IndexRunner {
	return &IndexRunner{
		baseRunner: newBaseRunner(accessToken),
	}
}

type baseRunner struct {
	accessToken *types.AccessToken
}

func newBaseRunner(accessToken *types.AccessToken) *baseRunner {
	return &baseRunner{
		accessToken: accessToken,
	}
}

type IndexRunner struct {
	*baseRunner

	create *api.CreateOrUpdateIndexRequest
	delete *api.DeleteIndexRequest
	list   *api.ListIndexesRequest
}

func (runner *IndexRunner) SetCreateIndexReq(create *api.CreateOrUpdateIndexRequest) {
	runner.create = create
}

func (runner *IndexRunner) SetDeleteIndexReq(drop *api.DeleteIndexRequest) {
	runner.delete = drop
}

func (runner *IndexRunner) SetListIndexesReq(list *api.ListIndexesRequest) {
	runner.list = list
}

func (runner *IndexRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, error) {
	currentSub, err := request.GetCurrentSub(ctx)
	if err != nil && config.DefaultConfig.Auth.Enabled {
		return Response{}, errors.Internal("Failed to get current sub for the request")
	}

	switch {
	case runner.create != nil:
		factory, err := schema.BuildSearch(runner.create.GetName(), runner.create.GetSchema())
		if err != nil {
			return Response{}, err
		}

		project, err := tenant.GetProject(runner.create.GetProject())
		if err != nil {
			return Response{}, createApiError(err)
		}
		factory.Sub = currentSub
		if err = tenant.CreateSearchIndex(ctx, tx, project, factory); err != nil {
			return Response{}, createApiError(err)
		}

		return Response{
			Status: database.CreatedStatus,
		}, nil
	case runner.delete != nil:
		project, err := tenant.GetProject(runner.create.GetProject())
		if err != nil {
			return Response{}, createApiError(err)
		}
		if err = tenant.DeleteSearchIndex(ctx, tx, project, runner.delete.GetName()); err != nil {
			return Response{}, createApiError(err)
		}

		return Response{
			Status: database.DeletedStatus,
		}, nil
	case runner.list != nil:
		project, err := tenant.GetProject(runner.create.GetProject())
		if err != nil {
			return Response{}, createApiError(err)
		}

		var indexes []string
		if indexes, err = tenant.ListSearchIndexes(ctx, tx, project); err != nil {
			return Response{}, createApiError(err)
		}

		return Response{
			Response: &api.ListIndexesResponse{
				Indexes: indexes,
			},
		}, nil
	}

	return Response{}, nil
}
