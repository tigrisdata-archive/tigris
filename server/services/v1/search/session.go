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
	"sync"

	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type Session interface {
	// Execute executes the request using the query runner
	Execute(ctx context.Context, runner Runner) (Response, error)

	// TxExecute executes in a fdb transaction. This is mainly used to manage search indexes. This metadata
	// is stored in fdb as part of project metadata and that modification is a transactional operation. This API
	// is automatically bumping up the metadata version as this should only be used for metadata operation.
	TxExecute(ctx context.Context, runner TxRunner) (Response, error)
}

type SessionManager struct {
	sync.RWMutex

	txMgr         *transaction.Manager
	tenantMgr     *metadata.TenantManager
	tenantTracker *metadata.CacheTracker
	versionH      *metadata.VersionHandler
}

func NewSessionManager(txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, tenantTracker *metadata.CacheTracker) *SessionManager {
	return &SessionManager{
		txMgr:         txMgr,
		tenantMgr:     tenantMgr,
		tenantTracker: tenantTracker,
		versionH:      tenantMgr.GetVersionHandler(),
	}
}

func (sessions *SessionManager) Execute(ctx context.Context, runner Runner) (Response, error) {
	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		return Response{}, err
	}

	tenant, err := sessions.tenantMgr.GetTenant(ctx, namespace)
	if err != nil {
		return Response{}, errors.NotFound("tenant '%s' not found", namespace)
	}

	resp, err := sessions.execute(ctx, tenant, runner)
	if err != nil && shouldRecheckTenantVersion(err) {
		_ = sessions.TrackVersion(ctx, tenant)

		resp, err = sessions.execute(ctx, tenant, runner)
	}

	return resp, createApiError(err)
}

func (sessions *SessionManager) execute(ctx context.Context, tenant *metadata.Tenant, runner Runner) (Response, error) {
	return runner.Run(ctx, tenant)
}

func (sessions *SessionManager) TrackVersion(ctx context.Context, tenant *metadata.Tenant) error {
	_, err := sessions.tenantTracker.InstantTracking(ctx, nil, tenant)
	return err
}

func (sessions *SessionManager) TxExecute(ctx context.Context, runner TxRunner) (Response, error) {
	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		return Response{}, err
	}
	tenant, err := sessions.tenantMgr.GetTenant(ctx, namespace)
	if err != nil {
		return Response{}, errors.NotFound("tenant '%s' not found", namespace)
	}

	if err = sessions.TrackVersion(ctx, tenant); err != nil {
		return Response{}, err
	}

	tx, err := sessions.txMgr.StartTx(ctx)
	if err != nil {
		return Response{}, errors.Internal("failed to start transaction")
	}
	resp, err := runner.Run(ctx, tx, tenant)
	if err != nil {
		_ = tx.Rollback(ctx)
		return Response{}, createApiError(err)
	}
	if err = sessions.versionH.Increment(ctx, tx); ulog.E(err) {
		_ = tx.Rollback(ctx)
		return Response{}, err
	}

	if err = tx.Commit(ctx); err != nil {
		return Response{}, createApiError(err)
	}
	return resp, nil
}
