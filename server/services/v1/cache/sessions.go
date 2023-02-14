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

package cache

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
)

type Session interface {
	// Execute executes the request using the query runner
	Execute(ctx context.Context, runner Runner) (Response, error)

	// TxExecute executes in a fdb transaction.
	// Metadata of caches are stored in fdb as part of project metadata and that modification is a transactional operation.
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
		versionH:      tenantMgr.GetVersionHandler(),
		tenantTracker: tenantTracker,
	}
}

func (sessMgr *SessionManager) Execute(ctx context.Context, runner Runner) (Response, error) {
	namespaceForThisSession, err := request.GetNamespace(ctx)
	if err != nil {
		return Response{}, err
	}
	tenant, err := sessMgr.tenantMgr.GetTenant(ctx, namespaceForThisSession)
	if err != nil {
		log.Warn().Err(err).Msgf("Could not find tenant, this must not happen with right authn/authz configured")
		return Response{}, errors.NotFound("Tenant %s not found", namespaceForThisSession)
	}

	// check and reload caches
	if _, err = sessMgr.tenantTracker.InstantTracking(ctx, nil, tenant); err != nil {
		return Response{}, err
	}
	return runner.Run(ctx, tenant)
}

func (sessMgr *SessionManager) TxExecute(ctx context.Context, runner TxRunner) (Response, error) {
	namespaceForThisSession, err := request.GetNamespace(ctx)
	if err != nil {
		return Response{}, err
	}
	tenant, err := sessMgr.tenantMgr.GetTenant(ctx, namespaceForThisSession)
	if err != nil {
		log.Warn().Err(err).Msgf("Could not find tenant, this must not happen with right authn/authz configured")
		return Response{}, errors.NotFound("Tenant %s not found", namespaceForThisSession)
	}

	// check and reload caches
	// TODO: optimize it
	if _, err = sessMgr.tenantTracker.InstantTracking(ctx, nil, tenant); err != nil {
		return Response{}, err
	}

	tx, err := sessMgr.txMgr.StartTx(ctx)
	if err != nil {
		log.Warn().Err(err).Msgf("Could not begin transaction")
		return Response{}, errors.Internal("Failed to perform transaction")
	}

	resp, ctx, err := runner.Run(ctx, tx, tenant)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to run runner in transaction")
		_ = tx.Rollback(ctx)
		return Response{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Response{}, errors.Internal("Failed to run runner in transaction, failed to commit")
	}
	return resp, nil
}
