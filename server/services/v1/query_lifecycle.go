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

package v1

import (
	"context"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/cdc"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc/codes"
)

// QueryLifecycleFactory is responsible for returning queryLifecycle objects
type QueryLifecycleFactory struct {
	txMgr     *transaction.Manager
	tenantMgr *metadata.TenantManager
	cdcMgr    *cdc.Manager
}

func NewQueryLifecycleFactory(txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, cdcMgr *cdc.Manager) *QueryLifecycleFactory {
	return &QueryLifecycleFactory{
		txMgr:     txMgr,
		tenantMgr: tenantMgr,
		cdcMgr:    cdcMgr,
	}
}

// Get will create and return a queryLifecycle object
func (f *QueryLifecycleFactory) Get() *queryLifecycle {
	return newQueryLifecycle(f.txMgr, f.tenantMgr, f.cdcMgr)
}

// queryLifecycle manages the lifecycle of a query that it is handling. Single place that can be used to validate
// the query, authorize the query, or to log or emit metrics related to this query.
type queryLifecycle struct {
	txMgr      *transaction.Manager
	tenantMgr  *metadata.TenantManager
	cdcMgr     *cdc.Manager
	versionMgr *metadata.MetaVersionMgr
}

func newQueryLifecycle(txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, cdcMgr *cdc.Manager) *queryLifecycle {
	return &queryLifecycle{
		txMgr:      txMgr,
		tenantMgr:  tenantMgr,
		cdcMgr:     cdcMgr,
		versionMgr: &metadata.MetaVersionMgr{},
	}
}

func (q *queryLifecycle) createOrGetTenant() (*metadata.Tenant, error) {
	// ToDo: extract the namespace from the token. For now, just use the default tenant
	tenant := q.tenantMgr.GetTenant(metadata.DefaultNamespaceName)
	if tenant != nil {
		log.Debug().Str("ns", tenant.String()).Msg("tenant found")
		return tenant, nil
	}

	tx, err := q.txMgr.StartTxWithoutTracking(context.TODO())
	if ulog.E(err) {
		return nil, err
	}

	defer func() {
		if err == nil {
			err = tx.Commit(context.TODO())
		} else {
			err = tx.Rollback(context.TODO())
		}
	}()
	log.Debug().Str("tenant", metadata.DefaultNamespaceName).Msg("tenant not found, creating")
	if tenant, err = q.tenantMgr.CreateOrGetTenant(context.TODO(), tx, metadata.NewDefaultNamespace()); ulog.E(err) {
		return nil, err
	}
	return tenant, nil
}

func (q *queryLifecycle) run(ctx context.Context, options *ReqOptions) (*Response, error) {
	if options == nil {
		return nil, api.Errorf(codes.Internal, "empty options")
	}
	if options.queryRunner == nil {
		return nil, api.Errorf(codes.Internal, "query runner is missing")
	}

	// ToDo: extract the namespace from the token. For now, just use the default tenant.
	// Tenant needs to be created in its own transaction.
	tenant, err := q.createOrGetTenant()
	if err != nil {
		return nil, err
	}

	tx, err := q.txMgr.GetInheritedOrStartTx(ctx, options.txCtx, false)
	if ulog.E(err) {
		return nil, err
	}

	var resp *Response
	var txErr error
	defer func() {
		var err error
		if txErr == nil {
			if options.metadataChange {
				// metadata change will bump up the metadata version
				err = q.versionMgr.Increment(ctx, tx)
			}
			if err == nil {
				if err = tx.Commit(ctx); err == nil {
					_ = tx.Context().ExecuteCB()
				}
			} else {
				err = tx.Rollback(ctx)
			}
		} else {
			err = tx.Rollback(ctx)
		}
		if txErr == nil {
			txErr = err
		}
	}()
	if !options.metadataChange {
		if txErr = tenant.ReloadIfVersionChanged(ctx, tx); ulog.E(txErr) {
			return nil, txErr
		}
	}

	resp, ctx, txErr = options.queryRunner.Run(ctx, tx, tenant)
	return resp, txErr
}
