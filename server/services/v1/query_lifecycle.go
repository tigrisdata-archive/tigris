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
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/cdc"
	"github.com/tigrisdata/tigrisdb/server/metadata"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
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
	txMgr     *transaction.Manager
	tenantMgr *metadata.TenantManager
	cdcMgr    *cdc.Manager
}

func newQueryLifecycle(txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, cdcMgr *cdc.Manager) *queryLifecycle {
	return &queryLifecycle{
		txMgr:     txMgr,
		tenantMgr: tenantMgr,
		cdcMgr:    cdcMgr,
	}
}

func (q *queryLifecycle) run(ctx context.Context, options *ReqOptions) (*Response, error) {
	if options == nil {
		return nil, api.Errorf(codes.Internal, "empty options")
	}
	if options.queryRunner == nil {
		return nil, api.Errorf(codes.Internal, "query runner is missing")
	}

	// ToDo: extract the namespace from the token. For now, just use the default tenant
	tenant := q.tenantMgr.GetTenant(metadata.DefaultNamespaceName)
	if tenant != nil {
		log.Debug().Str("ns", tenant.String()).Msg("tenant found")
	}

	ctx = q.cdcMgr.WrapContext(ctx)

	tx, err := q.txMgr.GetInheritedOrStartTx(ctx, options.txCtx, false)
	if ulog.E(err) {
		return nil, err
	}

	if tenant == nil {
		log.Debug().Str("tenant", metadata.DefaultNamespaceName).Msg("tenant not found, creating")
		if tenant, err = q.tenantMgr.CreateOrGetTenant(ctx, tx, metadata.NewDefaultNamespace()); ulog.E(err) {
			return nil, err
		}
	}

	var resp *Response
	var txErr error
	defer func() {
		var err error
		if txErr == nil {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if txErr == nil {
			txErr = err
		}
	}()

	resp, txErr = options.queryRunner.Run(ctx, tx, tenant)
	return resp, txErr
}
