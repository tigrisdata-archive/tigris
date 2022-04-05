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

	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"google.golang.org/grpc/codes"
)

// QueryLifecycleFactory is responsible for returning queryLifecycle objects
type QueryLifecycleFactory struct {
	txMgr *transaction.Manager
}

func NewQueryLifecycleFactory(txMgr *transaction.Manager) *QueryLifecycleFactory {
	return &QueryLifecycleFactory{
		txMgr: txMgr,
	}
}

// Get will create and return a queryLifecycle object
func (f *QueryLifecycleFactory) Get() *queryLifecycle {
	return newQueryLifecycle(f.txMgr)
}

// queryLifecycle manages the lifecycle of a query that it is handling. Single place that can be used to validate
// the query, authorize the query, or to log or emit metrics related to this query.
type queryLifecycle struct {
	txMgr *transaction.Manager
}

func newQueryLifecycle(txMgr *transaction.Manager) *queryLifecycle {
	return &queryLifecycle{
		txMgr: txMgr,
	}
}

func (q *queryLifecycle) run(ctx context.Context, options *ReqOptions) (*Response, error) {
	if options == nil {
		return nil, api.Errorf(codes.Internal, "empty options")
	}
	if options.queryRunner == nil {
		return nil, api.Errorf(codes.Internal, "query runner is missing")
	}

	tx, err := q.txMgr.GetInheritedOrStartTx(ctx, options.txCtx, false)
	if err != nil {
		return nil, err
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

	resp, txErr = options.queryRunner.Run(ctx, tx)
	return resp, txErr
}
