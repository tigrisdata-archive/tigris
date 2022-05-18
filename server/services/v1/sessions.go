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
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/midddleware"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc/codes"
)

// SessionManager is used to manage all the explicit query sessions. The execute method is executing the query.
// The method uses the txCtx to understand whether the query is already started(explicit transaction) if not then it
// will create a QuerySession and then will execute the query. For explicit transaction, Begin/Commit/Rollback is
// creating/storing/removing the QuerySession.
type SessionManager struct {
	sync.RWMutex

	txMgr     *transaction.Manager
	tenantMgr *metadata.TenantManager
	versionH  *metadata.VersionHandler
	tracker   *sessionTracker
}

func NewSessionManager(txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, versionH *metadata.VersionHandler) *SessionManager {
	return &SessionManager{
		txMgr:     txMgr,
		tenantMgr: tenantMgr,
		versionH:  versionH,
		tracker:   newSessionTracker(),
	}
}

// Create returns the QuerySession after creating all the necessary elements that a query execution needs.
// It first creates or get a tenant, read the metadata version and based on that reload the tenant cache and then finally
// create a transaction which will be used to execute all the query in this session.
func (sessMgr *SessionManager) Create(ctx context.Context, reloadVerOutside bool, track bool) (*QuerySession, error) {
	tenant, err := sessMgr.tenantMgr.CreateOrGetTenant(ctx, sessMgr.txMgr, metadata.NewDefaultNamespace())
	if err != nil {
		return nil, err
	}

	var version metadata.Version
	if reloadVerOutside {
		version, err = sessMgr.versionH.ReadInOwnTxn(ctx, sessMgr.txMgr)
		if err != nil {
			return nil, err
		}
	}

	tx, err := sessMgr.txMgr.StartTx(ctx)
	if err != nil {
		return nil, err
	}
	txCtx := tx.GetTxCtx()

	if reloadVerOutside {
		// use version calculated outside
		if err = tenant.ReloadUsingOutsideVersion(ctx, tx, version, txCtx.Id); ulog.E(err) {
			return nil, err
		}
	} else {
		// safe to read version in a transaction
		if err = tenant.ReloadUsingTxVersion(ctx, tx, txCtx.Id); ulog.E(err) {
			return nil, err
		}
	}

	q := &QuerySession{
		tx:      tx,
		txCtx:   txCtx,
		tenant:  tenant,
		version: version,
	}
	if track {
		sessMgr.tracker.add(txCtx.Id, q)
	}

	return &QuerySession{
		tx:      tx,
		txCtx:   txCtx,
		tenant:  tenant,
		version: version,
	}, nil
}

func (sessMgr *SessionManager) Get(id string) *QuerySession {
	return sessMgr.tracker.get(id)
}

func (sessMgr *SessionManager) Remove(id string) {
	sessMgr.tracker.remove(id)
}

// Execute is responsible to execute a query. In a way this method is managing the lifecycle of a query. For implicit
// transaction everything is done in this method. For explicit transaction, a session may already exist so it only
// needs to run without calling Commit/Rollback.
func (sessMgr *SessionManager) Execute(ctx context.Context, req *ReqOptions) (*Response, error) {
	if req.txCtx != nil {
		id := req.txCtx.Id
		session := sessMgr.tracker.get(id)
		resp, ctx, err := session.Run(ctx, req.queryRunner)
		session.ctx = ctx
		return resp, err
	} else {
		resp, err := sessMgr.executeWithRetry(ctx, req)
		if err == kv.ErrConflictingTransaction {
			return nil, api.Errorf(codes.Aborted, err.Error())
		}

		return resp, err
	}
}

func (sessMgr *SessionManager) executeWithRetry(ctx context.Context, req *ReqOptions) (resp *Response, err error) {
	delta := time.Duration(50) * time.Millisecond
	start := time.Now()
	for {
		var session *QuerySession
		// implicit sessions doesn't need tracking
		if session, err = sessMgr.Create(ctx, req.metadataChange, false); err != nil {
			return nil, err
		}

		if resp, ctx, err = session.Run(ctx, req.queryRunner); err != nil {
			_ = session.Rollback(ctx)
		} else {
			err = session.Commit(ctx, sessMgr.versionH, req.metadataChange, err)
		}

		if err != kv.ErrConflictingTransaction {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
			d, ok := ctx.Deadline()
			if ok && time.Until(d) <= delta {
				// if remaining is less than delta then probably not worth retrying
				return
			}
			if !ok && time.Since(start) > (middleware.DefaultTimeout-delta) {
				// this should not happen, adding a safeguard
				return
			}

			log.Debug().Msgf("retrying transactions id: %s, since: %v", session.txCtx.Id, time.Since(start))
			time.Sleep(time.Duration(rand.Intn(25)) * time.Millisecond)
		}
	}
}

type QuerySession struct {
	tx      transaction.Tx
	ctx     context.Context
	txCtx   *api.TransactionCtx
	tenant  *metadata.Tenant
	version metadata.Version
}

func (s *QuerySession) Run(ctx context.Context, runner QueryRunner) (*Response, context.Context, error) {
	resp, ctx, err := runner.Run(ctx, s.tx, s.tenant)
	return resp, ctx, err
}

func (s *QuerySession) Rollback(ctx context.Context) error {
	return s.tx.Rollback(ctx)
}

func (s *QuerySession) Commit(ctx context.Context, versionMgr *metadata.VersionHandler, incVersion bool, err error) error {
	if err != nil {
		_ = s.tx.Rollback(ctx)
		return err
	}

	if incVersion {
		// metadata change will bump up the metadata version, we are doing it in a different transaction
		// because it is not allowed to read and write the version in the same transaction
		if err = versionMgr.Increment(ctx, s.tx); ulog.E(err) {
			_ = s.tx.Rollback(ctx)
			return err
		}
	}

	return s.tx.Commit(ctx)
}

// sessionTracker is used to track sessions
type sessionTracker struct {
	sync.RWMutex

	sessions map[string]*QuerySession
}

func newSessionTracker() *sessionTracker {
	return &sessionTracker{
		sessions: make(map[string]*QuerySession),
	}
}

func (tracker *sessionTracker) get(id string) *QuerySession {
	tracker.RLock()
	defer tracker.RUnlock()

	return tracker.sessions[id]
}

func (tracker *sessionTracker) remove(id string) {
	tracker.Lock()
	defer tracker.Unlock()

	delete(tracker.sessions, id)
}

func (tracker *sessionTracker) add(id string, session *QuerySession) {
	tracker.Lock()
	defer tracker.Unlock()

	tracker.sessions[id] = session
}
