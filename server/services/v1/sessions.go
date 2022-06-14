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
	"github.com/tigrisdata/tigris/server/cdc"
	"github.com/tigrisdata/tigris/server/metadata"
	middleware "github.com/tigrisdata/tigris/server/midddleware"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// SessionManager is used to manage all the explicit query sessions. The execute method is executing the query.
// The method uses the txCtx to understand whether the query is already started(explicit transaction) if not then it
// will create a QuerySession and then will execute the query. For explicit transaction, Begin/Commit/Rollback is
// creating/storing/removing the QuerySession.
type SessionManager struct {
	sync.RWMutex

	txMgr       *transaction.Manager
	tenantMgr   *metadata.TenantManager
	versionH    *metadata.VersionHandler
	tracker     *sessionTracker
	searchStore search.Store
	txListeners []TxListener
}

func NewSessionManager(txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, versionH *metadata.VersionHandler, cdc *cdc.Manager, searchStore search.Store, encoder metadata.Encoder) *SessionManager {
	var txListeners []TxListener
	txListeners = append(txListeners, cdc)
	txListeners = append(txListeners, NewSearchIndexer(searchStore, encoder))

	return &SessionManager{
		txMgr:       txMgr,
		tenantMgr:   tenantMgr,
		versionH:    versionH,
		tracker:     newSessionTracker(),
		txListeners: txListeners,
		searchStore: searchStore,
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

	sessCtx, cancel := context.WithCancel(ctx)
	sessCtx = kv.WrapEventListenerCtx(sessCtx)
	q := &QuerySession{
		tx:          tx,
		ctx:         sessCtx,
		cancel:      cancel,
		txCtx:       txCtx,
		tenant:      tenant,
		version:     version,
		txListeners: sessMgr.txListeners,
	}
	if track {
		sessMgr.tracker.add(txCtx.Id, q)
	}

	return q, nil
}

func (sessMgr *SessionManager) Get(id string) *QuerySession {
	return sessMgr.tracker.get(id)
}

func (sessMgr *SessionManager) Remove(id string) {
	sessMgr.tracker.remove(id)
}

// Execute is responsible to execute a query. In a way this method is managing the lifecycle of a query. For implicit
// transaction everything is done in this method. For explicit transaction, a session may already exist, so it only
// needs to run without calling Commit/Rollback.
func (sessMgr *SessionManager) Execute(ctx context.Context, req *ReqOptions) (*Response, error) {
	if req.txCtx != nil {
		session := sessMgr.tracker.get(req.txCtx.Id)
		if session == nil {
			return nil, transaction.ErrSessionIsGone
		}
		resp, ctx, err := session.Run(req.queryRunner)
		session.ctx = ctx
		return resp, err
	}

	resp, err := sessMgr.executeWithRetry(ctx, req)
	if err == kv.ErrConflictingTransaction {
		return nil, api.Errorf(api.Code_ABORTED, err.Error())
	}
	return resp, err
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

		// use the same ctx assigned in the session
		if resp, session.ctx, err = session.Run(req.queryRunner); err != nil {
			_ = session.Rollback()
		} else {
			err = session.Commit(sessMgr.versionH, req.metadataChange, err)
		}

		if err != kv.ErrConflictingTransaction {
			return
		}

		select {
		case <-ctx.Done():
			session.cancel()
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
	tx          transaction.Tx
	ctx         context.Context
	cancel      context.CancelFunc
	txCtx       *api.TransactionCtx
	tenant      *metadata.Tenant
	version     metadata.Version
	txListeners []TxListener
}

func (s *QuerySession) Run(runner QueryRunner) (*Response, context.Context, error) {
	return runner.Run(s.ctx, s.tx, s.tenant)
}

func (s *QuerySession) Rollback() error {
	defer s.cancel()

	for _, listener := range s.txListeners {
		listener.OnRollback(s.ctx, s.tenant, kv.GetEventListener(s.ctx))
	}
	return s.tx.Rollback(s.ctx)
}

func (s *QuerySession) Commit(versionMgr *metadata.VersionHandler, incVersion bool, err error) error {
	defer s.cancel()

	if err != nil {
		_ = s.tx.Rollback(s.ctx)
		return err
	}

	if incVersion {
		// metadata change will bump up the metadata version, we are doing it in a different transaction
		// because it is not allowed to read and write the version in the same transaction
		if err = versionMgr.Increment(s.ctx, s.tx); ulog.E(err) {
			_ = s.tx.Rollback(s.ctx)
			return err
		}
	}

	for _, listener := range s.txListeners {
		if err = listener.OnPreCommit(s.ctx, s.tenant, s.tx, kv.GetEventListener(s.ctx)); err != nil {
			return err
		}
	}

	if err = s.tx.Commit(s.ctx); err == nil {
		for _, listener := range s.txListeners {
			if err = listener.OnPostCommit(s.ctx, s.tenant, kv.GetEventListener(s.ctx)); err != nil {
				log.Err(err).Msg("post commit failure")
				return api.Errorf(api.Code_DEADLINE_EXCEEDED, err.Error())
			}
		}
	}

	return err
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
