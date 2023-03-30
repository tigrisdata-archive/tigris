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
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/middleware"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// SessionManager is used to manage all the explicit query sessions. The execute method is executing the query.
// The method uses the txCtx to understand whether the query is already started(explicit transaction) if not then it
// will create a QuerySession and then will execute the query. For explicit transaction, Begin/Commit/Rollback is
// creating/storing/removing the QuerySession.

type Session interface {
	Create(ctx context.Context, trackVerInOwnTxn bool, instantVerTracking bool, track bool) (*QuerySession, error)
	Get(ctx context.Context) (*QuerySession, error)
	Remove(ctx context.Context) error
	ReadOnlyExecute(ctx context.Context, runner ReadOnlyQueryRunner, req ReqOptions) (Response, error)
	Execute(ctx context.Context, runner QueryRunner, req ReqOptions) (Response, error)
	executeWithRetry(ctx context.Context, runner QueryRunner, req ReqOptions) (resp Response, err error)
}

type SessionManager struct {
	sync.RWMutex

	txMgr         *transaction.Manager
	tenantMgr     *metadata.TenantManager
	versionH      *metadata.VersionHandler
	tracker       *sessionTracker
	tenantTracker *metadata.CacheTracker
	txListeners   []TxListener
}

type SessionManagerWithMetrics struct {
	s *SessionManager
}

func (m *SessionManagerWithMetrics) measure(ctx context.Context, name string, f func(ctx context.Context) error) {
	measurement := metrics.NewMeasurement(metrics.SessionManagerServiceName, name, metrics.SessionSpanType, metrics.GetSessionTags(name))
	ctx = measurement.StartTracing(ctx, true)
	if err := f(ctx); err != nil {
		measurement.CountErrorForScope(metrics.SessionErrorCount, measurement.GetSessionErrorTags(err))
		_ = measurement.FinishWithError(ctx, err)
		measurement.RecordDuration(metrics.SessionErrorRespTime, measurement.GetSessionErrorTags(err))
		return
	}
	measurement.CountOkForScope(metrics.SessionOkCount, measurement.GetSessionOkTags())
	_ = measurement.FinishTracing(ctx)
	measurement.RecordDuration(metrics.SessionRespTime, measurement.GetSessionOkTags())
}

func (m *SessionManagerWithMetrics) Create(ctx context.Context, trackVerInOwnTxn bool, instantVerTracking bool, track bool) (qs *QuerySession, err error) {
	m.measure(ctx, "Create", func(ctx context.Context) error {
		qs, err = m.s.Create(ctx, trackVerInOwnTxn, instantVerTracking, track)
		return createApiError(err)
	})
	return
}

func (m *SessionManagerWithMetrics) Get(ctx context.Context) (qs *QuerySession, err error) {
	// Very cheap in-memory operation, not measuring it to avoid overhead
	return m.s.Get(ctx)
}

func (m *SessionManagerWithMetrics) Remove(ctx context.Context) (err error) {
	// Very cheap in-memory operation, not measuring it to avoid overhead
	return m.s.Remove(ctx)
}

func (m *SessionManagerWithMetrics) ReadOnlyExecute(ctx context.Context, runner ReadOnlyQueryRunner, req ReqOptions) (resp Response, err error) {
	m.measure(ctx, "ReadOnlyExecute", func(ctx context.Context) error {
		resp, err = m.s.ReadOnlyExecute(ctx, runner, req)
		return err
	})
	return
}

func (m *SessionManagerWithMetrics) Execute(ctx context.Context, runner QueryRunner, req ReqOptions) (resp Response, err error) {
	m.measure(ctx, "Execute", func(ctx context.Context) error {
		resp, err = m.s.Execute(ctx, runner, req)
		return err
	})
	return
}

func (m *SessionManagerWithMetrics) executeWithRetry(ctx context.Context, runner QueryRunner, req ReqOptions) (resp Response, err error) {
	m.measure(ctx, "executeWithRetry", func(ctx context.Context) error {
		resp, err = m.s.executeWithRetry(ctx, runner, req)
		return createApiError(err)
	})
	return
}

func NewSessionManager(txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, listeners []TxListener, tenantTracker *metadata.CacheTracker) *SessionManager {
	return &SessionManager{
		txMgr:         txMgr,
		tenantMgr:     tenantMgr,
		versionH:      tenantMgr.GetVersionHandler(),
		tracker:       newSessionTracker(),
		txListeners:   listeners,
		tenantTracker: tenantTracker,
	}
}

func NewSessionManagerWithMetrics(txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, listeners []TxListener, tenantTracker *metadata.CacheTracker) *SessionManagerWithMetrics {
	return &SessionManagerWithMetrics{
		&SessionManager{
			txMgr:         txMgr,
			tenantMgr:     tenantMgr,
			versionH:      tenantMgr.GetVersionHandler(),
			tracker:       newSessionTracker(),
			txListeners:   listeners,
			tenantTracker: tenantTracker,
		},
	}
}

func (sessMgr *SessionManager) CreateReadOnlySession(ctx context.Context) (*ReadOnlySession, error) {
	namespaceForThisSession, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, err
	}
	tenant, err := sessMgr.tenantMgr.GetTenant(ctx, namespaceForThisSession)
	if err != nil {
		log.Warn().Err(err).Msgf("Could not find tenant, this must not happen with right authn/authz configured")
		return nil, errors.NotFound("Tenant %s not found", namespaceForThisSession)
	}

	tx, err := sessMgr.txMgr.StartTx(ctx)
	if err != nil {
		return nil, err
	}

	if _, err = sessMgr.tenantTracker.InstantTracking(ctx, tx, tenant); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	_ = tx.Commit(ctx)

	return &ReadOnlySession{
		ctx:    ctx,
		tenant: tenant,
	}, nil
}

// Create returns the QuerySession after creating all the necessary elements that a query execution needs.
// It first creates or get a tenant, read the metadata version and based on that reload the tenant cache and then finally
// create a transaction which will be used to execute all the query in this session.
func (sessMgr *SessionManager) Create(ctx context.Context, trackVerInOwnTxn bool, instantVerTracking bool, track bool) (*QuerySession, error) {
	namespaceForThisSession, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, err
	}
	tenant, err := sessMgr.tenantMgr.GetTenant(ctx, namespaceForThisSession)
	if err != nil {
		log.Warn().Err(err).Msgf("Could not find tenant, this must not happen with right authn/authz configured")
		return nil, errors.NotFound("Tenant %s not found", namespaceForThisSession)
	}

	tx, err := sessMgr.txMgr.StartTx(ctx)
	if err != nil {
		return nil, err
	}

	var versionTracker *metadata.Tracker
	if instantVerTracking {
		// InstantTracking will automatically reload if needed
		if trackVerInOwnTxn {
			versionTracker, err = sessMgr.tenantTracker.InstantTracking(ctx, nil, tenant)
		} else {
			versionTracker, err = sessMgr.tenantTracker.InstantTracking(ctx, tx, tenant)
		}
	} else {
		versionTracker, err = sessMgr.tenantTracker.DeferredTracking(ctx, tx, tenant)
	}
	if err != nil {
		return nil, err
	}
	txCtx := tx.GetTxCtx()
	sessCtx, cancel := context.WithCancel(ctx)
	sessCtx = kv.WrapEventListenerCtx(sessCtx)

	q := &QuerySession{
		tx:             tx,
		ctx:            sessCtx,
		cancel:         cancel,
		txCtx:          txCtx,
		tenant:         tenant,
		versionTracker: versionTracker,
		txListeners:    sessMgr.txListeners,
		tenantTracker:  sessMgr.tenantTracker,
	}
	if track {
		sessMgr.tracker.add(txCtx.Id, q)
	}

	return q, nil
}

func (sessMgr *SessionManager) Get(ctx context.Context) (*QuerySession, error) {
	txCtx := api.GetTransaction(ctx)
	return sessMgr.tracker.get(txCtx.GetId()), nil
}

func (sessMgr *SessionManager) Remove(ctx context.Context) error {
	txCtx := api.GetTransaction(ctx)
	sessMgr.tracker.remove(txCtx.GetId())
	return nil
}

// Execute is responsible to execute a query. In a way this method is managing the lifecycle of a query. For implicit
// transaction everything is done in this method. For explicit transaction, a session may already exist, so it only
// needs to run without calling Commit/Rollback.
func (sessMgr *SessionManager) Execute(ctx context.Context, runner QueryRunner, req ReqOptions) (Response, error) {
	if req.TxCtx != nil {
		session := sessMgr.tracker.get(req.TxCtx.Id)
		if session == nil {
			return Response{}, transaction.ErrSessionIsGone
		}
		resp, ctx, err := session.Run(runner)
		session.ctx = ctx
		return resp, createApiError(err)
	}

	resp, err := sessMgr.executeWithRetry(ctx, runner, req)
	if err == kv.ErrConflictingTransaction {
		return Response{}, errors.Aborted(err.Error())
	}
	return resp, createApiError(err)
}

func (sessMgr *SessionManager) ReadOnlyExecute(ctx context.Context, runner ReadOnlyQueryRunner, _ ReqOptions) (Response, error) {
	session, err := sessMgr.CreateReadOnlySession(ctx)
	if err != nil {
		return Response{}, createApiError(err)
	}

	resp, _, err := session.Run(runner)
	return resp, createApiError(err)
}

func (sessMgr *SessionManager) executeWithRetry(ctx context.Context, runner QueryRunner, req ReqOptions) (resp Response, err error) {
	delta := time.Duration(50) * time.Millisecond
	start := time.Now()
	for {
		var session *QuerySession
		// implicit sessions doesn't need tracking
		if session, err = sessMgr.Create(ctx, req.MetadataChange, req.InstantVerTracking, false); err != nil {
			return Response{}, err
		}

		// use the same ctx assigned in the session
		resp, session.ctx, err = session.Run(runner)
		if changed, err1 := session.versionTracker.Stop(session.ctx); err1 != nil || changed {
			// other than for write request, stop will be no-op.
			_ = session.tx.Rollback(session.ctx)
			continue
		}

		err = session.Commit(sessMgr.versionH, req.MetadataChange, err)
		log.Debug().Err(err).Msg("session.commit after")
		if err != kv.ErrConflictingTransaction && !search.IsErrDuplicateFieldNames(err) {
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
			time.Sleep(time.Duration(rand.Intn(25)) * time.Millisecond) //nolint:gosec
		}
	}
}

type ReadOnlySession struct {
	ctx    context.Context
	tenant *metadata.Tenant
}

func (s *ReadOnlySession) Run(runner ReadOnlyQueryRunner) (Response, context.Context, error) {
	return runner.ReadOnly(s.ctx, s.tenant)
}

type QuerySession struct {
	tx             transaction.Tx
	ctx            context.Context
	cancel         context.CancelFunc
	txCtx          *api.TransactionCtx
	tenant         *metadata.Tenant
	versionTracker *metadata.Tracker
	txListeners    []TxListener
	tenantTracker  *metadata.CacheTracker
}

func (s *QuerySession) GetTx() transaction.Tx {
	return s.tx
}

func (s *QuerySession) GetTransactionCtx() *api.TransactionCtx {
	return s.txCtx
}

func (s *QuerySession) Run(runner QueryRunner) (Response, context.Context, error) {
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
		if err = listener.OnPreCommit(s.ctx, s.tenant, s.tx, kv.GetEventListener(s.ctx)); ulog.E(err) {
			return err
		}
	}

	if err = s.tx.Commit(s.ctx); err == nil {
		if len(s.txListeners) > 0 {
			if s.GetTx().Context().GetStagedDatabase() != nil {
				// we need to reload tenant if in a transaction there is a DML as well as DDL both.
				if _, err = s.tenantTracker.InstantTracking(s.ctx, nil, s.tenant); err != nil {
					return err
				}
			}
		}

		for _, listener := range s.txListeners {
			if err = listener.OnPostCommit(s.ctx, s.tenant, kv.GetEventListener(s.ctx)); ulog.E(err) {
				return errors.DeadlineExceeded(err.Error())
			}
		}
	}

	return err
}

// sessionTracker is used to track sessions.
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
