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

package transaction

import (
	"context"

	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/keys"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrSessionIsNotStarted is returned when the session is not started but is getting used
	ErrSessionIsNotStarted = status.Errorf(codes.Internal, "session not started")

	// ErrSessionIsGone is returned when the session is gone but getting used
	ErrSessionIsGone = status.Errorf(codes.Internal, "session is gone")

	// ErrTxCtxMissing is returned when the caller needs an existing transaction but passed a nil tx ctx object
	ErrTxCtxMissing = status.Errorf(codes.Internal, "tx ctx is missing")
)

// Tx interface exposes a method to execute and then other method to end the transaction. When Tx is returned at that
// point transaction is already started so no need for explicit start.
type Tx interface {
	Insert(ctx context.Context, key keys.Key, value []byte) error
	Replace(ctx context.Context, key keys.Key, value []byte) error
	Update(ctx context.Context, key keys.Key, value []byte) error
	Delete(ctx context.Context, key keys.Key) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Manager is used to track all the sessions and provide all the functionality related to transactions. Once created
// this will create a session tracker for tracking the sessions.
type Manager struct {
	kv      kv.KV
	tracker *sessionTracker
}

func NewManager(kv kv.KV) *Manager {
	return &Manager{
		kv:      kv,
		tracker: newSessionTracker(),
	}
}

func (m *Manager) GetKV() kv.KV {
	return m.kv
}

// StartTx always starts a new session and tracks the session based on the input parameter.
func (m *Manager) StartTx(ctx context.Context, enableTracking bool) (Tx, *api.TransactionCtx, error) {
	session, err := newSession(m.kv)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "issue creating a session %v", err)
	}

	if err = session.start(ctx); err != nil {
		return nil, nil, err
	}

	if enableTracking {
		m.tracker.PutSession(session.txCtx.Id, session)
	}

	return NewTxExplicit(session, m.tracker, enableTracking), session.GetTxCtx(), nil
}

// GetTx will return an explicit transaction that is getting tracked. It is called mainly when the caller wants to
// change the state of existing session like in case of Commit/Rollback.
func (m *Manager) GetTx(txCtx *api.TransactionCtx) (Tx, error) {
	if txCtx == nil {
		return nil, ErrTxCtxMissing
	}

	session := m.tracker.GetSession(txCtx.GetId())
	if session == nil {
		return nil, ErrSessionIsGone
	}

	return NewTxExplicit(session, m.tracker, true), nil
}

// GetInherited will return only inherited transaction i.e. return only if it is tracked and caller only wants to
// execute some operation inside the existing session.
func (m *Manager) GetInherited(txCtx *api.TransactionCtx) (Tx, error) {
	if txCtx == nil {
		return nil, nil
	}

	session := m.tracker.GetSession(txCtx.GetId())
	if session == nil {
		return nil, ErrSessionIsGone
	}

	// session already exists, return inheritedTx object
	return NewTxInherited(session), nil
}

// GetInheritedOrStartTx will return either TxInherited if txCtx is not nil and the session is still with the tracker
// Or it will simply create a new explicit transaction.
func (m *Manager) GetInheritedOrStartTx(ctx context.Context, txCtx *api.TransactionCtx, enableTracking bool) (Tx, error) {
	if txCtx != nil {
		session := m.tracker.GetSession(txCtx.GetId())
		if session == nil {
			return nil, ErrSessionIsGone
		}

		// session already exists, return inheritedTx object
		return NewTxInherited(session), nil
	}

	tx, _, err := m.StartTx(ctx, enableTracking)
	return tx, err
}

// TxExplicit is used to start an explicit transaction. Caller can control whether this transaction's session needs
// to be tracked inside session tracker. Tracker a session is useful if the object is shared across the requests
// otherwise it is not useful in the same request flow.
type TxExplicit struct {
	*baseTx

	session         *session
	tracker         *sessionTracker
	trackingEnabled bool
}

// NewTxExplicit creates TxExplicit object
func NewTxExplicit(session *session, tracker *sessionTracker, trackingEnabled bool) *TxExplicit {
	return &TxExplicit{
		baseTx: &baseTx{
			session: session,
		},
		session:         session,
		tracker:         tracker,
		trackingEnabled: trackingEnabled,
	}
}

// Commit the transaction by calling commit and is also responsible for removing the session from
// the tracker
func (tx *TxExplicit) Commit(ctx context.Context) error {
	defer func() {
		if tx.trackingEnabled {
			tx.tracker.RemoveSession(tx.session.txCtx.Id)
		}
	}()

	return tx.session.commit(ctx)
}

// Rollback the transaction by calling rollback and is also responsible for removing the session from
// the tracker
func (tx *TxExplicit) Rollback(ctx context.Context) error {
	defer func() {
		if tx.trackingEnabled {
			tx.tracker.RemoveSession(tx.session.txCtx.Id)
		}
	}()

	return tx.session.rollback(ctx)
}

// TxInherited is a transaction that doesn't own the state of the session and is only used to execute operation
// in the context of a transaction which is started by some other thread.
type TxInherited struct {
	*baseTx

	session *session
}

// NewTxInherited create TxInherited object
func NewTxInherited(session *session) *TxInherited {
	return &TxInherited{
		baseTx: &baseTx{
			session: session,
		},
	}
}

// Commit is noop for TxInherited, because this object doesn't own "session" so it should not modify session's state
// and let the owner decide the outcome of the session.
func (tx *TxInherited) Commit(_ context.Context) error {
	return nil
}

func (tx *TxInherited) Rollback(_ context.Context) error {
	return nil
}

type baseTx struct {
	session *session
}

func (b *baseTx) Insert(ctx context.Context, key keys.Key, value []byte) error {
	return b.session.insert(ctx, key, value)
}

func (b *baseTx) Replace(ctx context.Context, key keys.Key, value []byte) error {
	return b.session.replace(ctx, key, value)
}

func (b *baseTx) Update(ctx context.Context, key keys.Key, value []byte) error {
	return b.session.update(ctx, key, value)
}

func (b *baseTx) Delete(ctx context.Context, key keys.Key) error {
	return b.session.delete(ctx, key)
}
