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
	"sync"

	"github.com/google/uuid"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

var (
	// ErrSessionIsNotStarted is returned when the session is not started but is getting used
	ErrSessionIsNotStarted = api.Errorf(api.Code_INTERNAL, "session not started")

	// ErrSessionIsGone is returned when the session is gone but getting used
	ErrSessionIsGone = api.Errorf(api.Code_INTERNAL, "session is gone")
)

// Tx interface exposes a method to execute and then other method to end the transaction. When Tx is returned at that
// point transaction is already started so no need for explicit start.
type Tx interface {
	Context() *SessionCtx
	GetTxCtx() *api.TransactionCtx
	Insert(ctx context.Context, key keys.Key, data *internal.TableData) error
	Replace(ctx context.Context, key keys.Key, data *internal.TableData) error
	Update(ctx context.Context, key keys.Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error)
	Delete(ctx context.Context, key keys.Key) error
	Read(ctx context.Context, key keys.Key) (kv.Iterator, error)
	Get(ctx context.Context, key []byte) ([]byte, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error
	SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error
	start(ctx context.Context) error
}

type StagedDB interface {
	Name() string
	GetCollection(string) *schema.DefaultCollection
}

// SessionCtx is used to store any baggage for the lifetime of the transaction. We use it to stage the database inside
// a transaction when the transaction is performing any DDLs.
type SessionCtx struct {
	db StagedDB
}

func (c *SessionCtx) StageDatabase(db StagedDB) {
	c.db = db
}

func (c *SessionCtx) GetStagedDatabase() StagedDB {
	return c.db
}

// Manager is used to track all the sessions and provide all the functionality related to transactions. Once created
// this will create a session tracker for tracking the sessions.

type Manager struct {
	kvStore kv.KeyValueStore
}

func NewManager(kvStore kv.KeyValueStore) *Manager {
	return &Manager{
		kvStore: kvStore,
	}
}

// StartTx always starts a new session and tracks the session based on the input parameter.
func (m *Manager) StartTx(ctx context.Context) (Tx, error) {
	var session Tx
	var err error
	if config.DefaultConfig.Tracing.Enabled {
		session, err = newTxSessionWithMetrics(m.kvStore)
	} else {
		session, err = newTxSession(m.kvStore)
	}
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "issue creating a session %v", err)
	}

	if err = session.start(ctx); err != nil {
		return nil, err
	}

	return session, nil
}

type sessionState uint8

const (
	sessionCreated sessionState = 1
	sessionActive  sessionState = 2
	sessionEnded   sessionState = 3
)

// TxSession is used to start an explicit transaction. Caller can control whether this transaction's session needs
// to be tracked inside session tracker. Tracker a session is useful if the object is shared across the requests
// otherwise it is not useful in the same request flow.
type TxSession struct {
	sync.RWMutex

	context *SessionCtx
	kvStore kv.KeyValueStore
	kTx     kv.Tx
	state   sessionState
	txCtx   *api.TransactionCtx
}

type TxSessionWithMetrics struct {
	t *TxSession
}

func newTxSession(kv kv.KeyValueStore) (*TxSession, error) {
	if kv == nil {
		return nil, api.Errorf(api.Code_INTERNAL, "session needs non-nil kv object")
	}
	return &TxSession{
		context: &SessionCtx{},
		kvStore: kv,
		state:   sessionCreated,
		txCtx:   generateTransactionCtx(),
	}, nil
}

func newTxSessionWithMetrics(kv kv.KeyValueStore) (*TxSessionWithMetrics, error) {
	if kv == nil {
		return nil, api.Errorf(api.Code_INTERNAL, "session needs non-nil kv object")
	}
	return &TxSessionWithMetrics{&TxSession{
		context: &SessionCtx{},
		kvStore: kv,
		state:   sessionCreated,
		txCtx:   generateTransactionCtx(),
	}}, nil
}

func (m *TxSessionWithMetrics) measure(ctx context.Context, name string, f func(ctx context.Context) error) {
	var finishTracing func()
	tags := metrics.GetFdbTags(ctx, name)
	spanMeta := metrics.NewSpanMeta(metrics.TxManagerTracingServiceName, name, "tx_manager", tags)
	ctx, finishTracing = spanMeta.StartTracing(ctx, true)
	defer finishTracing()
	// TODO: better error handling (error in trace, etc)
	ulog.E(f(ctx))
}

func (s *TxSession) GetTxCtx() *api.TransactionCtx {
	return s.txCtx
}

func (m *TxSessionWithMetrics) GetTxCtx() *api.TransactionCtx {
	return m.t.txCtx
}

func (s *TxSession) start(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	if s.state != sessionCreated {
		return api.Errorf(api.Code_INTERNAL, "session state is misused")
	}

	var err error
	if s.kTx, err = s.kvStore.BeginTx(ctx); err != nil {
		return err
	}
	s.state = sessionActive

	return nil
}

func (m *TxSessionWithMetrics) start(ctx context.Context) (err error) {
	m.measure(ctx, "start", func(ctx context.Context) error {
		err = m.t.start(ctx)
		return err
	})
	return
}

func (s *TxSession) validateSession() error {
	if s.state == sessionEnded {
		return ErrSessionIsGone
	}
	if s.state == sessionCreated {
		return ErrSessionIsNotStarted
	}

	return nil
}

func (s *TxSession) Insert(ctx context.Context, key keys.Key, data *internal.TableData) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return err
	}

	return s.kTx.Insert(ctx, key.Table(), kv.BuildKey(key.IndexParts()...), data)
}

func (m *TxSessionWithMetrics) Insert(ctx context.Context, key keys.Key, data *internal.TableData) (err error) {
	m.measure(ctx, "Insert", func(ctx context.Context) error {
		err = m.t.Insert(ctx, key, data)
		return err
	})
	return
}

func (s *TxSession) Replace(ctx context.Context, key keys.Key, data *internal.TableData) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return err
	}

	return s.kTx.Replace(ctx, key.Table(), kv.BuildKey(key.IndexParts()...), data)
}

func (m *TxSessionWithMetrics) Replace(ctx context.Context, key keys.Key, data *internal.TableData) (err error) {
	m.measure(ctx, "Replace", func(ctx context.Context) error {
		err = m.t.Replace(ctx, key, data)
		return err
	})
	return
}

func (s *TxSession) Update(ctx context.Context, key keys.Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return -1, err
	}

	return s.kTx.Update(ctx, key.Table(), kv.BuildKey(key.IndexParts()...), apply)
}

func (m *TxSessionWithMetrics) Update(ctx context.Context, key keys.Key, apply func(*internal.TableData) (*internal.TableData, error)) (encoded int32, err error) {
	m.measure(ctx, "Update", func(ctx context.Context) error {
		encoded, err = m.t.Update(ctx, key, apply)
		return err
	})
	return
}

func (s *TxSession) Delete(ctx context.Context, key keys.Key) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return err
	}

	return s.kTx.Delete(ctx, key.Table(), kv.BuildKey(key.IndexParts()...))
}

func (m *TxSessionWithMetrics) Delete(ctx context.Context, key keys.Key) (err error) {
	m.measure(ctx, "Delete", func(ctx context.Context) error {
		err = m.t.Delete(ctx, key)
		return err
	})
	return
}

func (s *TxSession) Read(ctx context.Context, key keys.Key) (kv.Iterator, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil, err
	}

	return s.kTx.Read(ctx, key.Table(), kv.BuildKey(key.IndexParts()...))
}

func (m *TxSessionWithMetrics) Read(ctx context.Context, key keys.Key) (it kv.Iterator, err error) {
	m.measure(ctx, "Read", func(ctx context.Context) error {
		it, err = m.t.Read(ctx, key)
		return err
	})
	return
}

func (s *TxSession) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil
	}

	return s.kTx.SetVersionstampedValue(ctx, key, value)
}

func (m *TxSessionWithMetrics) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) (err error) {
	m.measure(ctx, "SetVersionstampedValue", func(ctx context.Context) error {
		err = m.t.SetVersionstampedValue(ctx, key, value)
		return err
	})
	return
}

func (s *TxSession) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil
	}

	return s.kTx.SetVersionstampedKey(ctx, key, value)
}

func (m *TxSessionWithMetrics) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) (err error) {
	m.measure(ctx, "SetVersionstampedKey", func(ctx context.Context) error {
		err = m.t.SetVersionstampedKey(ctx, key, value)
		return err
	})
	return
}

func (s *TxSession) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil, err
	}

	return s.kTx.Get(ctx, key)
}

func (m *TxSessionWithMetrics) Get(ctx context.Context, key []byte) (val []byte, err error) {
	m.measure(ctx, "Get", func(ctx context.Context) error {
		val, err = m.t.Get(ctx, key)
		return err
	})
	return
}

func (s *TxSession) Commit(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	s.state = sessionEnded

	err := s.kTx.Commit(ctx)

	s.kTx = nil
	return err
}

func (m *TxSessionWithMetrics) Commit(ctx context.Context) (err error) {
	m.measure(ctx, "Commit", func(ctx context.Context) error {
		err = m.t.Commit(ctx)
		return err
	})
	return
}

func (s *TxSession) Rollback(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	s.state = sessionEnded

	err := s.kTx.Rollback(ctx)

	s.kTx = nil
	return err
}

func (m *TxSessionWithMetrics) Rollback(ctx context.Context) (err error) {
	m.measure(ctx, "Rollback", func(ctx context.Context) error {
		err = m.t.Rollback(ctx)
		return err
	})
	return
}

func (s *TxSession) Context() *SessionCtx {
	return s.context
}

func (m *TxSessionWithMetrics) Context() *SessionCtx {
	return m.t.context
}

func generateTransactionCtx() *api.TransactionCtx {
	return &api.TransactionCtx{
		Id:     uuid.New().String(),
		Origin: types.MyOrigin,
	}
}
