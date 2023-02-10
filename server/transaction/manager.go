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

package transaction

import (
	"context"
	"sync"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/kv"
)

var (
	// ErrSessionIsNotStarted is returned when the session is not started but is getting used.
	ErrSessionIsNotStarted = errors.Internal("session not started")

	// ErrSessionIsGone is returned when the session is gone but getting used.
	ErrSessionIsGone = errors.Internal("session is gone")
)

// BaseTx interface exposes base methods that can be used on a transactional object.
type BaseTx interface {
	Context() *SessionCtx
	GetTxCtx() *api.TransactionCtx
	Insert(ctx context.Context, key keys.Key, data *internal.TableData) error
	Replace(ctx context.Context, key keys.Key, data *internal.TableData, isUpdate bool) error
	Update(ctx context.Context, key keys.Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error)
	Delete(ctx context.Context, key keys.Key) error
	Read(ctx context.Context, key keys.Key) (kv.Iterator, error)
	ReadRange(ctx context.Context, lKey keys.Key, rKey keys.Key, isSnapshot bool) (kv.Iterator, error)
	Get(ctx context.Context, key []byte, isSnapshot bool) (kv.Future, error)
	SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error
	SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error
	AtomicAdd(ctx context.Context, key keys.Key, value int64) error
	AtomicRead(ctx context.Context, key keys.Key) (int64, error)
	AtomicReadRange(ctx context.Context, lKey keys.Key, rKey keys.Key, isSnapshot bool) (kv.AtomicIterator, error)
}

type Tx interface {
	BaseTx

	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// SessionCtx is used to store any baggage for the lifetime of the transaction. We use it to stage the database inside
// a transaction when the transaction is performing any DDLs.
type SessionCtx struct {
	db interface{}
}

func (c *SessionCtx) StageDatabase(db interface{}) {
	c.db = db
}

func (c *SessionCtx) GetStagedDatabase() interface{} {
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

// StartTx starts a new read-write tx session.
func (m *Manager) StartTx(ctx context.Context) (Tx, error) {
	session, err := newTxSession(m.kvStore)
	if err != nil {
		return nil, errors.Internal("issue creating a session %v", err)
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

func newTxSession(kv kv.KeyValueStore) (*TxSession, error) {
	if kv == nil {
		return nil, errors.Internal("session needs non-nil kv object")
	}
	return &TxSession{
		context: &SessionCtx{},
		kvStore: kv,
		state:   sessionCreated,
		txCtx:   generateTransactionCtx(),
	}, nil
}

func (s *TxSession) GetTxCtx() *api.TransactionCtx {
	return s.txCtx
}

func (s *TxSession) start(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	if s.state != sessionCreated {
		return errors.Internal("session state is misused")
	}

	var err error
	if s.kTx, err = s.kvStore.BeginTx(ctx); err != nil {
		return err
	}
	s.state = sessionActive

	return nil
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

func (s *TxSession) Replace(ctx context.Context, key keys.Key, data *internal.TableData, isUpdate bool) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return err
	}

	return s.kTx.Replace(ctx, key.Table(), kv.BuildKey(key.IndexParts()...), data, isUpdate)
}

func (s *TxSession) Update(ctx context.Context, key keys.Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return -1, err
	}

	return s.kTx.Update(ctx, key.Table(), kv.BuildKey(key.IndexParts()...), apply)
}

func (s *TxSession) Delete(ctx context.Context, key keys.Key) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return err
	}

	return s.kTx.Delete(ctx, key.Table(), kv.BuildKey(key.IndexParts()...))
}

func (s *TxSession) Read(ctx context.Context, key keys.Key) (kv.Iterator, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil, err
	}

	return s.kTx.Read(ctx, key.Table(), kv.BuildKey(key.IndexParts()...))
}

func (s *TxSession) ReadRange(ctx context.Context, lKey keys.Key, rKey keys.Key, isSnapshot bool) (kv.Iterator, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil, err
	}

	if rKey != nil && lKey != nil {
		return s.kTx.ReadRange(ctx, lKey.Table(), kv.BuildKey(lKey.IndexParts()...), kv.BuildKey(rKey.IndexParts()...), isSnapshot)
	} else if lKey != nil {
		return s.kTx.ReadRange(ctx, lKey.Table(), kv.BuildKey(lKey.IndexParts()...), nil, isSnapshot)
	}

	return s.kTx.ReadRange(ctx, rKey.Table(), nil, kv.BuildKey(rKey.IndexParts()...), isSnapshot)
}

func (s *TxSession) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return err
	}

	return s.kTx.SetVersionstampedValue(ctx, key, value)
}

func (s *TxSession) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return err
	}

	return s.kTx.SetVersionstampedKey(ctx, key, value)
}

func (s *TxSession) AtomicAdd(ctx context.Context, key keys.Key, value int64) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return err
	}

	return s.kTx.AtomicAdd(ctx, key.Table(), kv.BuildKey(key.IndexParts()...), value)
}

func (s *TxSession) AtomicRead(ctx context.Context, key keys.Key) (int64, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return 0, err
	}

	return s.kTx.AtomicRead(ctx, key.Table(), kv.BuildKey(key.IndexParts()...))
}

func (s *TxSession) AtomicReadRange(ctx context.Context, lKey keys.Key, rKey keys.Key, isSnapshot bool) (kv.AtomicIterator, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil, err
	}

	if rKey != nil && lKey != nil {
		return s.kTx.AtomicReadRange(ctx, lKey.Table(), kv.BuildKey(lKey.IndexParts()...), kv.BuildKey(rKey.IndexParts()...), isSnapshot)
	} else if lKey != nil {
		return s.kTx.AtomicReadRange(ctx, lKey.Table(), kv.BuildKey(lKey.IndexParts()...), nil, isSnapshot)
	}

	return s.kTx.AtomicReadRange(ctx, rKey.Table(), nil, kv.BuildKey(rKey.IndexParts()...), isSnapshot)
}

func (s *TxSession) Get(ctx context.Context, key []byte, isSnapshot bool) (kv.Future, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil, err
	}

	return s.kTx.Get(ctx, key, isSnapshot)
}

func (s *TxSession) Commit(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	s.state = sessionEnded

	err := s.kTx.Commit(ctx)

	s.kTx = nil
	return err
}

func (s *TxSession) Rollback(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	if s.kTx == nil {
		// already committed, no-op
		return nil
	}
	s.state = sessionEnded

	err := s.kTx.Rollback(ctx)

	s.kTx = nil
	return err
}

func (s *TxSession) Context() *SessionCtx {
	return s.context
}

func generateTransactionCtx() *api.TransactionCtx {
	return &api.TransactionCtx{
		Id:     uuid.New().String(),
		Origin: types.MyOrigin,
	}
}
