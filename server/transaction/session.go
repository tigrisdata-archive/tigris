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
	"os"
	"sync"

	"github.com/google/uuid"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/keys"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sessionState uint8

const (
	sessionCreated sessionState = 1
	sessionActive  sessionState = 2
	sessionEnded   sessionState = 3
)

// session wraps a transaction's state through which transactions are created and executed.
type session struct {
	sync.RWMutex

	kv    kv.KV
	kTx   kv.Tx
	state sessionState
	txCtx *api.TransactionCtx
}

func newSession(kv kv.KV) (*session, error) {
	if kv == nil {
		return nil, status.Errorf(codes.Internal, "session needs non-nil kv object")
	}
	return &session{
		kv:    kv,
		state: sessionCreated,
		txCtx: generateTransactionCtx(),
	}, nil
}

func (s *session) GetTxCtx() *api.TransactionCtx {
	return s.txCtx
}

func (s *session) SetState(state sessionState) {
	s.Lock()
	defer s.Unlock()

	if s.state == sessionEnded {
		return
	}

	s.state = state
}

func (s *session) GetState() sessionState {
	s.RLock()
	defer s.RUnlock()

	return s.state
}

func (s *session) start(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	if s.state != sessionCreated {
		return status.Errorf(codes.Internal, "session state is misused")
	}

	s.state = sessionActive
	var err error
	if s.kTx, err = s.kv.Tx(ctx); err != nil {
		return err
	}

	return nil
}

func (s *session) validateSession() error {
	if s.state == sessionEnded {
		return ErrSessionIsGone
	}
	if s.state == sessionCreated {
		return ErrSessionIsNotStarted
	}

	return nil
}

func (s *session) insert(ctx context.Context, key keys.Key, value []byte) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil
	}

	return s.kTx.Insert(ctx, key.Prefix(), kv.BuildKey(key.PrimaryKeys()...), value)
}

func (s *session) replace(ctx context.Context, key keys.Key, value []byte) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil
	}

	return s.kTx.Replace(ctx, key.Prefix(), kv.BuildKey(key.PrimaryKeys()...), value)
}

func (s *session) update(ctx context.Context, key keys.Key, value []byte) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil
	}

	return s.kTx.UpdateRange(ctx, key.Prefix(), kv.BuildKey(key.PrimaryKeys()...), kv.BuildKey(key.PrimaryKeys()...), nil)
}

func (s *session) delete(ctx context.Context, key keys.Key) error {
	s.Lock()
	defer s.Unlock()

	if err := s.validateSession(); err != nil {
		return nil
	}

	return s.kTx.Delete(ctx, key.Prefix(), kv.BuildKey(key.PrimaryKeys()...))
}

func (s *session) commit(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	s.state = sessionEnded

	err := s.kTx.Commit(ctx)

	s.kTx = nil
	return err
}

func (s *session) rollback(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	s.state = sessionEnded

	err := s.kTx.Rollback(ctx)

	s.kTx = nil
	return err
}

func generateTransactionCtx() *api.TransactionCtx {
	origin, _ := os.Hostname() // not necessarily it needs to be hostname, something sticky for routing
	return &api.TransactionCtx{
		Id:     uuid.New().String(),
		Origin: origin,
	}
}

// sessionTracker is used to track sessions
type sessionTracker struct {
	sync.RWMutex

	sessions map[string]*session
}

func newSessionTracker() *sessionTracker {
	return &sessionTracker{
		sessions: make(map[string]*session),
	}
}

func (tracker *sessionTracker) GetSession(id string) *session {
	tracker.RLock()
	defer tracker.RUnlock()

	return tracker.sessions[id]
}

func (tracker *sessionTracker) RemoveSession(id string) {
	tracker.Lock()
	defer tracker.Unlock()

	delete(tracker.sessions, id)
}

func (tracker *sessionTracker) PutSession(id string, session *session) {
	tracker.Lock()
	defer tracker.Unlock()

	tracker.sessions[id] = session
}
