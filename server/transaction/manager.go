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
	"sync"

	api "github.com/tigrisdata/tigrisdb/api/server/v1"
)

type Manager struct {
	sync.RWMutex

	sessions map[string]*Session
}

func NewTransactionMgr() *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
	}
}

func (m *Manager) StartTxn() (*api.Transaction, error) {
	m.Lock()
	defer m.Unlock()

	session, err := NewSession()
	if err != nil {
		return nil, err
	}
	m.sessions[session.Txn.Id] = session
	return session.Txn, nil
}

func (m *Manager) GetSession(id string) *Session {
	m.RLock()
	defer m.RUnlock()

	return m.sessions[id]
}

func (m *Manager) EndTxn(id string, commit bool) error {
	s := m.GetSession(id)

	m.Lock()
	defer m.Unlock()
	if s != nil {
		if commit {
			return s.Commit()
		} else {
			return s.Rollback()
		}
	}

	return nil
}
