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

package cdc

import (
	"context"
	"sync"

	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/store/kv"
)

type Manager struct {
	sync.RWMutex

	pubs map[string]*Publisher
}

func NewManager() *Manager {
	return &Manager{
		pubs: make(map[string]*Publisher),
	}
}

func (m *Manager) GetPublisher(dbName string) *Publisher {
	m.Lock()
	defer m.Unlock()

	if m.pubs[dbName] == nil {
		m.pubs[dbName] = NewPublisher(dbName)
	}
	return m.pubs[dbName]
}

func (m *Manager) WrapContext(ctx context.Context) context.Context {
	l := kv.Listener(nil)

	if config.DefaultConfig.Cdc.Enabled {
		l = &TxListener{tx: &Tx{}}
	} else {
		l = &kv.NoListener{}
	}

	return context.WithValue(ctx, kv.ListenerCtxKey{}, l)
}

func (m *Manager) SetDatabaseName(ctx context.Context, dbName string) {
	l, ok := kv.GetListener(ctx).(*TxListener)
	if ok {
		l.SetPublisher(m.GetPublisher(dbName))
	}
}
