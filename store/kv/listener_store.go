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

package kv

import (
	"context"

	"github.com/tigrisdata/tigris/internal"
)

// ListenerStore is the before any other kv layer in the chain so that event data can be pushed to the listener.
type ListenerStore struct {
	TxStore
}

func NewListenerStore(store TxStore) TxStore {
	return &ListenerStore{
		TxStore: store,
	}
}

func (store *ListenerStore) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := store.TxStore.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	return &ListenerTx{
		Tx: tx,
	}, nil
}

// ListenerTx is the tx created for ListenerStore.
type ListenerTx struct {
	Tx
}

func (tx *ListenerTx) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	listener := GetEventListener(ctx)
	listener.OnSet(InsertEvent, table, key, data)

	return tx.Tx.Insert(ctx, table, key, data)
}

func (tx *ListenerTx) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error {
	listener := GetEventListener(ctx)
	if isUpdate {
		listener.OnSet(UpdateEvent, table, key, data)
	} else {
		listener.OnSet(ReplaceEvent, table, key, data)
	}

	return tx.Tx.Replace(ctx, table, key, data, isUpdate)
}

func (tx *ListenerTx) Delete(ctx context.Context, table []byte, key Key) error {
	listener := GetEventListener(ctx)

	listener.OnClear(DeleteEvent, table, key)

	return tx.Tx.Delete(ctx, table, key)
}
