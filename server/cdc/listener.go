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

package cdc

import (
	"context"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

type Tx struct {
	Id  []byte
	Ops []*kv.Event
}

func (p *Publisher) OnCommit(ctx context.Context, tx transaction.Tx, listener kv.EventListener) error {
	events := listener.GetEvents()
	if len(events) == 0 {
		return nil
	}

	json, err := jsoniter.Marshal(&Tx{
		Ops: events,
	})
	if err != nil {
		return err
	}

	key, err := p.keySpace.getNextKey()
	if err != nil {
		return err
	}

	td := internal.NewTableDataWithEncoding(json, internal.JsonEncoding)
	enc, err := internal.Encode(td)
	if err != nil {
		return err
	}

	return tx.SetVersionstampedKey(ctx, key, enc)
}

func (p *Publisher) OnRollback(_ context.Context, _ kv.EventListener) {}
