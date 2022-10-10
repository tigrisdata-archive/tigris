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

	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

// TxListener allows listening to a transaction outcome and process the events that is passed in the param accordingly.
type TxListener interface {
	// OnPreCommit is called before committing the transaction. This means this method is useful when an operation needs
	// to guarantee transaction atomicity. Error returned by this method will roll back the user transaction.
	OnPreCommit(context.Context, *metadata.Tenant, transaction.Tx, kv.EventListener) error
	// OnPostCommit is called after committing the transaction. This means the transaction is complete and any post
	// activity can happen as part of this implementation.
	OnPostCommit(context.Context, *metadata.Tenant, kv.EventListener) error
	// OnRollback is called when the user transaction is rolled back.
	OnRollback(context.Context, *metadata.Tenant, kv.EventListener)
}

type NoopTxListener struct{}

func (l *NoopTxListener) OnPreCommit(context.Context, *metadata.Tenant, transaction.Tx, kv.EventListener) error {
	return nil
}

func (l *NoopTxListener) OnPostCommit(context.Context, *metadata.Tenant, kv.EventListener) error {
	return nil
}
func (l *NoopTxListener) OnRollback(context.Context, *metadata.Tenant, kv.EventListener) {}
