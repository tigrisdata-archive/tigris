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

package kv

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const (
	InsertType      = "insert"
	ReplaceType     = "replace"
	UpdateType      = "update"
	UpdateRangeType = "updateRange"
	DeleteType      = "delete"
	DeleteRangeType = "deleteRange"
)

type ListenerCtxKey struct{}

type Listener interface {
	OnSet(opType string, table []byte, key []byte, data []byte)
	OnClearRange(opType string, table []byte, lKey []byte, rKey []byte)
	OnCommit(tx *fdb.Transaction) error
	OnCancel()
}

type NoListener struct{}

func (l *NoListener) OnSet(string, []byte, []byte, []byte) {
}

func (l *NoListener) OnClearRange(string, []byte, []byte, []byte) {
}

func (l *NoListener) OnCommit(*fdb.Transaction) error {
	return nil
}

func (l *NoListener) OnCancel() {
}

func GetListener(ctx context.Context) Listener {
	l, ok := ctx.Value(ListenerCtxKey{}).(Listener)
	if l != nil && ok {
		return l
	} else {
		return &NoListener{}
	}
}
