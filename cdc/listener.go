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
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigrisdb/internal"
)

type Tx struct {
	Ops []Op
}

type Op struct {
	Type  string
	Table []byte
	Key   []byte `json:",omitempty"`
	LKey  []byte `json:",omitempty"`
	RKey  []byte `json:",omitempty"`
	Data  []byte `json:",omitempty"`
}

func (tx *Tx) addOp(entry Op) {
	tx.Ops = append(tx.Ops, entry)
}

type TxListener struct {
	pub *Publisher
	tx  *Tx
}

func (l *TxListener) SetPublisher(pub *Publisher) {
	l.pub = pub
}

func (l *TxListener) OnSet(opType string, table []byte, key []byte, data []byte) {
	l.tx.addOp(Op{
		Type:  opType,
		Table: table,
		Key:   key,
		Data:  data,
	})
}

func (l *TxListener) OnClearRange(opType string, table []byte, lKey []byte, rKey []byte) {
	l.tx.addOp(Op{
		Type:  opType,
		Table: table,
		LKey:  lKey,
		RKey:  rKey,
	})
}

func (l *TxListener) OnCommit(fdbTx *fdb.Transaction) error {
	defer func() {
		l.tx.Ops = nil
	}()

	if len(l.tx.Ops) == 0 {
		return nil
	}

	json, err := jsoniter.Marshal(l.tx)
	if err != nil {
		return err
	}

	if l.pub == nil {
		return errors.New("no publisher set")
	}

	key, err := l.pub.getNextKey()
	if err != nil {
		return err
	}

	data := internal.NewTableDataWithEncoding(json, internal.JsonEncoding)
	bytes, err := internal.Encode(data)
	if err != nil {
		return err
	}

	fdbTx.SetVersionstampedKey(key, bytes)

	return nil
}

func (l *TxListener) OnCancel() {
	l.tx.Ops = nil
}
