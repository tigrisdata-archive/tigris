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
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/internal"
)

type Tx struct {
	Id  []byte
	Ops []Op
}

type Op struct {
	Op    string
	Table []byte
	Key   []byte `json:",omitempty"`
	LKey  []byte `json:",omitempty"`
	RKey  []byte `json:",omitempty"`
	Data  []byte `json:",omitempty"`
	Last  bool
}

func (tx *Tx) addOp(entry Op) {
	tx.Ops = append(tx.Ops, entry)
}

type TxListener struct {
	keySpace *PublisherKeySpace
	tx       *Tx
}

func (l *TxListener) OnSet(op string, table []byte, key []byte, data []byte) {
	l.tx.addOp(Op{
		Op:    op,
		Table: table,
		Key:   key,
		Data:  data,
	})
}

func (l *TxListener) OnClearRange(op string, table []byte, lKey []byte, rKey []byte) {
	l.tx.addOp(Op{
		Op:    op,
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

	l.tx.Ops[len(l.tx.Ops)-1].Last = true

	json, err := jsoniter.Marshal(l.tx)
	if err != nil {
		return err
	}

	key, err := l.keySpace.getNextKey()
	if err != nil {
		return err
	}

	td := internal.NewTableDataWithEncoding(json, internal.JsonEncoding)
	enc, err := internal.Encode(td)
	if err != nil {
		return err
	}

	fdbTx.SetVersionstampedKey(key, enc)

	return nil
}

func (l *TxListener) OnCancel() {
	l.tx.Ops = nil
}
