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
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/store/kv"
)

type Publisher struct {
	keySpace *PublisherKeySpace
}

type PublisherKeySpace struct {
	cdcBytes []byte
	beginKey fdb.Key
	endKey   fdb.Key
}

func NewPublisherKeySpace(dbName string) *PublisherKeySpace {
	cdcBytes := []byte("cdc_" + dbName)
	return &PublisherKeySpace{
		cdcBytes: cdcBytes,
		beginKey: getKey(cdcBytes, [10]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
		endKey:   getKey(cdcBytes, [10]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}),
	}
}

func getKey(cdcBytes []byte, tv [10]byte) fdb.Key {
	s := subspace.FromBytes(cdcBytes)
	v := tuple.Versionstamp{TransactionVersion: tv, UserVersion: 0}
	t := []tuple.TupleElement{v}
	k := s.Pack(t)
	return k
}

func (p *PublisherKeySpace) getNextKey() (fdb.Key, error) {
	s := subspace.FromBytes(p.cdcBytes)
	v := tuple.IncompleteVersionstamp(0)
	t := []tuple.TupleElement{v}
	return s.PackWithVersionstamp(t)
}

func NewPublisher(dbName string) *Publisher {
	return &Publisher{
		keySpace: NewPublisherKeySpace(dbName),
	}
}

func (p *Publisher) NewStreamer(kvStore kv.KeyValueStore) (*Streamer, error) {
	s := Streamer{
		keySpace: p.keySpace,
		db:       kvStore.GetInternalDatabase().(fdb.Database),
		cfg:      config.DefaultConfig.Cdc,
	}

	err := s.start()
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func (p *Publisher) NewListener() kv.Listener {
	if config.DefaultConfig.Cdc.Enabled {
		return &TxListener{
			tx:       &Tx{},
			keySpace: p.keySpace,
		}
	} else {
		return &kv.NoListener{}
	}
}
