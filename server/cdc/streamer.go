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
	"bytes"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

type Streamer struct {
	db       fdb.Database
	lastKey  fdb.Key
	cfg      config.CdcConfig
	keySpace *PublisherKeySpace
	ticker   *time.Ticker
	Txs      chan Tx
}

func (s *Streamer) start() error {
	key, err := s.db.ReadTransact(func(rtx fdb.ReadTransaction) (interface{}, error) {
		kr := fdb.KeyRange{Begin: s.keySpace.beginKey, End: s.keySpace.endKey}
		r := rtx.GetRange(kr, fdb.RangeOptions{Limit: 1, Reverse: true})

		i := r.Iterator()
		if i.Advance() {
			kv, err := i.Get()
			if err != nil {
				return nil, err
			}
			return kv.Key, nil
		} else {
			return s.keySpace.beginKey, nil
		}
	})
	if err != nil {
		return err
	}

	s.lastKey = key.(fdb.Key)
	s.Txs = make(chan Tx, s.cfg.StreamBuffer)
	s.ticker = time.NewTicker(s.cfg.StreamInterval)
	go func() {
		for range s.ticker.C {
			if err := s.read(); err != nil {
				log.Err(err).Msg("read failed")
				return
			}
		}
	}()

	return nil
}

func (s *Streamer) read() error {
	_, err := s.db.ReadTransact(func(rtx fdb.ReadTransaction) (interface{}, error) {
		kr := fdb.KeyRange{Begin: s.lastKey, End: s.keySpace.endKey}
		r := rtx.GetRange(kr, fdb.RangeOptions{Limit: s.cfg.StreamBatch})

		i := r.Iterator()
		for i.Advance() {
			kv, err := i.Get()
			if err != nil {
				return nil, err
			}

			if bytes.Equal(s.lastKey, kv.Key) {
				continue
			}

			data, err := internal.Decode(kv.Value)
			if err != nil {
				return nil, err
			}

			tx := Tx{}
			err = jsoniter.Unmarshal(data.RawData, &tx)
			if err != nil {
				return nil, err
			}

			tx.Id = kv.Key

			if len(s.Txs) < cap(s.Txs) {
				s.lastKey = kv.Key
				s.Txs <- tx
			} else {
				// buffer overflow
				close(s.Txs)
				break
			}
		}

		return nil, nil
	})

	return err
}

func (s *Streamer) Close() {
	s.ticker.Stop()
}
