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

package metadata

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// versionKey is the metadata version key whose value is returned to the clients in the transaction.
var versionKey = append([]byte{0xff}, []byte("/metadataVersion")...) // versionValue is the value set when calling setVersionstampedValue, any value other than this is rejected.

type (
	Version       []byte
	VersionFuture kv.Future
)

// VersionHandler is used to maintain a version for each schema change. Using this we can implement transactional DDL APIs.
// This will also be used to provide a strongly consistent Cache lookup on the schemas i.e. anytime version changes we
// know that a DDL operation is performed which means we can invalidate the cache and reload from the disk.
type VersionHandler struct {
	Key   []byte
	Value []byte
}

func newVersionHandler(key []byte) *VersionHandler {
	if len(key) == 0 {
		key = versionKey
	}

	log.Debug().Str("key", string(key)).Msg("init version handler")

	// IMPORTANT: Version stamp value must be 14 bytes long. Otherwise, FDB API call will fail.
	h := VersionHandler{Key: key, Value: make([]byte, 14)}

	return &h
}

// Increment is used to increment the metadata version.
func (m *VersionHandler) Increment(ctx context.Context, tx transaction.Tx) error {
	return tx.SetVersionstampedValue(ctx, m.Key, m.Value)
}

// Read is blocking and returns the latest metadata version.
func (m *VersionHandler) Read(ctx context.Context, tx transaction.Tx, isSnapshot bool) (Version, error) {
	vf, err := m.ReadFuture(ctx, tx, isSnapshot)
	if err != nil {
		return nil, err
	}

	return vf.Get()
}

// ReadFuture is a non-blocking API to return the future corresponding to the latest metadata version.
func (m *VersionHandler) ReadFuture(ctx context.Context, tx transaction.Tx, isSnapshot bool) (VersionFuture, error) {
	return tx.Get(ctx, m.Key, isSnapshot)
}

// ReadInOwnTxn creates a transaction and then reads the version. This is useful when a transaction is also changing
// the metadata then it is better to read the metadata version in its own transaction as the read-write-read or write-read
// metadata version is not allowed in a transaction.
func (m *VersionHandler) ReadInOwnTxn(ctx context.Context, txMgr *transaction.Manager, isSnapshot bool) (version Version, err error) {
	tx, e := txMgr.StartTx(ctx)
	if ulog.E(e) {
		return nil, e
	}

	defer func() {
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			_ = tx.Rollback(ctx)
		}
	}()

	versionF, err := m.ReadFuture(ctx, tx, isSnapshot)
	if err != nil {
		return nil, err
	}
	return versionF.Get()
}
