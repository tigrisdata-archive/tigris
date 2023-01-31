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
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

type metadataSubspace struct {
	SubspaceName []byte
	Version      []byte
}

func (m *metadataSubspace) insertMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key, payload []byte) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	err := tx.Insert(ctx, key, internal.NewTableData(payload))

	log.Debug().Err(err).Str("type", string(m.SubspaceName)).Str("key", key.String()).Str("value", string(payload)).Msg("storing metadata succeed")

	return err
}

func (m *metadataSubspace) getMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key) ([]byte, error) {
	if invalidArgs != nil {
		return nil, invalidArgs
	}

	it, err := tx.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	var row kv.KeyValue
	if !it.Next(&row) {
		return nil, it.Err()
	}

	log.Debug().Str("type", string(m.SubspaceName)).Str("key", key.String()).Str("value", string(row.Data.RawData)).Msgf("reading metadata succeed")

	return row.Data.RawData, nil
}

func (m *metadataSubspace) updateMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key, payload []byte) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	_, err := tx.Update(ctx, key, func(data *internal.TableData) (*internal.TableData, error) {
		return internal.NewTableData(payload), nil
	})

	log.Debug().Err(err).Str("type", string(m.SubspaceName)).Str("key", key.String()).Str("value", string(payload)).Msg("update metadata")

	return err
}

func (m *metadataSubspace) deleteMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	err := tx.Delete(ctx, key)

	log.Debug().Err(err).Str("type", string(m.SubspaceName)).Str("key", key.String()).Msg("delete metadata")

	return err
}
