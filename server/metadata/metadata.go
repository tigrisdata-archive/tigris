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

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type metadataSubspace struct {
	SubspaceName []byte
	KeyVersion   []byte
}

//nolint:unparam
func (m *metadataSubspace) insertMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key,
	ver int32, metadata any,
) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	payload, err := jsoniter.Marshal(metadata)
	if ulog.E(err) {
		return errors.Internal("failed to marshal %s metadata", string(m.SubspaceName))
	}

	return m.insertPayload(ctx, tx, nil, key, ver, payload)
}

func (m *metadataSubspace) insertPayload(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key,
	ver int32, payload []byte,
) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	data := internal.NewTableDataWithVersion(payload, ver)

	err := tx.Insert(ctx, key, data)

	log.Debug().Err(err).Str("type", string(m.SubspaceName)).Str("key", key.String()).
		Str("value", string(payload)).Msg("storing metadata succeed")

	return err
}

func (m *metadataSubspace) getPayload(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key,
) (*internal.TableData, error) {
	if invalidArgs != nil {
		return nil, invalidArgs
	}

	it, err := tx.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	var row kv.KeyValue
	if !it.Next(&row) {
		if it.Err() != nil {
			return nil, it.Err()
		}

		return nil, errors.ErrNotFound
	}

	log.Debug().Str("type", string(m.SubspaceName)).Str("key", key.String()).
		Str("value", string(row.Data.RawData)).Msgf("reading metadata succeed")

	return row.Data, nil
}

func (m *metadataSubspace) getMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key,
	metadata any,
) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	payload, err := m.getPayload(ctx, tx, nil, key)
	if err != nil {
		return err
	}

	if payload == nil {
		return errors.ErrNotFound
	}

	if err = jsoniter.Unmarshal(payload.RawData, metadata); ulog.E(err) {
		return errors.Internal("failed to unmarshal metadata")
	}

	return nil
}

func (m *metadataSubspace) updatePayload(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key,
	ver int32, payload []byte,
) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	err := tx.Replace(ctx, key, internal.NewTableDataWithVersion(payload, ver), false)

	log.Debug().Err(err).Str("type", string(m.SubspaceName)).Str("key", key.String()).
		Str("value", string(payload)).Msg("update metadata")

	return err
}

//nolint:unparam
func (m *metadataSubspace) updateMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key,
	ver int32, metadata any,
) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	payload, err := jsoniter.Marshal(metadata)
	if ulog.E(err) {
		return errors.Internal("failed to marshal %s metadata", string(m.SubspaceName))
	}

	return m.updatePayload(ctx, tx, nil, key, ver, payload)
}

func (m *metadataSubspace) deleteMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error, key keys.Key,
) error {
	if invalidArgs != nil {
		return invalidArgs
	}

	err := tx.Delete(ctx, key)

	log.Debug().Err(err).Str("type", string(m.SubspaceName)).Str("key", key.String()).Msg("delete metadata")

	return err
}

func (m *metadataSubspace) softDeleteMetadata(ctx context.Context, tx transaction.Tx, invalidArgs error,
	fromKey keys.Key, toKey keys.Key,
) (err error) {
	defer func() {
		log.Debug().Err(err).Str("type", string(m.SubspaceName)).Str("delKey", fromKey.String()).
			Str("addKey", toKey.String()).Msg("soft delete metadata (delete)")
	}()

	if invalidArgs != nil {
		return invalidArgs
	}

	it, err1 := tx.Read(ctx, fromKey)
	if err1 != nil {
		return err1
	}

	var row kv.KeyValue
	if !it.Next(&row) {
		return it.Err()
	}

	if err = tx.Delete(ctx, fromKey); err != nil {
		return err
	}

	return tx.Replace(ctx, toKey, row.Data, false)
}

func (m *metadataSubspace) listMetadata(ctx context.Context, tx transaction.Tx, key keys.Key, keyLen int,
	fn func(dropped bool, name string, data *internal.TableData) error,
) error {
	it, err := tx.Read(ctx, key)
	if err != nil {
		return err
	}

	var v kv.KeyValue

	for it.Next(&v) {
		if len(v.Key) != keyLen {
			log.Error().Interface("key", v.Key).Str("subspace", string(m.SubspaceName)).Msg("invalid key")
			return errors.Internal("not a valid key %v", v.Key)
		}

		// format: <..., Name, keyEnd>
		end, ok := v.Key[keyLen-1].(string)
		if !ok || (end != keyEnd && end != keyDroppedEnd) {
			return errors.Internal("key trailer is missing %v", v.Key)
		}

		name, ok := v.Key[keyLen-2].(string)
		if !ok {
			return errors.Internal("name not found %T %v", v.Key[keyLen-2], v.Key[keyLen-2])
		}

		if err = fn(end == keyDroppedEnd, name, v.Data); err != nil {
			return err
		}
	}

	return it.Err()
}
