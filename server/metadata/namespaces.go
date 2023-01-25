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
	"encoding/json"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

const (
	NamespaceSubspaceName = "namespace"
)

// NamespaceSubspace is used to store metadata about Tigris namespaces.
type NamespaceSubspace struct {
	MDNameRegistry
}

var namespaceVersion = []byte{0x01}

func NewNamespaceStore(mdNameRegistry MDNameRegistry) *NamespaceSubspace {
	return &NamespaceSubspace{
		MDNameRegistry: mdNameRegistry,
	}
}

func (n *NamespaceSubspace) InsertNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, metadataKey string, payload []byte) error {
	if err := validateNamespaceArgs(namespaceId, metadataKey, payload); err != nil {
		return err
	}
	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId), []byte(metadataKey))
	if err := tx.Insert(ctx, key, internal.NewTableData(payload)); err != nil {
		log.Debug().Str("key", key.String()).Str("value", string(payload)).Err(err).Msg("storing namespace metadata failed")
		return err
	}

	log.Debug().Str("key", key.String()).Str("value", string(payload)).Msg("storing namespace metadata succeed")

	return nil
}

func (n *NamespaceSubspace) InsertProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, projName string, projMetadata *ProjectMetadata) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}
	if projName == "" {
		return errors.InvalidArgument("invalid projName, projName must not be blank")
	}
	if projMetadata == nil {
		return errors.InvalidArgument("invalid projMetadata, projMetadata must not be nil")
	}

	payload, err := json.Marshal(projMetadata)
	if err != nil {
		log.Err(err).Msg("Failed to marshal db metadata")
		return errors.Internal("Failed to update db metadata, failed to marshal db metadata")
	}
	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId), dbKey, projName)

	if err := tx.Insert(ctx, key, internal.NewTableData(payload)); err != nil {
		log.Debug().Str("key", key.String()).Str("value", string(payload)).Err(err).Msg("storing namespace metadata failed")
		return err
	}

	log.Debug().Str("key", key.String()).Str("value", string(payload)).Msg("storing namespace metadata succeed")
	return nil
}

func (n *NamespaceSubspace) GetProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, projName string) (*ProjectMetadata, error) {
	if namespaceId < 1 {
		return nil, errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}
	if projName == "" {
		return nil, errors.InvalidArgument("invalid projName, projName must not be blank")
	}

	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId), dbKey, projName)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	var row kv.KeyValue
	if it.Next(&row) {
		log.Debug().Str("key", key.String()).Str("value", string(row.Data.RawData)).Msg("reading namespace metadata succeed")
		var projMetadata ProjectMetadata
		if err = json.Unmarshal(row.Data.RawData, &projMetadata); err != nil {
			log.Err(err).Msg("Failed to read db metadata")
			return nil, errors.Internal("Failed to read database metadata")
		}
		return &projMetadata, nil
	}
	return nil, it.Err()
}

func (n *NamespaceSubspace) UpdateProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, projName string, projMetadata *ProjectMetadata) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}
	if projName == "" {
		return errors.InvalidArgument("invalid projName, projName must not be blank")
	}
	if projMetadata == nil {
		return errors.InvalidArgument("invalid projMetadata, projMetadata must not be nil")
	}

	payload, err := json.Marshal(projMetadata)
	if err != nil {
		log.Err(err).Msg("Failed to marshal db metadata")
		return errors.Internal("Failed to update db metadata, failed to marshal db metadata")
	}
	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId), dbKey, projName)

	_, err = tx.Update(ctx, key, func(data *internal.TableData) (*internal.TableData, error) {
		return internal.NewTableData(payload), nil
	})
	if err != nil {
		log.Warn().Str("key", key.String()).Str("value", string(payload)).Err(err).Msg("Updating project metadata failed")
		return err
	}

	log.Debug().Str("key", key.String()).Str("value", string(payload)).Msg("Updating db metadata succeed")
	return nil
}

func (n *NamespaceSubspace) DeleteProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, projName string) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}
	if projName == "" {
		return errors.InvalidArgument("invalid projName, projName must not be blank")
	}
	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId), dbKey, projName)
	err := tx.Delete(ctx, key)
	if err != nil {
		log.Debug().Str("key", key.String()).Err(err).Msg("Delete database metadata failed")
		return err
	}
	log.Debug().Str("key", key.String()).Msg("Delete database metadata succeed")
	return nil
}

func (n *NamespaceSubspace) GetNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, metadataKey string) ([]byte, error) {
	if err := validateNamespaceArgsPartial1(namespaceId, metadataKey); err != nil {
		return nil, err
	}
	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId), []byte(metadataKey))
	it, err := tx.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	var row kv.KeyValue
	if it.Next(&row) {
		log.Debug().Str("key", key.String()).Str("value", string(row.Data.RawData)).Msg("reading namespace metadata succeed")
		return row.Data.RawData, nil
	}

	return nil, it.Err()
}

func (n *NamespaceSubspace) UpdateNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, metadataKey string, payload []byte) error {
	if err := validateNamespaceArgs(namespaceId, metadataKey, payload); err != nil {
		return err
	}
	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId), []byte(metadataKey))

	_, err := tx.Update(ctx, key, func(data *internal.TableData) (*internal.TableData, error) {
		return internal.NewTableData(payload), nil
	})
	if err != nil {
		return err
	}
	log.Debug().Str("key", key.String()).Str("value", string(payload)).Msg("update namespace metadata succeed")
	return nil
}

func (n *NamespaceSubspace) DeleteNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, metadataKey string) error {
	if err := validateNamespaceArgsPartial1(namespaceId, metadataKey); err != nil {
		return err
	}
	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId), []byte(metadataKey))
	err := tx.Delete(ctx, key)
	if err != nil {
		log.Debug().Str("key", key.String()).Err(err).Msg("Delete namespace metadata failed")
		return err
	}
	log.Debug().Str("key", key.String()).Msg("Delete namespace metadata  succeed")
	return nil
}

func (n *NamespaceSubspace) DeleteNamespace(ctx context.Context, tx transaction.Tx, namespaceId uint32) error {
	if err := validateNamespaceArgsPartial2(namespaceId); err != nil {
		return err
	}
	key := keys.NewKey(n.NamespaceSubspaceName(), namespaceVersion, UInt32ToByte(namespaceId))
	err := tx.Delete(ctx, key)
	if err != nil {
		log.Debug().Str("key", key.String()).Err(err).Msg("Delete namespace failed")
		return err
	}
	log.Debug().Str("key", key.String()).Msg("Delete namespace succeed")
	return nil
}

func validateNamespaceArgs(namespaceId uint32, metadataKey string, value []byte) error {
	if err := validateNamespaceArgsPartial1(namespaceId, metadataKey); err != nil {
		return err
	}

	if value == nil {
		return errors.InvalidArgument("invalid nil payload")
	}
	return nil
}

func validateNamespaceArgsPartial1(namespaceId uint32, metadataKey string) error {
	if err := validateNamespaceArgsPartial2(namespaceId); err != nil {
		return err
	}
	if metadataKey == "" {
		return errors.InvalidArgument("invalid empty metadataKey")
	}

	if metadataKey == dbKey {
		return errors.InvalidArgument("invalid metadataKey. " + dbKey + " is reserved")
	}

	if metadataKey == namespaceKey {
		return errors.InvalidArgument("invalid metadataKey. " + namespaceKey + " is reserved")
	}

	return nil
}

func validateNamespaceArgsPartial2(namespaceId uint32) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}

	return nil
}
