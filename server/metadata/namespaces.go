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
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// NamespaceSubspace is used to store metadata about Tigris namespaces.
type NamespaceSubspace struct {
	metadataSubspace
}

var namespaceVersion = []byte{0x01}

func NewNamespaceStore(mdNameRegistry *NameRegistry) *NamespaceSubspace {
	return &NamespaceSubspace{
		metadataSubspace{
			SubspaceName: mdNameRegistry.NamespaceSubspaceName(),
			Version:      namespaceVersion,
		},
	}
}

func (n *NamespaceSubspace) getProjKey(namespaceId uint32, projName string) keys.Key {
	return keys.NewKey(n.SubspaceName, n.Version, UInt32ToByte(namespaceId), dbKey, projName)
}

func (n *NamespaceSubspace) getNSKey(namespaceId uint32, metadataKey string) keys.Key {
	if metadataKey != "" {
		return keys.NewKey(n.SubspaceName, n.Version, UInt32ToByte(namespaceId), []byte(metadataKey))
	}

	return keys.NewKey(n.SubspaceName, n.Version, UInt32ToByte(namespaceId))
}

func (n *NamespaceSubspace) InsertProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, projName string, projMetadata *ProjectMetadata) error {
	if err := n.validateProjectArgs(namespaceId, projName, &projMetadata); err != nil {
		return err
	}

	payload, err := jsoniter.Marshal(projMetadata)
	if ulog.E(err) {
		return errors.Internal("Failed to update db metadata, failed to marshal db metadata")
	}

	return n.insertMetadata(ctx, tx, nil, n.getProjKey(namespaceId, projName), payload)
}

func (n *NamespaceSubspace) GetProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, projName string) (*ProjectMetadata, error) {
	payload, err := n.getMetadata(ctx, tx,
		n.validateProjectArgs(namespaceId, projName, nil),
		n.getProjKey(namespaceId, projName),
	)
	if err != nil {
		return nil, err
	}

	if payload == nil {
		return nil, nil
	}

	var projMetadata ProjectMetadata
	if err = jsoniter.Unmarshal(payload, &projMetadata); ulog.E(err) {
		return nil, errors.Internal("Failed to read database metadata")
	}

	return &projMetadata, nil
}

func (n *NamespaceSubspace) UpdateProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, projName string, projMetadata *ProjectMetadata) error {
	if err := n.validateProjectArgs(namespaceId, projName, &projMetadata); err != nil {
		return err
	}

	payload, err := jsoniter.Marshal(projMetadata)
	if ulog.E(err) {
		return errors.Internal("Failed to update db metadata, failed to marshal db metadata")
	}

	return n.updateMetadata(ctx, tx,
		nil,
		n.getProjKey(namespaceId, projName),
		payload,
	)
}

func (n *NamespaceSubspace) DeleteProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, projName string) error {
	return n.deleteMetadata(ctx, tx,
		n.validateProjectArgs(namespaceId, projName, nil),
		n.getProjKey(namespaceId, projName),
	)
}

func (n *NamespaceSubspace) validateProjectArgs(namespaceId uint32, projName string, metadata **ProjectMetadata) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}

	if projName == "" {
		return errors.InvalidArgument("invalid projName, projName must not be blank")
	}

	if metadata != nil && *metadata == nil {
		return errors.InvalidArgument("invalid projMetadata, projMetadata must not be nil")
	}

	return nil
}

func (n *NamespaceSubspace) InsertNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, metadataKey string, payload []byte) error {
	return n.insertMetadata(ctx, tx,
		n.validateNSArgs(namespaceId, &metadataKey, &payload),
		n.getNSKey(namespaceId, metadataKey),
		payload,
	)
}

func (n *NamespaceSubspace) GetNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, metadataKey string) ([]byte, error) {
	return n.getMetadata(ctx, tx,
		n.validateNSArgs(namespaceId, &metadataKey, nil),
		n.getNSKey(namespaceId, metadataKey),
	)
}

func (n *NamespaceSubspace) UpdateNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, metadataKey string, payload []byte) error {
	return n.updateMetadata(ctx, tx,
		n.validateNSArgs(namespaceId, &metadataKey, &payload),
		n.getNSKey(namespaceId, metadataKey),
		payload,
	)
}

func (n *NamespaceSubspace) DeleteNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, metadataKey string) error {
	return n.deleteMetadata(ctx, tx,
		n.validateNSArgs(namespaceId, &metadataKey, nil),
		n.getNSKey(namespaceId, metadataKey),
	)
}

func (n *NamespaceSubspace) DeleteNamespace(ctx context.Context, tx transaction.Tx, namespaceId uint32) error {
	return n.deleteMetadata(ctx, tx,
		n.validateNSArgs(namespaceId, nil, nil),
		n.getNSKey(namespaceId, ""),
	)
}

func (n *NamespaceSubspace) validateNSArgs(namespaceId uint32, metadataKey *string, value *[]byte) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}

	if metadataKey != nil {
		if *metadataKey == "" {
			return errors.InvalidArgument("invalid empty metadataKey")
		}

		if *metadataKey == dbKey {
			return errors.InvalidArgument("invalid metadataKey. " + dbKey + " is reserved")
		}

		if *metadataKey == namespaceKey {
			return errors.InvalidArgument("invalid metadataKey. " + namespaceKey + " is reserved")
		}
	}

	if value != nil && *value == nil {
		return errors.InvalidArgument("invalid nil payload")
	}

	return nil
}
