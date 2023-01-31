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

// CollectionSubspace is used to store metadata about Tigris collections.
type CollectionSubspace struct {
	metadataSubspace
}

// CollectionMetadata contains collection wide metadata.
type CollectionMetadata struct {
	OldestSchemaVersion int32
}

var collectionVersion = []byte{0x01}

func NewCollectionStore(nameRegistry *NameRegistry) *CollectionSubspace {
	return &CollectionSubspace{
		metadataSubspace{
			SubspaceName: nameRegistry.CollectionSubspaceName(),
			Version:      collectionVersion,
		},
	}
}

func (c *CollectionSubspace) getKey(nsID uint32, dbID uint32, collID uint32) keys.Key {
	return keys.NewKey(c.SubspaceName, schVersion, UInt32ToByte(nsID), UInt32ToByte(dbID), UInt32ToByte(collID), keyEnd)
}

func (c *CollectionSubspace) Insert(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32, metadata *CollectionMetadata) error {
	if err := c.validateArgs(nsID, dbID, collID, &metadata); err != nil {
		return err
	}

	payload, err := jsoniter.Marshal(metadata)
	if ulog.E(err) {
		return errors.Internal("failed to marshal collection metadata")
	}

	return c.insertMetadata(ctx, tx,
		nil,
		c.getKey(nsID, dbID, collID),
		payload,
	)
}

func (c *CollectionSubspace) Get(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32) (*CollectionMetadata, error) {
	payload, err := c.getMetadata(ctx, tx,
		c.validateArgs(nsID, dbID, collID, nil),
		c.getKey(nsID, dbID, collID),
	)
	if err != nil {
		return nil, err
	}

	if payload == nil {
		return nil, nil
	}

	var metadata CollectionMetadata
	if err = jsoniter.Unmarshal(payload, &metadata); ulog.E(err) {
		return nil, errors.Internal("failed to unmarshal collection metadata")
	}

	return &metadata, nil
}

func (c *CollectionSubspace) Update(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32, metadata *CollectionMetadata) error {
	if err := c.validateArgs(nsID, dbID, collID, &metadata); err != nil {
		return err
	}

	payload, err := jsoniter.Marshal(metadata)
	if ulog.E(err) {
		return errors.Internal("failed to marshal collection metadata")
	}

	return c.updateMetadata(ctx, tx,
		nil,
		c.getKey(nsID, dbID, collID),
		payload,
	)
}

func (c *CollectionSubspace) Delete(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32) error {
	return c.deleteMetadata(ctx, tx,
		c.validateArgs(nsID, dbID, collID, nil),
		c.getKey(nsID, dbID, collID),
	)
}

func (u *CollectionSubspace) validateArgs(nsID uint32, dbID uint32, collID uint32, metadata **CollectionMetadata) error {
	if nsID == 0 || dbID == 0 || collID == 0 {
		return errors.InvalidArgument("invalid id")
	}

	if metadata != nil && *metadata == nil {
		return errors.InvalidArgument("invalid nil payload")
	}

	return nil
}
