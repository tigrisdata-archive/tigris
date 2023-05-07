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
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// CollectionMetadata contains collection wide metadata.
type CollectionMetadata struct {
	ID      uint32          `json:"id,omitempty"`
	Indexes []*schema.Index `json:"indexes"`
}

// CollectionSubspace is used to store metadata about Tigris collections.
type CollectionSubspace struct {
	metadataSubspace
	queue *QueueSubspace
}

const collMetaValueVersion int32 = 1

func newCollectionStore(nameRegistry *NameRegistry, queue *QueueSubspace) *CollectionSubspace {
	return &CollectionSubspace{
		queue: queue,
		metadataSubspace: metadataSubspace{
			SubspaceName: nameRegistry.EncodingSubspaceName(),
			KeyVersion:   []byte{encKeyVersion},
		},
	}
}

func (c *CollectionSubspace) getKey(nsID uint32, dbID uint32, name string) keys.Key {
	if name == "" {
		return keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(nsID), UInt32ToByte(dbID), collectionKey)
	}

	return keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(nsID), UInt32ToByte(dbID), collectionKey, name, keyEnd)
}

func (c *CollectionSubspace) Create(ctx context.Context, tx transaction.Tx, ns Namespace, db *Database, collName string, collId uint32, indexes []*schema.Index,
) (*CollectionMetadata, error) {
	for _, index := range indexes {
		// The indexes are created when the collection is created which means we do not need to
		// do any background building, the index is already up to date and can be used for queries
		index.State = schema.INDEX_ACTIVE
	}

	meta := &CollectionMetadata{
		collId,
		indexes,
	}

	if err := c.insert(ctx, tx, ns.Id(), db.Id(), collName, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (c *CollectionSubspace) updateMetadataIndexes(ctx context.Context, tx transaction.Tx, ns Namespace, db *Database, name string, _ uint32, metadata *CollectionMetadata, updatedIndexes []*schema.Index,
) error {
	shouldAddIndexBuildTask := false
	for _, updateIdx := range updatedIndexes {
		existingIdx := schema.FindIndex(metadata.Indexes, updateIdx.Name)
		if existingIdx != nil {
			if updateIdx.State == schema.UNKNOWN {
				updateIdx.State = existingIdx.State
			}
		} else {
			updateIdx.State = schema.INDEX_WRITE_MODE
			shouldAddIndexBuildTask = true
		}
	}

	if shouldAddIndexBuildTask && config.DefaultConfig.Workers.Enabled {
		queueData, err := jsoniter.Marshal(IndexBuildTask{
			TaskType:    1,
			NamespaceId: ns.StrId(),
			ProjName:    db.DbName(),
			Branch:      db.BranchName(),
			CollName:    name,
		})
		if err != nil {
			return err
		}

		if err = c.queue.Enqueue(ctx, tx, NewQueueItem(0, queueData, BUILD_INDEX_QUEUE_TASK), 0); err != nil {
			return err
		}
	}

	metadata.Indexes = updatedIndexes
	return nil
}

func (c *CollectionSubspace) Update(ctx context.Context, tx transaction.Tx, ns Namespace, db *Database, name string, id uint32, updatedIndexes []*schema.Index,
) (*CollectionMetadata, error) {
	metadata, err := c.Get(ctx, tx, ns.Id(), db.Id(), name)
	if err != nil {
		return nil, err
	}

	if err = c.updateMetadataIndexes(ctx, tx, ns, db, name, id, metadata, updatedIndexes); err != nil {
		return nil, err
	}

	err = c.updateMetadata(ctx, tx,
		c.validateArgs(ns.Id(), db.Id(), name, &metadata),
		c.getKey(ns.Id(), db.Id(), name),
		collMetaValueVersion,
		metadata,
	)

	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (c *CollectionSubspace) insert(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, name string,
	metadata *CollectionMetadata,
) error {
	return c.insertMetadata(ctx, tx,
		c.validateArgs(nsID, dbID, name, &metadata),
		c.getKey(nsID, dbID, name),
		collMetaValueVersion,
		metadata,
	)
}

func (*CollectionSubspace) decodeMetadata(_ string, payload *internal.TableData) (*CollectionMetadata, error) {
	if payload == nil {
		return nil, errors.ErrNotFound
	}

	if payload.Ver == 0 {
		return &CollectionMetadata{ID: ByteToUInt32(payload.RawData)}, nil
	}

	var metadata CollectionMetadata
	if err := jsoniter.Unmarshal(payload.RawData, &metadata); ulog.E(err) {
		return nil, errors.Internal("failed to unmarshal collection metadata")
	}

	return &metadata, nil
}

func (c *CollectionSubspace) Get(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, name string,
) (*CollectionMetadata, error) {
	payload, err := c.getPayload(ctx, tx,
		c.validateArgs(nsID, dbID, name, nil),
		c.getKey(nsID, dbID, name),
	)
	if err != nil {
		return nil, err
	}

	return c.decodeMetadata(name, payload)
}

func (c *CollectionSubspace) delete(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, name string,
) error {
	return c.deleteMetadata(ctx, tx,
		c.validateArgs(nsID, dbID, name, nil),
		c.getKey(nsID, dbID, name),
	)
}

func (c *CollectionSubspace) softDelete(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, name string,
) error {
	newKey := keys.NewKey(c.SubspaceName, c.KeyVersion, UInt32ToByte(nsID), UInt32ToByte(dbID), collectionKey, name,
		keyDroppedEnd)

	return c.softDeleteMetadata(ctx, tx,
		c.validateArgs(nsID, dbID, name, nil),
		c.getKey(nsID, dbID, name),
		newKey,
	)
}

func (*CollectionSubspace) validateArgs(nsID uint32, dbID uint32, name string, metadata **CollectionMetadata) error {
	if nsID == 0 || dbID == 0 {
		return errors.InvalidArgument("invalid id")
	}

	if name == "" {
		return errors.InvalidArgument("empty collection name")
	}

	if metadata != nil && *metadata == nil {
		return errors.InvalidArgument("invalid nil payload")
	}

	return nil
}

func (c *CollectionSubspace) list(ctx context.Context, tx transaction.Tx, namespaceId uint32, databaseId uint32,
) (map[string]*CollectionMetadata, error) {
	collections := make(map[string]*CollectionMetadata)
	droppedCollections := make(map[string]uint32)

	if err := c.listMetadata(ctx, tx, c.getKey(namespaceId, databaseId, ""), 6,
		func(dropped bool, name string, data *internal.TableData) error {
			cm, err := c.decodeMetadata(name, data)
			if err != nil {
				return err
			}

			if dropped {
				droppedCollections[name] = cm.ID
			} else {
				collections[name] = cm
			}

			return nil
		},
	); err != nil {
		return nil, err
	}

	// retrogression check; if created and dropped both exists then the created id should be greater than dropped id
	log.Debug().Uint32("db", databaseId).Interface("list", droppedCollections).Msg("dropped collections")
	log.Debug().Uint32("db", databaseId).Interface("list", collections).Msg("created collections")

	for droppedC, droppedValue := range droppedCollections {
		if createdValue, ok := collections[droppedC]; ok && droppedValue >= createdValue.ID {
			return nil, errors.Internal(
				"retrogression found in collection assigned value collection [%s] droppedValue [%d] createdValue [%d]",
				droppedC, droppedValue, createdValue.ID)
		}
	}

	return collections, nil
}
