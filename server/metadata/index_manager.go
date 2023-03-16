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

	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/transaction"
)

type IndexManager struct {
	indexStore *IndexSubspace
	queueStore *QueueSubspace
}

func newIndexManager(mdNameRegistry *NameRegistry) *IndexManager {
	return &IndexManager{
		indexStore: newIndexStore(mdNameRegistry),
		queueStore: newQueueStore(mdNameRegistry),
	}
}

func (im *IndexManager) InsertActiveIndex(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32, idx *schema.Index, id uint32) (*IndexMetadata, error) {
	return im.InsertIndexWithState(ctx, tx, nsID, dbID, collID, idx, id, schema.INDEX_ACTIVE)
}

func (im *IndexManager) InsertIndex(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32, idx *schema.Index, id uint32) (*IndexMetadata, error) {
	return im.InsertIndexWithState(ctx, tx, nsID, dbID, collID, idx, id, schema.INDEX_WRITE_MODE)
}

func (im *IndexManager) InsertIndexWithState(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32, idx *schema.Index, id uint32, state schema.IndexState) (*IndexMetadata, error) {
	meta := &IndexMetadata{ID: id, State: state, IdxType: idx.IndexType(), Name: idx.Name}

	err := im.indexStore.insert(ctx, tx, nsID, dbID, collID, meta.Name, meta)
	if err != nil {
		return nil, err
	}
	idx.State = state

	// TODO: if not active add job to index
	// if meta.State == WRITE {
	// im.queueStore.insert....
	// }
	return meta, nil
}

func (im *IndexManager) SoftDelete(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32, idxMeta *IndexMetadata) error {
	idxMeta.State = schema.INDEX_DELETED
	err := im.indexStore.Update(ctx, tx, nsID, dbID, collID, idxMeta.Name, idxMeta)
	if err != nil {
		return err
	}
	// TODO: Add job to delete index
	// im.queueStore.insert...
	return im.indexStore.softDelete(ctx, tx, nsID, dbID, collID, idxMeta.Name)
}

func (im *IndexManager) GetIndexes(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32) (map[string]*IndexMetadata, error) {
	return im.indexStore.list(ctx, tx, nsID, dbID, collID)
}

func (im *IndexManager) Get(ctx context.Context, tx transaction.Tx, nsID uint32, dbID uint32, collID uint32, name string) (*IndexMetadata, error) {
	return im.indexStore.Get(ctx, tx, nsID, dbID, collID, name)
}

func (im *IndexManager) GetActiveIndexes() []*schema.QueryableField {
	return nil
}
