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
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

const (
	queueMetaValueVersion int32  = 1
	itemsSpace            int64  = 1
	maxId                 string = "$MAX"
	maxPriority                  = 10
)

type QueueSubspace struct {
	metadataSubspace
}

type QueueItem struct {
	Id          string `json:"id"`
	Priority    int64  `json:"priority"`
	ErrorCount  uint8  `json:"error_count"`
	LeaseId     string `json:"lease_id,omitempty"`
	Data        []byte `json:"data"`
	VestingTime int64  `json:"vesting_time"`
}

func NewQueueItem(priority int64, data []byte) *QueueItem {
	return &QueueItem{
		Id:         uuid.NewUUIDAsString(),
		Priority:   priority,
		ErrorCount: 0,
		Data:       data,
	}
}

func newQueueStore(nameRegistry *NameRegistry) *QueueSubspace {
	return &QueueSubspace{
		metadataSubspace{
			SubspaceName: nameRegistry.QueueSubspaceName(),
			KeyVersion:   []byte{byte(queueMetaValueVersion)},
		},
	}
}

func (q *QueueSubspace) getKey(vestingTime int64, priority int64, id string) keys.Key {
	if id == maxId {
		return keys.NewKey(q.SubspaceName, q.KeyVersion, itemsSpace, vestingTime, priority, 0xFF)
	}

	return keys.NewKey(q.SubspaceName, q.KeyVersion, itemsSpace, vestingTime, priority, id)
}

func (q *QueueSubspace) Enqueue(ctx context.Context, tx transaction.Tx, item *QueueItem, delay time.Duration) error {
	if len(item.Id) == 0 {
		return errors.Internal("cannot enqueue without an id")
	}
	item.VestingTime = time.Now().Add(delay).UnixMilli()
	key := q.getKey(item.VestingTime, item.Priority, item.Id)
	tableData, err := q.encodeItem(item)
	if err != nil {
		return err
	}
	return tx.Replace(ctx, key, tableData, false)
}

func (q *QueueSubspace) Peak(ctx context.Context, tx transaction.Tx, max int) ([]QueueItem, error) {
	var items []QueueItem
	count := 0
	currentTime := time.Now().UnixMilli()
	endKey := q.getKey(currentTime, maxPriority, maxId)
	iter, err := tx.ReadRange(ctx, nil, endKey, false)
	if err != nil {
		return items, err
	}
	var v kv.KeyValue
	for iter.Next(&v) && count < max {
		count += 1
		item, err := q.decodeItem(v.Data)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}

	return items, iter.Err()
}

func (q *QueueSubspace) Find(ctx context.Context, tx transaction.Tx, item *QueueItem) (*QueueItem, error) {
	startKey := q.getKey(item.VestingTime, 0, item.Id)
	endKey := q.getKey(item.VestingTime, maxPriority, item.Id)

	iter, err := tx.ReadRange(ctx, startKey, endKey, false)
	if err != nil {
		return nil, err
	}
	var v kv.KeyValue
	for iter.Next(&v) {
		found, err := q.decodeItem(v.Data)
		if err != nil {
			return nil, err
		}

		if item.Id == found.Id {
			return item, nil
		}
	}

	if iter.Err() != nil {
		return nil, iter.Err()
	}
	return nil, errors.Internal("QueueItem \"%s\" was not found", item.Id)
}

func (q *QueueSubspace) Dequeue(ctx context.Context, tx transaction.Tx, item *QueueItem) error {
	itemKey := q.getKey(item.VestingTime, item.Priority, item.Id)
	return tx.Delete(ctx, itemKey)
}

func (q *QueueSubspace) ObtainLease(ctx context.Context, tx transaction.Tx, item *QueueItem, leaseTime time.Duration) (*QueueItem, error) {
	item, err := q.Find(ctx, tx, item)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, errors.Internal("Queue Item was not found")
	}

	if err = q.Dequeue(ctx, tx, item); err != nil {
		return nil, err
	}

	item.LeaseId = uuid.NewUUIDAsString()
	if err = q.Enqueue(ctx, tx, item, leaseTime); ulog.E(err) {
		return nil, err
	}

	return item, nil
}

func (q *QueueSubspace) Complete(ctx context.Context, tx transaction.Tx, item *QueueItem) error {
	found, err := q.Find(ctx, tx, item)
	if err != nil {
		return err
	}

	if found.LeaseId != item.LeaseId {
		return errors.Internal("another worker has claimed this item")
	}

	return q.Dequeue(ctx, tx, item)
}

func (q *QueueSubspace) RenewLease(ctx context.Context, tx transaction.Tx, item *QueueItem, leaseTime time.Duration) error {
	found, err := q.Find(ctx, tx, item)
	if err != nil {
		return err
	}

	if found.LeaseId != item.LeaseId {
		return errors.Internal("another worker has claimed this item")
	}

	if err := q.Dequeue(ctx, tx, item); err != nil {
		return err
	}
	if err := q.Enqueue(ctx, tx, item, leaseTime); err != nil {
		return err
	}
	return nil
}

// For local debugging and testing.
//
//nolint:unused
func (q *QueueSubspace) scanTable(ctx context.Context, tx transaction.Tx) error {
	//nolint:unused
	minVestingTime := int64(1678281734380) // 8 March 2023 - No queue item will be before this
	currentTime := time.Now().Add(2 * time.Hour).UnixMilli()
	t := time.Now().UnixMilli()
	startKey := q.getKey(minVestingTime, 0, "")
	endKey := q.getKey(currentTime, maxPriority, maxId)

	iter, err := tx.ReadRange(ctx, startKey, endKey, false)
	if err != nil {
		return err
	}
	log.Debug().Msg("Queue Table Scan")
	var v kv.KeyValue
	for iter.Next(&v) {
		item, err := q.decodeItem(v.Data)
		if err != nil {
			return err
		}
		log.Debug().Msgf("scan key: %s id: %s time: %d Greater: %t", v.Key, item.Id, item.VestingTime, item.VestingTime > t)
	}

	return iter.Err()
}

func (q *QueueSubspace) decodeItem(data *internal.TableData) (*QueueItem, error) {
	var item QueueItem
	if err := jsoniter.Unmarshal(data.RawData, &item); err != nil {
		return nil, errors.Internal("failed to unmarshal queue item")
	}

	return &item, nil
}

func (q *QueueSubspace) encodeItem(item *QueueItem) (*internal.TableData, error) {
	jsonItem, err := jsoniter.Marshal(item)
	if ulog.E(err) {
		return nil, errors.Internal("failed to marshal queue item")
	}
	data := internal.NewTableDataWithVersion(jsonItem, queueMetaValueVersion)
	return data, nil
}
