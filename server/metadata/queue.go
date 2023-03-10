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
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// The queue api for use with background workers.
// The api allows a worker to:
//
// 1. enqueue an item,
// 2. peek to see items in the list that are ready to be processed. Vesting time < current time
// 3. Claim an item in the queue and set how long the worker will work on it
// 4. Extend the lease
// 5. Mark the item as complete
//
// The FDB structure for the queue is:
// ["queue subspace", "version", "vesting time", "priority", "item id"] = queue item
//
// The vesting time creates an ordering in the queue. The peek api only returns items that are less or equal to
// the current time. So when an item is claimed and worked on, the vesting time is used to keep the item in the queue
// but push its vesting time into the future so that no other workers can claim it. The worker can contiue to increase
// the vesting time so that no other workers can claim the item. If the worker fails to complete the item, the vesting time
// will eventually reach the current time and be visible to other workers to claim and work on.
//

const (
	queueMetaValueVersion int32  = 1
	itemsSpace            int64  = 1
	maxId                 string = "$MAX"
	maxPriority                  = 10
)

var (
	ErrMissingId       = fmt.Errorf("cannot enqueue without an id")
	ErrNotFound        = fmt.Errorf("QueueItem was not found")
	ErrClaimed         = fmt.Errorf("another worker has claimed this item")
	ErrJsonDeSerialize = fmt.Errorf("failed to unmarshal queue item")
	ErrJsonSerialize   = fmt.Errorf("failed to marshal queue item")
)

type QueueSubspace struct {
	metadataSubspace
}

type QueueItem struct {
	Id         string    `json:"id"`
	Priority   int64     `json:"priority"`
	ErrorCount uint8     `json:"error_count"`
	LeaseId    string    `json:"lease_id,omitempty"`
	Data       []byte    `json:"data"`
	Vesting    time.Time `json:"vesting_time"`
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

func (q *QueueSubspace) getKey(vesting time.Time, priority int64, id string) keys.Key {
	if id == maxId {
		return keys.NewKey(q.SubspaceName, q.KeyVersion, itemsSpace, vesting.UnixMilli(), priority, 0xFF)
	}

	return keys.NewKey(q.SubspaceName, q.KeyVersion, itemsSpace, vesting.UnixMilli(), priority, id)
}

func (q *QueueSubspace) Enqueue(ctx context.Context, tx transaction.Tx, item *QueueItem, delay time.Duration) error {
	if len(item.Id) == 0 {
		return ErrMissingId
	}
	item.Vesting = time.Now().Add(delay)
	key := q.getKey(item.Vesting, item.Priority, item.Id)
	tableData, err := q.encodeItem(item)
	if err != nil {
		return err
	}
	return tx.Replace(ctx, key, tableData, false)
}

func (q *QueueSubspace) Peek(ctx context.Context, tx transaction.Tx, max int) ([]QueueItem, error) {
	var items []QueueItem
	count := 0
	currentTime := time.Now()
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
	startKey := q.getKey(item.Vesting, 0, item.Id)
	endKey := q.getKey(item.Vesting, maxPriority, item.Id)

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
	return nil, ErrNotFound
}

func (q *QueueSubspace) Dequeue(ctx context.Context, tx transaction.Tx, item *QueueItem) error {
	itemKey := q.getKey(item.Vesting, item.Priority, item.Id)
	return tx.Delete(ctx, itemKey)
}

func (q *QueueSubspace) ObtainLease(ctx context.Context, tx transaction.Tx, item *QueueItem, leaseTime time.Duration) (*QueueItem, error) {
	item, err := q.Find(ctx, tx, item)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, ErrNotFound
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
		return ErrClaimed
	}

	return q.Dequeue(ctx, tx, item)
}

func (q *QueueSubspace) RenewLease(ctx context.Context, tx transaction.Tx, item *QueueItem, leaseTime time.Duration) error {
	found, err := q.Find(ctx, tx, item)
	if err != nil {
		return err
	}

	if found.LeaseId != item.LeaseId {
		return ErrClaimed
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
	minVesting := time.UnixMilli(int64(1678281734380)) // 8 March 2023 - No queue item will be before this
	currentTime := time.Now().Add(2 * time.Hour)
	t := time.Now()
	startKey := q.getKey(minVesting, 0, "")
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
		log.Debug().Msgf("scan key: %s id: %s time: %d Greater: %t", v.Key, item.Id, item.Vesting.UnixMilli(), item.Vesting.After(t))
	}

	return iter.Err()
}

func (q *QueueSubspace) decodeItem(data *internal.TableData) (*QueueItem, error) {
	var item QueueItem
	if err := jsoniter.Unmarshal(data.RawData, &item); ulog.E(err) {
		return nil, ErrJsonDeSerialize
	}

	return &item, nil
}

func (q *QueueSubspace) encodeItem(item *QueueItem) (*internal.TableData, error) {
	jsonItem, err := jsoniter.Marshal(item)
	if ulog.E(err) {
		return nil, ErrJsonSerialize
	}
	data := internal.NewTableDataWithVersion(jsonItem, queueMetaValueVersion)
	return data, nil
}
