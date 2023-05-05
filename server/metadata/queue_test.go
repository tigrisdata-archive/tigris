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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/transaction"
)

func initQueueTest(t *testing.T) (*QueueSubspace, transaction.Tx, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := newQueueStore(newTestNameRegistry(t))

	_ = kvStore.DropTable(ctx, s.SubspaceName)

	tm := transaction.NewManager(kvStore)
	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)

	return s, tx, func() {
		assert.NoError(t, tx.Rollback(ctx))
	}
}

func TestEnqueueAndpeek(t *testing.T) {
	t.Run("insert 1 and peek", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		queue, tx, cleanup := initQueueTest(t)
		defer cleanup()

		checkQueueEmpty(t, queue, tx)

		item := NewQueueItem(0, []byte("item1"))
		assert.NoError(t, queue.Enqueue(ctx, tx, item, 100*time.Millisecond))

		checkQueueEmpty(t, queue, tx)
		time.Sleep(500 * time.Millisecond)

		items, err := queue.Peek(ctx, tx, 10)
		assert.NoError(t, err)
		assert.Equal(t, item.Data, items[0].Data)
	})

	t.Run("insert many and peek", func(t *testing.T) {
		toEnQueue := []string{"one", "two", "three", "four", "five"}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		queue, tx, cleanup := initQueueTest(t)
		defer cleanup()

		for i, data := range toEnQueue {
			item := NewQueueItem(0, []byte(data))
			assert.NoError(t, queue.Enqueue(ctx, tx, item, time.Duration(i*100)*time.Millisecond))
		}

		// Assert items only appear after certain times
		for i := 1; i < 6; i++ {
			items, err := queue.Peek(ctx, tx, 5)
			assert.NoError(t, err)
			assert.Len(t, items, i)
			time.Sleep(101 * time.Millisecond)
			for z, item := range items {
				assert.Equal(t, item.Data, []byte(toEnQueue[z]))
			}
		}
	})

	t.Run("peek limits work", func(t *testing.T) {
		toEnQueue := []string{"one", "two", "three", "four", "five"}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		queue, tx, cleanup := initQueueTest(t)
		defer cleanup()

		for _, data := range toEnQueue {
			item := NewQueueItem(0, []byte(data))
			assert.NoError(t, queue.Enqueue(ctx, tx, item, 1*time.Millisecond))
		}

		time.Sleep(10 * time.Millisecond)

		for i := 0; i < 5; i++ {
			checkQueueLength(t, queue, tx, i)
		}
	})
}

func TestPriorityInPeek(t *testing.T) {
	t.Run("orderd by different time and priority", func(t *testing.T) {
		item1 := NewQueueItem(0, []byte("one-item"))
		item2 := NewQueueItem(1, []byte("two-item"))
		item3 := NewQueueItem(0, []byte("one-item"))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		queue, tx, cleanup := initQueueTest(t)
		defer cleanup()

		// Enqueue Item
		assert.NoError(t, queue.Enqueue(ctx, tx, item1, 10*time.Millisecond))
		assert.NoError(t, queue.Enqueue(ctx, tx, item2, 20*time.Millisecond))
		assert.NoError(t, queue.Enqueue(ctx, tx, item3, 30*time.Millisecond))

		time.Sleep(100 * time.Millisecond)

		items := checkQueueLength(t, queue, tx, 3)

		assert.Equal(t, item1.Id, items[0].Id)
		assert.Equal(t, item2.Id, items[1].Id)
		assert.Equal(t, item3.Id, items[2].Id)
	})
}

func TestEnqueueObtainComplete(t *testing.T) {
	item := NewQueueItem(0, []byte("one-item"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue, tx, cleanup := initQueueTest(t)
	defer cleanup()

	checkQueueEmpty(t, queue, tx)

	// Enqueue Item
	assert.NoError(t, queue.Enqueue(ctx, tx, item, 10*time.Millisecond))
	time.Sleep(11 * time.Millisecond)

	items, err := queue.Peek(ctx, tx, 5)
	assert.NoError(t, err)
	assert.Len(t, items, 1)

	working, err := queue.ObtainLease(ctx, tx, &items[0], 100*time.Millisecond)
	assert.NoError(t, err)
	checkQueueEmpty(t, queue, tx)

	time.Sleep(101 * time.Millisecond)
	checkQueueLength(t, queue, tx, 1)
	working, err = queue.ObtainLease(ctx, tx, working, 30*time.Millisecond)
	assert.NoError(t, err)
	checkQueueEmpty(t, queue, tx)

	assert.NoError(t, queue.Complete(ctx, tx, working))
	time.Sleep(200 * time.Millisecond)
	checkQueueEmpty(t, queue, tx)
}

func TestErrors(t *testing.T) {
	item := NewQueueItem(0, []byte("one-item"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue, tx, cleanup := initQueueTest(t)
	defer cleanup()

	_, err := queue.ObtainLease(ctx, tx, item, 100*time.Millisecond)
	assert.Error(t, err)
	assert.Equal(t, err, ErrNotFound)

	err = queue.Complete(ctx, tx, item)
	assert.Error(t, err)
	assert.Equal(t, err, ErrNotFound)
}

func TestWorkProcessFlow(t *testing.T) {
	items := make([]*QueueItem, 0, 5)
	activeItems := make([]QueueItem, 0, 5)
	for _, data := range []string{"one", "two", "three", "four", "five"} {
		items = append(items, NewQueueItem(0, []byte(data)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue, tx, cleanup := initQueueTest(t)
	defer cleanup()

	// Enqueue Items
	for i, item := range items {
		assert.NoError(t, queue.Enqueue(ctx, tx, item, time.Duration(i*10)*time.Millisecond))
	}

	time.Sleep(200 * time.Millisecond)
	waiting := checkQueueLength(t, queue, tx, 5)

	// Claim items Items
	for z := 4; z >= 0; z-- {
		item := waiting[z]
		active, err := queue.ObtainLease(ctx, tx, &item, 10*time.Second)
		assert.Equal(t, active.Id, item.Id)
		assert.NoError(t, err)
		activeItems = append(activeItems, *active)

		checkQueueLength(t, queue, tx, z)
	}

	// Work on items
	time.Sleep(1 * time.Second)
	for i := range activeItems {
		err := queue.Complete(ctx, tx, &activeItems[i])
		assert.NoError(t, err)
	}

	// Check empty queue
	checkQueueEmpty(t, queue, tx)
}

func checkQueueLength(t *testing.T, queue *QueueSubspace, tx transaction.Tx, length int) []QueueItem {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	items, err := queue.Peek(ctx, tx, length)
	assert.NoError(t, err)
	assert.Len(t, items, length)
	return items
}

func checkQueueEmpty(t *testing.T, queue *QueueSubspace, tx transaction.Tx) {
	checkQueueLength(t, queue, tx, 0)
}
