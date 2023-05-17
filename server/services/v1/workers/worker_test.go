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

package workers

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

var kvStore kv.TxStore

func TestCompleteMultipleJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tm := transaction.NewManager(kvStore)
	queue := metadata.NewQueueStore(&metadata.NameRegistry{QueueSB: "test_queue_" + t.Name()})
	_ = kvStore.DropTable(ctx, queue.SubspaceName)
	pool := NewWorkerPool(10, queue, tm, nil, &search.NoopStore{}, 10*time.Millisecond, 100*time.Millisecond)
	assert.NoError(t, pool.Start())

	for i := 0; i < 5; i++ {
		tx, err := tm.StartTx(ctx)
		assert.NoError(t, err)
		err = queue.Enqueue(ctx, tx, testTask(t, time.Duration(i*1)*time.Millisecond, 0, false), 0)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))
	}

	eventChan := make(chan Event)
	pool.subscribe(eventChan)

	for i := 0; i < 5; i++ {
		event := <-eventChan
		assert.True(t, event.Success)
	}
	tx, err := tm.StartTx(ctx)
	assert.NoError(t, err)
	items, err := queue.GetAll(ctx, tx)
	assert.NoError(t, err)
	assert.NoError(t, tx.Rollback(ctx))
	assert.Len(t, items, 0)
}

func TestJobWithToManyErrorsIsDropped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tm := transaction.NewManager(kvStore)
	queue := metadata.NewQueueStore(&metadata.NameRegistry{QueueSB: "test_queue_" + t.Name()})
	_ = kvStore.DropTable(ctx, queue.SubspaceName)

	pool := NewWorkerPool(3, queue, tm, nil, &search.NoopStore{}, 10*time.Millisecond, 100*time.Millisecond)
	defer pool.Stop()
	assert.NoError(t, pool.Start())
	eventChan := make(chan Event)
	pool.subscribe(eventChan)

	tx, err := tm.StartTx(ctx)
	assert.NoError(t, err)
	assert.NoError(t, queue.Enqueue(ctx, tx, testTask(t, 0, MAX_ERROR_COUNT+1, false), time.Duration(10*int(time.Millisecond))))
	assert.NoError(t, tx.Commit(ctx))

	event := <-eventChan
	assert.Equal(t, uint8(10), event.Item.ErrorCount)
	assert.Equal(t, false, event.Success)

	time.Sleep(1 * time.Second)
	tx, err = tm.StartTx(ctx)
	assert.NoError(t, err)
	items, err := queue.GetAll(ctx, tx)
	assert.NoError(t, err)
	assert.Len(t, items, 0)
	assert.NoError(t, tx.Rollback(ctx))
}

func TestJobWithErrorCountMultipleWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tm := transaction.NewManager(kvStore)
	queue := metadata.NewQueueStore(&metadata.NameRegistry{QueueSB: "test_queue_" + t.Name()})
	_ = kvStore.DropTable(ctx, queue.SubspaceName)

	pool := NewWorkerPool(10, queue, tm, nil, &search.NoopStore{}, 100*time.Millisecond, 100*time.Millisecond)
	defer pool.Stop()
	assert.NoError(t, pool.Start())
	completedChan := make(chan Event)
	pool.subscribe(completedChan)

	for i := 0; i < 4; i++ {
		tx, err := tm.StartTx(ctx)
		assert.NoError(t, err)
		assert.NoError(t, queue.Enqueue(ctx, tx, testTask(t, 0, i+1, false), time.Duration(i*10*int(time.Millisecond))))
		assert.NoError(t, tx.Commit(ctx))
	}
	for i := 0; i < 4; i++ {
		event := <-completedChan
		assert.Equal(t, uint8(i+1), event.Item.ErrorCount)
	}

	tx, err := tm.StartTx(ctx)
	assert.NoError(t, err)
	items, err := queue.GetAll(ctx, tx)
	assert.NoError(t, err)
	assert.Len(t, items, 0)
	assert.NoError(t, tx.Rollback(ctx))
}

func TestJobWithDyingWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tm := transaction.NewManager(kvStore)
	queue := metadata.NewQueueStore(&metadata.NameRegistry{QueueSB: "test_queue_" + t.Name()})
	_ = kvStore.DropTable(ctx, queue.SubspaceName)

	pool := NewWorkerPool(1, queue, tm, nil, &search.NoopStore{}, 100*time.Millisecond, 100*time.Millisecond)
	defer pool.Stop()
	assert.NoError(t, pool.Start())

	completedChan := make(chan Event)
	pool.subscribe(completedChan)

	tx, err := tm.StartTx(ctx)
	assert.NoError(t, err)
	assert.NoError(t, queue.Enqueue(ctx, tx, testTask(t, 0, 1, true), time.Duration(10*int(time.Millisecond))))
	assert.NoError(t, tx.Commit(ctx))
	event := <-completedChan
	assert.Equal(t, uint8(2), event.Item.ErrorCount)

	tx, err = tm.StartTx(ctx)
	assert.NoError(t, err)
	items, err := queue.GetAll(ctx, tx)
	assert.NoError(t, err)
	assert.Len(t, items, 0)
	assert.NoError(t, tx.Rollback(ctx))
}

func testTask(t *testing.T, sleepTime time.Duration, errorCount int, shouldStopWorker bool) *metadata.QueueItem {
	task := WorkerTestTask{
		Sleep:            sleepTime,
		NumErrors:        errorCount,
		ShouldStopWorker: shouldStopWorker,
	}
	data, err := jsoniter.Marshal(task)
	assert.NoError(t, err)
	return metadata.NewQueueItem(0, data, metadata.TEST_QUEUE_TASK)
}

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled", Format: "console"})

	fdbCfg, err := config.GetTestFDBConfig("../../../../")
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB config: %v", err))
	}

	kvStore, err = kv.NewBuilder().WithStats().Build(fdbCfg)
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB KV %v", err))
	}

	os.Exit(m.Run())
}
