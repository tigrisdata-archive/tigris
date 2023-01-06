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

package realtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/store/cache"
)

const (
	heartbeatTable           = "heartbeat"
	defaultExpiryTimeSeconds = 120
)

type HeartbeatFactory struct {
	sync.Mutex

	cache      cache.Cache
	encoder    metadata.CacheEncoder
	heartbeats map[string]*HeartbeatTable
}

func NewHeartbeatFactory(cache cache.Cache, encoder metadata.CacheEncoder) *HeartbeatFactory {
	return &HeartbeatFactory{
		cache:      cache,
		encoder:    encoder,
		heartbeats: make(map[string]*HeartbeatTable),
	}
}

func (factory *HeartbeatFactory) GetHeartbeatTable(tenantId uint32, projectId uint32) *HeartbeatTable {
	factory.Lock()
	defer factory.Unlock()

	tableName, _ := factory.encoder.EncodeCacheTableName(tenantId, projectId, heartbeatTable)
	if table, ok := factory.heartbeats[tableName]; ok {
		return table
	}

	return NewHeartbeat(factory.cache, tableName)
}

type HeartbeatTable struct {
	cache         cache.Cache
	tableName     string
	lastHeartbeat time.Time
}

func NewHeartbeat(cache cache.Cache, tableName string) *HeartbeatTable {
	return &HeartbeatTable{
		cache:     cache,
		tableName: tableName,
	}
}

func (h *HeartbeatTable) timeToPing() bool {
	return time.Since(h.lastHeartbeat) >= 5*time.Second
}

func (h *HeartbeatTable) Ping(sessionId string) error {
	if !h.timeToPing() {
		return nil
	}

	err := h.cache.Set(
		context.TODO(),
		h.tableName,
		sessionId,
		internal.NewCacheData([]byte(fmt.Sprintf("%d", time.Now().UnixNano()))),
		&cache.SetOptions{EX: defaultExpiryTimeSeconds},
	)
	h.lastHeartbeat = time.Now()
	return err
}

func (h *HeartbeatTable) GroupsExpired(groupsName []string) bool {
	if h.lastHeartbeat.IsZero() {
		return false
	}
	if count, err := h.cache.Exists(context.TODO(), h.tableName, groupsName...); err == nil && count == 0 {
		return true
	}

	return false
}
