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
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/cache"
)

const (
	pingSessionDuration     = 1 * time.Second
	sessionTrackingDuration = 30 * time.Second
)

type Sessions struct {
	sync.RWMutex

	cache            cache.Cache
	devices          map[string]*Session
	txMgr            *transaction.Manager
	tenantMgr        *metadata.TenantManager
	channelFactory   *ChannelFactory
	heartbeatFactory *HeartbeatFactory
	versionH         *metadata.VersionHandler
	tenantTracker    *metadata.CacheTracker
}

func NewSessionMgr(cache cache.Cache, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager, heartbeatF *HeartbeatFactory, factory *ChannelFactory) *Sessions {
	return &Sessions{
		cache:            cache,
		txMgr:            txMgr,
		tenantMgr:        tenantMgr,
		heartbeatFactory: heartbeatF,
		devices:          make(map[string]*Session),
		channelFactory:   factory,
		tenantTracker:    metadata.NewCacheTracker(tenantMgr, txMgr),
	}
}

func (s *Sessions) TrackSessions() {
	go func() {
		ticker := time.NewTicker(sessionTrackingDuration)
		defer ticker.Stop()
		for range ticker.C {
			s.destroyInactiveSessions()
		}
	}()

	go func() {
		ticker := time.NewTicker(pingSessionDuration)
		defer ticker.Stop()
		for range ticker.C {
			s.pingSessions()
		}
	}()
}

func (s *Sessions) pingSessions() {
	s.RLock()
	defer s.RUnlock()

	for _, d := range s.devices {
		if !d.IsActive() {
			_ = d.sendHeartbeat()
		}
	}
}

func (s *Sessions) destroyInactiveSessions() {
	s.Lock()
	defer s.Unlock()

	for _, d := range s.devices {
		if !d.IsActive() {
			s.RemoveDevice(context.TODO(), d)
		}
	}
}

func (s *Sessions) RemoveDevice(_ context.Context, session *Session) {
	if session, ok := s.devices[session.id]; ok {
		delete(s.devices, session.id)
	}
}

func (s *Sessions) AddDevice(ctx context.Context, conn *websocket.Conn, params ConnectionParams) (*Session, error) {
	if device, ok := s.devices[params.SessionId]; ok {
		return device, nil
	}

	sess, err := s.CreateDeviceSession(ctx, conn, params)
	if err != nil {
		return nil, err
	}
	s.devices[sess.id] = sess
	return sess, nil
}

func (s *Sessions) ExecuteRunner(ctx context.Context, runner RTMRunner) (Response, error) {
	namespaceForThisSession, err := request.GetNamespace(ctx)
	if err != nil {
		return Response{}, err
	}
	tenant, err := s.tenantMgr.GetTenant(ctx, namespaceForThisSession)
	if err != nil {
		return Response{}, errors.NotFound("tenant '%s' not found", namespaceForThisSession)
	}

	if _, err = s.tenantTracker.InstantTracking(ctx, nil, tenant); err != nil {
		return Response{}, err
	}

	return runner.Run(ctx, tenant)
}
