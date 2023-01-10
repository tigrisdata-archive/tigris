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

package quota

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	ulog "github.com/tigrisdata/tigris/util/log"
	"go.uber.org/atomic"
)

type storage struct {
	tenantQuota sync.Map
	cfg         *config.QuotaConfig
	tenantMgr   *metadata.TenantManager

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

type storageState struct {
	Size atomic.Int64
}

func skipStorageCheck(name string) bool {
	switch name {
	case api.DropCollectionMethodName, api.DropDatabaseMethodName:
		return true
	case api.DeleteMethodName:
		return true
	}

	return false
}

func (s *storage) Allow(ctx context.Context, namespace string, size int, isWrite bool) error {
	l := s.getState(namespace)

	var method string
	if md, _ := request.GetRequestMetadataFromContext(ctx); md != nil {
		method = md.GetMethodName()
	}

	if !isWrite || skipStorageCheck(method) {
		return nil
	}

	return s.checkStorage(namespace, l, size)
}

func (s *storage) Wait(_ context.Context, namespace string, size int, isWrite bool) error {
	l := s.getState(namespace)

	if !isWrite {
		return nil
	}

	return s.checkStorage(namespace, l, size)
}

func (s *storage) getState(namespace string) *storageState {
	is, ok := s.tenantQuota.Load(namespace)
	if !ok {
		// Create new state if didn't exist before
		is = &storageState{}
		s.tenantQuota.Store(namespace, is)
	}

	return is.(*storageState)
}

func (s *storage) checkStorage(namespace string, ss *storageState, size int) error {
	sz := ss.Size.Load()

	sizeLimit := s.cfg.Storage.DataSizeLimit
	nsLimit, ok := s.cfg.Storage.Namespaces[namespace]
	if ok {
		sizeLimit = nsLimit.Size
	}

	if sz+int64(size) >= sizeLimit {
		return ErrStorageSizeExceeded
	}

	return nil
}

func initStorage(tm *metadata.TenantManager, cfg *config.QuotaConfig) *storage {
	log.Debug().Msg("Initializing storage quota manager")

	ctx, cancel := context.WithCancel(context.Background())

	s := &storage{tenantMgr: tm, ctx: ctx, cancel: cancel, cfg: cfg}

	s.wg.Add(1)

	go s.refreshLoop()

	return s
}

func (s *storage) Cleanup() {
	s.cancel()
	s.wg.Wait()
}

func getDbSize(ctx context.Context, tenant *metadata.Tenant, db *metadata.Database) int64 {
	dbSize, err := tenant.DatabaseSize(ctx, db)
	if err != nil {
		ulog.E(err)
	}
	return dbSize
}

func getCollSize(ctx context.Context, tenant *metadata.Tenant, db *metadata.Database, coll *schema.DefaultCollection) int64 {
	collSize, err := tenant.CollectionSize(ctx, db, coll)
	if err != nil {
		ulog.E(err)
	}
	return collSize
}

func (s *storage) getTenantSize(ctx context.Context, namespace string) int64 {
	tenant, err := s.tenantMgr.GetTenant(ctx, namespace)
	if ulog.E(err) {
		return 0
	}
	size, err := tenant.Size(ctx)
	ulog.E(err)
	return size
}

func (s *storage) updateMetricsForNamespace(ctx context.Context, namespace string) {
	tenant, err := s.tenantMgr.GetTenant(ctx, namespace)
	if ulog.E(err) {
		return
	}
	tenantName := tenant.GetNamespace().Metadata().Name

	for _, dbName := range tenant.ListDatabaseWithBranches(ctx) {
		db, err := tenant.GetDatabase(ctx, metadata.NewDatabaseName(dbName))
		if ulog.E(err) {
			return
		}
		if db == nil {
			// as there is no coordination between listDatabase and getDatabase that means a database can be dropped in between
			continue
		}

		metrics.UpdateDbSizeMetrics(namespace, tenantName, dbName, getDbSize(ctx, tenant, db))

		for _, coll := range db.ListCollection() {
			metrics.UpdateCollectionSizeMetrics(namespace, tenantName, dbName, coll.Name, getCollSize(ctx, tenant, db, coll))
		}
	}

	dsz := s.getTenantSize(ctx, namespace)

	metrics.UpdateNameSpaceSizeMetrics(namespace, tenantName, dsz)

	if s.cfg.Storage.Enabled {
		ss := s.getState(namespace)
		ss.Size.Store(dsz)
	}
}

func (s *storage) updateAllMetrics(ctx context.Context) {
	for _, namespace := range s.tenantMgr.GetNamespaceNames() {
		s.updateMetricsForNamespace(ctx, namespace)
	}
}

func (s *storage) refreshLoop() {
	defer s.wg.Done()

	log.Debug().Dur("refresh_interval", s.cfg.Storage.RefreshInterval).Msg("Initializing storage refresh loop")

	t := time.NewTicker(s.cfg.Storage.RefreshInterval)
	defer t.Stop()

	for {
		log.Debug().Msg("Refreshing storage size metrics")

		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Storage.RefreshInterval)

		s.updateAllMetrics(ctx)

		cancel()

		select {
		case <-t.C:
		case <-s.ctx.Done():
			log.Debug().Msg("Storage size refresh loop exited")
			return
		}
	}
}
