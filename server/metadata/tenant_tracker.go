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
	"bytes"
	"context"
	"sync"

	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"go.uber.org/atomic"
)

// Tracker is an object attached to a transaction so that a transaction can identify if metadata is changed
// and whether it needs to restart the transaction. Tracker is not thread-safe and should be used only in a single session.
type Tracker struct {
	tenant       string
	endVersion   Version
	startVersion Version
	future       kv.Future
	stopped      atomic.Int32
	stopCB       func(ctx context.Context) (bool, error)
}

// Stop is needed to stop the tracker and reload the tenant if needed.
func (tracker *Tracker) Stop(ctx context.Context) (bool, error) {
	return tracker.stopCB(ctx)
}

// changeDetected is a helper method to identify if the metadata version has changed since the transaction started.
func (tracker *Tracker) changeDetected() bool {
	return bytes.Compare(tracker.endVersion, tracker.startVersion) > 0
}

// CacheTracker is to track if tenant cache is stale and trigger reloads on it. CacheTracker is used by the session
// manager to identify during the running transaction whether there is a need to fill tenant state from the database
// or whether the cached version is up-to-date.
type CacheTracker struct {
	sync.RWMutex

	txMgr          *transaction.Manager
	versionH       *VersionHandler
	tenantVersions map[string]Version
	tenantMgr      *TenantManager
}

// NewCacheTracker creates and returns the cache tracker. It uses tenant manager state to populate in-memory
// version tracking for each tenant.
func NewCacheTracker(tenantMgr *TenantManager, txMgr *transaction.Manager) *CacheTracker {
	tenantVersionMap := make(map[string]Version)
	for tenantName, tenant := range tenantMgr.tenants {
		tenantVersionMap[tenantName] = tenant.version
	}

	return &CacheTracker{
		txMgr:          txMgr,
		tenantVersions: tenantVersionMap,
		versionH:       tenantMgr.versionH,
		tenantMgr:      tenantMgr,
	}
}

func (cacheTracker *CacheTracker) addTenantIfNotTracked(tenant *Tenant) {
	cacheTracker.Lock()
	defer cacheTracker.Unlock()

	if _, ok := cacheTracker.tenantVersions[tenant.namespace.StrId()]; !ok {
		cacheTracker.tenantVersions[tenant.namespace.StrId()] = tenant.version
	}
}

func (cacheTracker *CacheTracker) getTenantVersion(tenant *Tenant) (Version, bool) {
	cacheTracker.RLock()
	defer cacheTracker.RUnlock()

	version, ok := cacheTracker.tenantVersions[tenant.namespace.StrId()]
	return version, ok
}

// InstantTracking is when a tracker is needed outside the caller’s transaction. This is used by DDL
// transactions that are also bumping up the metadata version.
func (cacheTracker *CacheTracker) InstantTracking(ctx context.Context, tx transaction.Tx, tenant *Tenant) (*Tracker, error) {
	var version Version
	var err error
	if tx == nil {
		version, err = cacheTracker.versionH.ReadInOwnTxn(ctx, cacheTracker.txMgr, false)
	} else {
		version, err = cacheTracker.versionH.Read(ctx, tx, false)
	}
	if err != nil {
		return nil, err
	}

	tenantVersion, ok := cacheTracker.getTenantVersion(tenant)
	if !ok {
		cacheTracker.addTenantIfNotTracked(tenant)
		tenantVersion = tenant.version
	}

	tracker := &Tracker{
		endVersion:   version,
		tenant:       tenant.namespace.StrId(),
		startVersion: tenantVersion,
	}
	tracker.stopCB = func(ctx context.Context) (bool, error) {
		// stopCB returns nothing as tracking happened
		return false, nil
	}

	if err := cacheTracker.stopTracking(ctx, tenant, tracker); err != nil {
		return nil, err
	}

	return tracker, nil
}

// DeferredTracking returns a tracker that has a future attached to the caller's transaction.
func (cacheTracker *CacheTracker) DeferredTracking(ctx context.Context, tx transaction.Tx, tenant *Tenant) (*Tracker, error) {
	future, err := cacheTracker.versionH.ReadFuture(ctx, tx, true)
	if err != nil {
		return nil, err
	}

	tenantVersion, ok := cacheTracker.getTenantVersion(tenant)
	if !ok {
		cacheTracker.addTenantIfNotTracked(tenant)
		tenantVersion = tenant.version
	}

	tracker := &Tracker{
		future:       future,
		tenant:       tenant.namespace.StrId(),
		startVersion: tenantVersion,
	}

	tracker.stopCB = func(ctx context.Context) (bool, error) {
		if tracker.stopped.Load() == 1 {
			return false, nil
		}
		tracker.stopped.Store(1)

		val, err := tracker.future.Get()
		if err != nil {
			return false, err
		}
		tracker.endVersion = val

		isChanged := tracker.changeDetected()

		return isChanged, cacheTracker.stopTracking(ctx, tenant, tracker)
	}

	return tracker, nil
}

// StopTracking guarantees that once this call is finished the tenant cache is up-to-date to the tracker’s seen version
// in other words the metadata version that the transaction has seen the tenant state is either newer or at least the same.
func (cacheTracker *CacheTracker) stopTracking(ctx context.Context, tenant *Tenant, tracker *Tracker) error {
	if !tracker.changeDetected() {
		// if tracker state is same then we don't need to do anything
		return nil
	}

	tenantVersion, _ := cacheTracker.getTenantVersion(tenant)
	if bytes.Compare(tenantVersion, tracker.endVersion) >= 0 {
		return nil
	}

	cacheTracker.Lock()
	defer cacheTracker.Unlock()
	if bytes.Compare(cacheTracker.tenantVersions[tenant.namespace.StrId()], tracker.endVersion) >= 0 {
		// check again to avoid reloading multiple times
		return nil
	}

	tx, err := cacheTracker.txMgr.StartTx(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	var version Version
	if version, err = cacheTracker.versionH.Read(ctx, tx, false); err != nil {
		return err
	}

	// tx is handled inside
	if err = tenant.Reload(ctx, tx, version, cacheTracker.tenantMgr.searchSchemasSnapshot, cacheTracker.txMgr); err != nil {
		return err
	}

	cacheTracker.tenantVersions[tenant.namespace.StrId()] = version
	return nil
}
