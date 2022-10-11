// Copyright 2022 Tigris Data, Inc.
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
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
)

const (
	rateHysteresis = 10 // ±10 rps per instance wouldn't cause rate regulation

	// when rate adjustment is needed increment current rate by ± this value
	// (this is percentage of maximum per node per namespace limit)
	// Set by config.DefaultConfig.Quota.Namespace.Node.(Read|Write)RateLimit.
	rateIncrement = 10

	// allow human user to go 5% beyond the quota.
	overProvisionedPercent = 5
)

type Backend interface {
	CurRates(ctx context.Context, namespace string) (int64, int64, error)
}

type instanceState struct {
	setReadLimit  atomic.Int64
	setWriteLimit atomic.Int64

	State
}

type namespace struct {
	tenantQuota sync.Map
	tenantMgr   *metadata.TenantManager

	cfg *config.QuotaConfig

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	backend Backend
}

func unlimitedDefaultNamespace(namespace string, cfg *config.NamespaceLimitsConfig) bool {
	if namespace != request.DefaultNamespaceName {
		return false
	}

	_, ok := cfg.Namespaces[namespace]

	// No special configuration for default namespace
	return !ok
}

func checkMaxSize(units int, namespace string, isWrite bool, cfg *config.NamespaceLimitsConfig) error {
	// Maximum per node size
	if units > cfg.Node.Limit(isWrite) {
		return ErrMaxRequestSizeExceeded
	}

	// Maximum per instance size
	if units > cfg.NamespaceLimits(namespace).Limit(isWrite) {
		return ErrMaxRequestSizeExceeded
	}

	return nil
}

func isBlacklistedNamespace(namespace string, cfg *config.NamespaceLimitsConfig) bool {
	l, ok := cfg.Namespaces[namespace]
	if !ok {
		return false
	}

	return l.ReadUnits <= 0 && l.WriteUnits <= 0
}

func (i *namespace) allowOrWait(ctx context.Context, namespace string, size int, isWrite bool, isWait bool) error {
	if unlimitedDefaultNamespace(namespace, &i.cfg.Namespace) {
		return nil
	}

	if isBlacklistedNamespace(namespace, &i.cfg.Namespace) {
		if isWrite {
			return ErrWriteUnitsExceeded
		}
		return ErrReadUnitsExceeded
	}

	units := toUnits(size, isWrite)

	if err := checkMaxSize(units, namespace, isWrite, &i.cfg.Namespace); err != nil {
		return err
	}

	if isWait {
		return i.getState(namespace).Wait(ctx, units, isWrite)
	}

	return i.getState(namespace).Allow(units, isWrite)
}

// allowOrWait taking overprovisioning of human user into account
func (i *namespace) allowOrWaitWithOP(ctx context.Context, namespace string, size int, isWrite bool, isWait bool) error {
	err := i.allowOrWait(ctx, namespace, size, isWrite, isWait)
	if err == nil {
		return nil
	}

	// Allow human user to go beyond the quota limit
	if request.IsHumanUser(ctx) && (errors.Is(err, ErrReadUnitsExceeded) || errors.Is(err, ErrWriteUnitsExceeded)) {
		return i.allowOrWait(ctx, getHumanUserNamespace(namespace), size, isWrite, isWait)
	}

	return err
}

func (i *namespace) Allow(ctx context.Context, namespace string, size int, isWrite bool) error {
	return i.allowOrWaitWithOP(ctx, namespace, size, isWrite, false)
}

func (i *namespace) Wait(ctx context.Context, namespace string, size int, isWrite bool) error {
	return i.allowOrWaitWithOP(ctx, namespace, size, isWrite, true)
}

func isHumanUserNamespace(namespace string) bool {
	return strings.HasSuffix(namespace, "$op")
}

func getHumanUserNamespace(namespace string) string {
	return namespace + "$op"
}

func (i *namespace) getLimits(namespace string) (int, int) {
	cfg, ok := i.cfg.Namespace.Namespaces[namespace]
	if !ok {
		cfg = i.cfg.Namespace.Default
	}

	ru, wu := cfg.ReadUnits, cfg.WriteUnits
	if isHumanUserNamespace(namespace) {
		ru = ru * overProvisionedPercent / 100
		wu = wu * overProvisionedPercent / 100
	}

	return ru, wu
}

func (i *namespace) getState(namespace string) *instanceState {
	is, ok := i.tenantQuota.Load(namespace)
	if !ok {
		ru, wu := i.getLimits(namespace)
		// Create new state if didn't exist before
		is = &instanceState{
			State: State{
				Write: Limiter{
					isWrite: true,
					Rate:    rate.NewLimiter(rate.Limit(wu), wu),
				},
				Read: Limiter{
					Rate: rate.NewLimiter(rate.Limit(ru), ru),
				},
			},
		}
		i.tenantQuota.Store(namespace, is)
	}

	return is.(*instanceState)
}

func (i *namespace) loadCurNamespaceState() {
	for _, ns := range i.tenantMgr.GetNamespaceNames() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		is := i.getState(ns)
		curRead, curWrite, err := i.backend.CurRates(ctx, ns)
		if err == nil {
			i.updateLimits(ns, is, curRead, curWrite)
		} else {
			log.Debug().Err(err).Msg("Error updating remote rate metrics")
		}

		cancel()
	}
}

// calcLimit calculates new limit for the metric, based on updated cluster wide current and maximum rate.
func calcLimit(setNodeLimit int64, maxNodeLimit int64, curNamespace int64, maxNamespace int64, hysteresis int64, increment int64) int64 {
	if setNodeLimit == 0 {
		setNodeLimit = maxNodeLimit
	}

	inc := maxNodeLimit * increment / 100
	if inc == 0 {
		inc = 1
	}

	if curNamespace > maxNamespace+hysteresis {
		setNodeLimit -= inc
	} else if curNamespace < maxNamespace-hysteresis {
		setNodeLimit += inc
	}

	if setNodeLimit < 1 {
		setNodeLimit = 1
	} else if setNodeLimit > maxNodeLimit {
		setNodeLimit = maxNodeLimit
	}

	return setNodeLimit
}

func (i *namespace) updateLimits(ns string, is *instanceState, curRead int64, curWrite int64) {
	ru, wu := i.getLimits(ns)

	// calculate read limits
	readLimit := calcLimit(is.setReadLimit.Load(), int64(i.cfg.Namespace.Node.ReadUnits), curRead, int64(ru),
		rateHysteresis, rateIncrement)
	// update read limiter config
	if readLimit != is.setReadLimit.Load() {
		is.Read.SetLimit(int(readLimit))
		is.Read.SetBurst(int(readLimit))
		is.setReadLimit.Store(readLimit)
		metrics.UpdateQuotaCurrentNodeLimit(ns, int(readLimit), false)
		log.Debug().Str("ns", ns).Int64("read_units", readLimit).Msg("Adjusted namespace quota read limits")
	}

	// calculate write limits
	writeLimit := calcLimit(is.setWriteLimit.Load(), int64(i.cfg.Namespace.Node.WriteUnits), curWrite, int64(wu),
		rateHysteresis, rateIncrement)
	// update write limiter config
	if writeLimit != is.setWriteLimit.Load() {
		is.Write.SetLimit(int(writeLimit))
		is.Write.SetBurst(int(writeLimit))
		is.setWriteLimit.Store(writeLimit)
		metrics.UpdateQuotaCurrentNodeLimit(ns, int(writeLimit), true)
		log.Debug().Str("ns", ns).Int64("write_units", writeLimit).Msg("Adjusted quota write limits updated")
	}
}

func initNamespace(tm *metadata.TenantManager, cfg *config.QuotaConfig, backend Backend) *namespace {
	log.Debug().Msg("Initializing per namespace quota manager")

	ctx, cancel := context.WithCancel(context.Background())

	i := &namespace{
		cfg: cfg, tenantMgr: tm, ctx: ctx, cancel: cancel,
		backend: backend,
	}

	i.wg.Add(1)

	go i.refreshLoop()

	return i
}

func (i *namespace) Cleanup() {
	i.cancel()
	i.wg.Wait()
}

func (i *namespace) refreshLoop() {
	defer i.wg.Done()

	log.Debug().Dur("refresh_interval", i.cfg.Namespace.RefreshInterval).Msg("Initializing storage refresh loop")

	t := time.NewTicker(i.cfg.Namespace.RefreshInterval)
	defer t.Stop()

	for {
		log.Debug().Msg("Refreshing namespace request rates metrics")

		i.loadCurNamespaceState()

		select {
		case <-t.C:
		case <-i.ctx.Done():
			log.Debug().Msg("Namespace rate refresh loop exited")
			return
		}
	}
}
