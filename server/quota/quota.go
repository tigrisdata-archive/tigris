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

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
)

var (
	ErrReadUnitsExceeded      = errors.ResourceExhausted("request read rate exceeded")
	ErrWriteUnitsExceeded     = errors.ResourceExhausted("request write rate exceeded")
	ErrStorageSizeExceeded    = errors.ResourceExhausted("data size limit exceeded")
	ErrMaxRequestSizeExceeded = errors.ResourceExhausted("maximum request size limit exceeded")
)

type Quota interface {
	Allow(ctx context.Context, namespace string, size int, isWrite bool) error
	Wait(ctx context.Context, namespace string, size int, isWrite bool) error
	Cleanup()
}

type State struct {
	Read  Limiter
	Write Limiter
}

func (s *State) Allow(size int, isWrite bool) error {
	if isWrite {
		return s.Write.Allow(size)
	}

	return s.Read.Allow(size)
}

func (s *State) Wait(ctx context.Context, size int, isWrite bool) error {
	if isWrite {
		return s.Write.Wait(ctx, size)
	}

	return s.Read.Wait(ctx, size)
}

type Manager struct {
	quota []Quota
}

var mgr Manager

// this is extracted from Init for tests.
func initManager(tm *metadata.TenantManager, cfg *config.Config) *Manager {
	var q []Quota

	if cfg.Quota.ReadUnitSize != 0 {
		config.ReadUnitSize = cfg.Quota.ReadUnitSize
	}

	if cfg.Quota.WriteUnitSize != 0 {
		config.WriteUnitSize = cfg.Quota.WriteUnitSize
	}

	log.Debug().Int("read_unit_size", config.ReadUnitSize).Int("write_unit_size", config.WriteUnitSize).Msg("Initializing quota manager")

	// metrics calculation is piggybacked to storage quota, so initialize
	// storage quota manager even when quota is disabled, but metrics are enabled
	if cfg.Quota.Storage.Enabled || cfg.Metrics.Size.Enabled {
		q = append(q, initStorage(tm, &cfg.Quota))
	}

	if cfg.Quota.Node.Enabled {
		q = append(q, initNode(&cfg.Quota))
	}

	if cfg.Quota.Namespace.Enabled {
		if cfg.Observability.Provider == "datadog" {
			if cfg.Quota.Namespace.RefreshInterval <= 0 {
				log.Fatal().Msg("refresh interval should be non-empty")
			}

			q = append(q, initNamespace(tm, &cfg.Quota, initDatadogMetrics(cfg)))
		} else {
			q = append(q, initNamespace(tm, &cfg.Quota, initNoopMetrics(cfg)))
		}
	}

	return &Manager{quota: q}
}

func Init(tm *metadata.TenantManager, cfg *config.Config) error {
	mgr = *initManager(tm, cfg)

	return nil
}

func (m *Manager) cleanup() {
	for _, q := range mgr.quota {
		q.Cleanup()
	}
	log.Debug().Msg("Cleaned up quota manager")
}

func Cleanup() {
	mgr.cleanup()
}

func unitSize(isWrite bool) int64 {
	if isWrite {
		return int64(config.WriteUnitSize)
	}

	return int64(config.ReadUnitSize)
}

func toUnits(size int, isWrite bool) int {
	us := unitSize(isWrite)

	// + (unitSize - 1) guarantees that fractional part is counted as one unit
	return int((int64(size) + us - 1) / us)
}

func reportError(namespace string, size int, isWrite bool, err error) error {
	if errors.Is(err, ErrReadUnitsExceeded) || errors.Is(err, ErrWriteUnitsExceeded) {
		metrics.UpdateQuotaRateThrottled(namespace, toUnits(size, isWrite), isWrite)
	} else if errors.Is(err, ErrStorageSizeExceeded) {
		metrics.UpdateQuotaStorageThrottled(namespace, size)
	}

	return err
}

// Allow checks read, write rates and storage size limits for the namespace
// and returns error if at least one of them is exceeded.
func Allow(ctx context.Context, namespace string, size int, isWrite bool) error {
	for _, q := range mgr.quota {
		if err := q.Allow(ctx, namespace, size, isWrite); err != nil {
			return reportError(namespace, size, isWrite, err)
		}
	}

	metrics.UpdateQuotaUsage(namespace, toUnits(size, isWrite), isWrite)

	return nil
}

func Wait(ctx context.Context, namespace string, size int, isWrite bool) error {
	for _, q := range mgr.quota {
		if err := q.Wait(ctx, namespace, size, isWrite); err != nil {
			return reportError(namespace, size, isWrite, err)
		}
	}

	metrics.UpdateQuotaUsage(namespace, toUnits(size, isWrite), isWrite)

	return nil
}
