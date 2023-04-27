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

package billing

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/defaults"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type UsageReporter struct {
	glbState     metrics.UsageProvider
	billingSvc   Provider
	interval     time.Duration
	nsMgr        metadata.NamespaceMetadataMgr
	tenantGetter metadata.TenantGetter
	ctx          context.Context
}

func NewUsageReporter(gs metrics.UsageProvider, tm metadata.NamespaceMetadataMgr, tg metadata.TenantGetter, billing Provider) (*UsageReporter, error) {
	if gs == nil || tm == nil {
		return nil, errors.Internal("usage reporter cannot be initialized")
	}

	return &UsageReporter{
		glbState:     gs,
		billingSvc:   billing,
		interval:     config.DefaultConfig.Billing.Reporter.RefreshInterval,
		nsMgr:        tm,
		tenantGetter: tg,
		ctx:          context.Background(),
	}, nil
}

func (r *UsageReporter) Start() {
	if config.DefaultConfig.Billing.Reporter.Enabled {
		go r.refreshLoop()
	}
}

func (r *UsageReporter) refreshLoop() {
	log.Info().Dur("refresh_interval", r.interval).Msg("Starting usage reporter")
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for range t.C {
		ulog.E(r.pushUsage())
		ulog.E(r.pushStorage())
	}
}

func (r *UsageReporter) pushUsage() error {
	chunk := r.glbState.Flush()
	log.Info().Msgf("reporting usage data for %d tenants", len(chunk.Tenants))

	if len(chunk.Tenants) == 0 {
		log.Info().Msg("no tenant data to report")
		return nil
	}

	events := make([]*UsageEvent, 0, len(chunk.Tenants))

	// refresh and get metadata for all tenants if anyone is missing metronome integration
	for namespaceId := range chunk.Tenants {
		nsMeta := r.nsMgr.GetNamespaceMetadata(r.ctx, namespaceId)
		if nsMeta == nil || nsMeta.Accounts.Metronome == nil {
			err := r.nsMgr.RefreshNamespaceAccounts(r.ctx)
			if err != nil {
				log.Error().Err(err).Msg("failed to refresh namespace metadata")
			}
			break
		}
	}

	for namespaceId, stats := range chunk.Tenants {
		nsMeta := r.nsMgr.GetNamespaceMetadata(r.ctx, namespaceId)
		if nsMeta == nil {
			log.Error().Msgf("invalid namespace id %s", namespaceId)
			continue
		}
		if len(nsMeta.StrId) == 0 || nsMeta.StrId == defaults.DefaultNamespaceName {
			// invalid namespace id, permanently disable account creation
			nsMeta.Accounts.DisableMetronome()
			err := r.nsMgr.UpdateNamespaceMetadata(r.ctx, *nsMeta)
			if err != nil {
				log.Error().Err(err).Msgf("error saving metadata for user %s", nsMeta.StrId)
			}
			continue
		}

		if id, enabled := nsMeta.Accounts.GetMetronomeId(); !enabled {
			continue
		} else if len(id) == 0 {
			log.Info().Msgf("creating Metronome account for %s", nsMeta.StrId)

			billingId, err := r.billingSvc.CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name)
			if !ulog.E(err) && billingId != uuid.Nil {
				nsMeta.Accounts.AddMetronome(billingId.String())

				// add default plan to the user
				added, err := r.billingSvc.AddDefaultPlan(r.ctx, billingId)
				if err != nil || !added {
					log.Error().Err(err).Msgf("error adding default plan to user id %s", nsMeta.StrId)
				}

				// save updated namespace metadata
				err = r.nsMgr.UpdateNamespaceMetadata(r.ctx, *nsMeta)
				if err != nil {
					log.Error().Err(err).Msgf("error saving metadata for user %s", nsMeta.StrId)
				}
			}
		}

		// continue to send event, metronome will disregard it if account does not exist
		event := NewUsageEventBuilder().
			WithNamespaceId(nsMeta.StrId).
			WithTimestamp(chunk.EndTime).
			WithDatabaseUnits(stats.ReadUnits + stats.WriteUnits).
			WithSearchUnits(stats.SearchUnits).
			Build()
		events = append(events, event)

		log.Debug().Msgf("reporting usage for %s -> %+v", namespaceId, event)
	}

	err := r.billingSvc.PushUsageEvents(r.ctx, events)
	if err != nil {
		return err
	}
	return nil
}

func (r *UsageReporter) pushStorage() error {
	tenants := r.tenantGetter.AllTenants(r.ctx)
	log.Info().Msgf("reporting storage data for %d tenants", len(tenants))

	events := make([]*StorageEvent, 0, len(tenants))
	for _, t := range tenants {
		nsMeta := t.GetNamespace().Metadata()
		if id, enabled := nsMeta.Accounts.GetMetronomeId(); len(id) == 0 || !enabled {
			// user doesn't have metronome integration; skip
			continue
		}
		var dbBytes int64
		// ensure size
		dbStats, err := t.Size(r.ctx)
		if err != nil || dbStats == nil {
			log.Error().Err(err).Str("ns", nsMeta.StrId).Msgf("tenant.Size() failed")
		} else {
			dbBytes += dbStats.StoredBytes
		}

		secondaryIndexStats, err := t.IndexSize(r.ctx)
		if err != nil || secondaryIndexStats == nil {
			log.Error().Err(err).Str("ns", nsMeta.StrId).Msgf("tenant.IndexSize() failed")
		} else {
			dbBytes += secondaryIndexStats.StoredBytes
		}

		// todo: add search index sizes
		event := NewStorageEventBuilder().
			WithNamespaceId(nsMeta.StrId).
			WithTimestamp(time.Now()).
			WithDatabaseBytes(dbBytes).
			Build()
		events = append(events, event)

		log.Debug().Msgf("reporting storage for %s -> %+v", nsMeta.StrId, event)
	}

	return r.billingSvc.PushStorageEvents(r.ctx, events)
}
