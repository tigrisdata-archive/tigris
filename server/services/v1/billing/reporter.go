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
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type UsageReporter struct {
	glbState     metrics.UsageProvider
	billingSvc   Provider
	interval     time.Duration
	nsMgr        metadata.NamespaceMetadataMgr
	tenantGetter metadata.TenantGetter
	ctx          context.Context
	txMgr        *transaction.Manager
}

func NewUsageReporter(
	gs metrics.UsageProvider,
	tm metadata.NamespaceMetadataMgr,
	tg metadata.TenantGetter,
	billing Provider,
	txMgr *transaction.Manager,
) (*UsageReporter, error) {
	if gs == nil || tm == nil || txMgr == nil {
		return nil, errors.Internal("usage reporter cannot be initialized")
	}

	return &UsageReporter{
		glbState:     gs,
		billingSvc:   billing,
		interval:     config.DefaultConfig.Billing.Reporter.RefreshInterval,
		nsMgr:        tm,
		tenantGetter: tg,
		ctx:          context.Background(),
		txMgr:        txMgr,
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
		if nsMeta == nil || nsMeta.Accounts == nil || nsMeta.Accounts.Metronome == nil {
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
			log.Error().Str("ns", namespaceId).Msgf("namespace metadata cannot be nil")
			continue
		}
		if success := r.setupBillingAccount(nsMeta); !success {
			continue
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

// returns false if account cannot be setup, no data should be reported in that case
// returns true, if billing account already exists or new account was setup.
func (r *UsageReporter) setupBillingAccount(nsMeta *metadata.NamespaceMetadata) bool {
	if nsMeta == nil {
		return false
	}

	if nsMeta.Accounts == nil {
		nsMeta.Accounts = &metadata.AccountIntegrations{}
	}

	if len(nsMeta.StrId) == 0 || nsMeta.StrId == defaults.DefaultNamespaceName {
		// invalid namespace id, permanently disable account creation
		nsMeta.Accounts.DisableMetronome()
		err := r.nsMgr.UpdateNamespaceMetadata(r.ctx, *nsMeta)
		if err != nil {
			log.Error().Err(err).Str("ns", nsMeta.StrId).Msgf("error saving metadata for %s", nsMeta.StrId)
		}
		return false
	}
	if id, enabled := nsMeta.Accounts.GetMetronomeId(); !enabled {
		return false
	} else if len(id) > 0 {
		return true
	}

	log.Info().Str("ns", nsMeta.StrId).Msgf("creating Metronome account for %s", nsMeta.StrId)
	billingId, err := r.billingSvc.CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name)
	if !ulog.E(err) && billingId != uuid.Nil {
		// add default plan to the user
		added, err := r.billingSvc.AddDefaultPlan(r.ctx, billingId)
		if err != nil || !added {
			log.Error().Err(err).Str("ns", nsMeta.StrId).Msgf("error adding default plan %s", nsMeta.StrId)
		}
	} else {
		// check if we received HTTP 409 from billing svc, in that case namespaceId already exists there
		var metronomeError *MetronomeError
		if !(errors.As(err, &metronomeError) && metronomeError.HttpCode == 409) {
			return false
		}
		// get id from billing svc and store it
		if billingId, err = r.billingSvc.GetAccountId(r.ctx, nsMeta.StrId); ulog.E(err) || billingId == uuid.Nil {
			return true
		}
	}

	nsMeta.Accounts.AddMetronome(billingId.String())
	// save updated namespace metadata
	err = r.nsMgr.UpdateNamespaceMetadata(r.ctx, *nsMeta)
	if err != nil {
		log.Error().Err(err).Str("ns", nsMeta.StrId).Msgf("error saving metadata for %s", nsMeta.StrId)
	}
	return true
}

func (r *UsageReporter) pushStorage() error {
	tenants := r.tenantGetter.AllTenants(r.ctx)
	log.Info().Msgf("reporting storage data for %d tenants", len(tenants))

	events := make([]*StorageEvent, 0, len(tenants))
	for _, t := range tenants {
		nsMeta := t.GetNamespace().Metadata()
		if nsMeta.Accounts == nil {
			nsMeta.Accounts = &metadata.AccountIntegrations{}
		}

		if id, enabled := nsMeta.Accounts.GetMetronomeId(); len(id) == 0 || !enabled {
			// user doesn't have metronome integration; skip
			continue
		}
		var dbBytes, indexBytes int64
		// ensure size available
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

		tx, err := r.txMgr.StartTx(r.ctx)
		if err != nil {
			log.Error().Err(err).Str("ns", nsMeta.StrId).Msgf("failed to start transaction")
		}
		searchStats, err := t.SearchSize(r.ctx, tx)
		if err != nil || searchStats == nil {
			log.Error().Err(err).Str("ns", nsMeta.StrId).Msgf("tenant.SearchSize() failed")
		} else {
			indexBytes += searchStats.StoredBytes
		}

		event := NewStorageEventBuilder().
			WithNamespaceId(nsMeta.StrId).
			WithTimestamp(time.Now()).
			WithDatabaseBytes(dbBytes).
			WithIndexBytes(indexBytes).
			Build()
		events = append(events, event)

		log.Debug().Msgf("reporting storage for %s -> %+v", nsMeta.StrId, event)
	}

	return r.billingSvc.PushStorageEvents(r.ctx, events)
}
