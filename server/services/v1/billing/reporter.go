// Copyright 2022-2023 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package billing

import (
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/rs/zerolog/log"
	"time"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/server/metadata"
	"context"
	"strconv"
	"github.com/google/uuid"
	"github.com/tigrisdata/tigris/errors"
)

type UsageReporter struct {
	glbState   *metrics.GlobalStatus
	billingSvc Provider
	interval   time.Duration
	tm         *metadata.TenantManager
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewUsageReporter(gs *metrics.GlobalStatus, tm *metadata.TenantManager) (*UsageReporter, error) {
	if gs == nil || tm == nil {
		return nil, errors.Internal("usage reporter cannot be initialized")
	}

	return &UsageReporter{
		glbState:   gs,
		billingSvc: NewProvider(),
		interval:   config.DefaultConfig.Billing.Reporter.RefreshInterval,
		tm:         tm,
		ctx:        context.Background(),
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
	for _ = range t.C {
		ulog.E(r.push())
	}
}

func (r *UsageReporter) push() error {
	chunk := r.glbState.Flush()

	if len(chunk.Tenants) == 0 {
		log.Info().Msg("no tenant data to report")
		return nil
	}

	trxnSuffix := strconv.FormatInt(chunk.EndTime.Unix(), 10)
	var events []*UsageEvent

	for tenantName, stats := range chunk.Tenants {
		tenant, err := r.tm.GetTenant(r.ctx, tenantName)
		if err != nil {
			return err
		}
		nsMeta := tenant.GetNamespace().Metadata()

		// create account if not yet
		_, enabled := nsMeta.Accounts.GetMetronomeId()
		if !enabled {
			log.Info().Msg("creating Metronome account")
			billingId, err := r.billingSvc.CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name)
			if !ulog.E(err) && billingId != uuid.Nil {
				nsMeta.Accounts.AddMetronome(billingId.String())
				// todo: save namespace metadata

				added, err := r.billingSvc.AddDefaultPlan(r.ctx, billingId)
				if err != nil || !added {
					log.Error().Err(err).Msg("error adding default plan to customer")
				}
				// continue to send event, metronome will disregard it if account does not exist
			}
		}

		event := NewUsageEventBuilder().
			WithNamespaceId(nsMeta.StrId).
			WithTransactionId(nsMeta.StrId + "_" + trxnSuffix).
			WithTimestamp(chunk.EndTime).
			WithDatabaseUnits(stats.ReadUnits + stats.WriteUnits).
			Build()
		events = append(events, event)

		if err != nil {
			return err
		}
		log.Debug().Msgf("reporting usage for %s -> %+v", tenantName, event)
	}

	err := r.billingSvc.PushUsageEvents(r.ctx, events)
	if err != nil {
		return err
	}
	return nil
}
