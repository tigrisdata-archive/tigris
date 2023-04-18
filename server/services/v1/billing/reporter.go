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
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type UsageReporter struct {
	glbState   metrics.UsageProvider
	billingSvc Provider
	interval   time.Duration
	tenantMgr  metadata.NamespaceMetadataMgr
	ctx        context.Context
}

func NewUsageReporter(gs metrics.UsageProvider, tm metadata.NamespaceMetadataMgr, billing Provider) (*UsageReporter, error) {
	if gs == nil || tm == nil {
		return nil, errors.Internal("usage reporter cannot be initialized")
	}

	return &UsageReporter{
		glbState:   gs,
		billingSvc: billing,
		interval:   config.DefaultConfig.Billing.Reporter.RefreshInterval,
		tenantMgr:  tm,
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
	for range t.C {
		ulog.E(r.push())
	}
}

func (r *UsageReporter) push() error {
	chunk := r.glbState.Flush()
	log.Info().Msgf("reporting data for %d tenants", len(chunk.Tenants))

	if len(chunk.Tenants) == 0 {
		log.Info().Msg("no tenant data to report")
		return nil
	}

	trxnSuffix := strconv.FormatInt(chunk.EndTime.Unix(), 10)
	events := make([]*UsageEvent, 0, len(chunk.Tenants))

	// refresh and get metadata for all tenants if anyone is missing metronome integration
	for namespaceId := range chunk.Tenants {
		nsMeta := r.tenantMgr.GetNamespaceMetadata(r.ctx, namespaceId)
		if nsMeta == nil || nsMeta.Accounts.Metronome == nil {
			err := r.tenantMgr.RefreshNamespaceAccounts(r.ctx)
			if err != nil {
				log.Error().Err(err).Msg("failed to refresh namespace metadata")
			}
			break
		}
	}

	for namespaceId, stats := range chunk.Tenants {
		nsMeta := r.tenantMgr.GetNamespaceMetadata(r.ctx, namespaceId)
		if nsMeta == nil {
			log.Error().Msgf("invalid namespace id %s", namespaceId)
			continue
		}
		// todo: remove this extraneous logging
		log.Error().Msgf("received usage for id %s - %d, %d, %d", namespaceId, stats.ReadUnits, stats.WriteUnits, stats.SearchUnits)

		// create account if not yet
		_, enabled := nsMeta.Accounts.GetMetronomeId()
		if !enabled {
			log.Info().Msgf("creating Metronome account for %s", namespaceId)

			billingId, err := r.billingSvc.CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name)
			if !ulog.E(err) && billingId != uuid.Nil {
				nsMeta.Accounts.AddMetronome(billingId.String())

				// add default plan to the user
				added, err := r.billingSvc.AddDefaultPlan(r.ctx, billingId)
				if err != nil || !added {
					log.Error().Err(err).Msgf("error adding default plan to user id %s", namespaceId)
				}

				// save updated namespace metadata
				err = r.tenantMgr.UpdateNamespaceMetadata(r.ctx, *nsMeta)
				if err != nil {
					log.Error().Err(err).Msgf("error saving metadata for user %s", namespaceId)
				}
				// continue to send event, metronome will disregard it if account does not exist
			}
		}

		event := NewUsageEventBuilder().
			WithNamespaceId(nsMeta.StrId).
			WithTransactionId(nsMeta.StrId + "_" + trxnSuffix).
			WithTimestamp(chunk.EndTime).
			WithDatabaseUnits(stats.ReadUnits + stats.WriteUnits).
			WithSearchUnits(stats.SearchUnits).
			Build()
		events = append(events, event)

		log.Debug().Msgf("reporting usage for %s -> %+v", namespaceId, event)
		log.Info().Msgf("done reporting %d events", len(events))
	}

	err := r.billingSvc.PushUsageEvents(r.ctx, events)
	if err != nil {
		return err
	}
	return nil
}
