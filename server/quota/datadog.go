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
	"time"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
)

var RunningAverageLength = 60 * time.Second

type Datadog struct {
	Datadog *metrics.Datadog
}

func (d *Datadog) CurRates(ctx context.Context, namespace string) (int64, int64, error) {
	r, err := d.Datadog.GetCurrentMetricValue(ctx, namespace, "tigris.quota.usage.read_units", api.TigrisOperation_ALL, RunningAverageLength)
	if err != nil {
		return 0, 0, err
	}

	w, err := d.Datadog.GetCurrentMetricValue(ctx, namespace, "tigris.quota.usage.write_units", api.TigrisOperation_ALL, RunningAverageLength)
	if err != nil {
		return 0, 0, err
	}

	return r, w, nil
}

func initDatadogMetrics(cfg *config.Config) *Datadog {
	log.Debug().Msg("initializing Datadog coordinated quota backend")
	return &Datadog{metrics.InitDatadog(cfg)}
}

type NoopMetrics struct{}

func (d *NoopMetrics) CurRates(_ context.Context, _ string) (int64, int64, error) {
	return 0, 0, nil
}

// This backend maximizes per node namespace quota usage.
func initNoopMetrics(_ *config.Config) *NoopMetrics {
	log.Debug().Msg("initializing Noop coordinated quota backend")
	return &NoopMetrics{}
}
