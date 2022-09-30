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

package metrics

import (
	"io"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
)

var (
	root           tally.Scope
	Reporter       promreporter.Reporter
	Requests       tally.Scope
	FdbMetrics     tally.Scope
	SearchMetrics  tally.Scope
	SessionMetrics tally.Scope
	SizeMetrics    tally.Scope
	NetworkMetrics tally.Scope
	AuthMetrics    tally.Scope
)

func getVersion() string {
	if util.Version != "" {
		return util.Version
	} else {
		return "dev"
	}
}

func GetGlobalTags() map[string]string {
	return map[string]string{
		"service": util.Service,
		"env":     config.GetEnvironment(),
		"version": getVersion(),
	}
}

func getTigrisDefaultSummaryObjectives() map[float64]float64 {
	return map[float64]float64{
		0.5:   0.01,
		0.75:  0.001,
		0.95:  0.001,
		0.99:  0.001,
		0.999: 0.0001,
	}
}

func getTimerSummaryObjectives() map[float64]float64 {
	res := make(map[float64]float64)
	for confQuantile, objective := range getTigrisDefaultSummaryObjectives() {
		for _, wantedQuantile := range config.DefaultConfig.Metrics.TimerQuantiles {
			if confQuantile == wantedQuantile {
				res[confQuantile] = objective
			}
		}
	}
	return res
}

func InitializeMetrics() io.Closer {
	var closer io.Closer
	log.Debug().Msg("Initializing metrics")
	Reporter = promreporter.NewReporter(promreporter.Options{
		DefaultSummaryObjectives: getTimerSummaryObjectives(),
	})
	root, closer = tally.NewRootScope(tally.ScopeOptions{
		Tags:           GetGlobalTags(),
		CachedReporter: Reporter,
		// Panics with .
		Separator: promreporter.DefaultSeparator,
	}, 1*time.Second)
	if config.DefaultConfig.Metrics.Enabled {
		if config.DefaultConfig.Metrics.Requests.Enabled {
			// Request level metrics (HTTP and GRPC)
			Requests = root.SubScope("requests")
			initializeRequestScopes()
		}
		if config.DefaultConfig.Metrics.Fdb.Enabled {
			// FDB level metrics
			FdbMetrics = root.SubScope("fdb")
			initializeFdbScopes()
		}
		if config.DefaultConfig.Metrics.Search.Enabled {
			// Search level metrics
			SearchMetrics = root.SubScope("search")
			initializeSearchScopes()
		}
		if config.DefaultConfig.Metrics.Session.Enabled {
			// Session level metrics
			SessionMetrics = root.SubScope("session")
			initializeSessionScopes()
		}
		if config.DefaultConfig.Metrics.Size.Enabled {
			// Size metrics
			SizeMetrics = root.SubScope("size")
			initializeSizeScopes()
		}
		if config.DefaultConfig.Metrics.Network.Enabled {
			// Network metrics
			NetworkMetrics = root.SubScope("net")
			initializeNetworkScopes()
		}
		if config.DefaultConfig.Metrics.Auth.Enabled {
			// Auth metrics
			AuthMetrics = root.SubScope("auth")
			initializeAuthScopes()
		}
	}
	return closer
}
