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

package metrics

import (
	"io"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
)

var (
	root                  tally.Scope
	Reporter              promreporter.Reporter
	Requests              tally.Scope
	FdbMetrics            tally.Scope
	SearchMetrics         tally.Scope
	SecondaryIndexMetrics tally.Scope
	SessionMetrics        tally.Scope
	SizeMetrics           tally.Scope
	QuotaMetrics          tally.Scope
	NetworkMetrics        tally.Scope
	AuthMetrics           tally.Scope
	SchemaMetrics         tally.Scope
	MetronomeMetrics      tally.Scope
	GlobalSt              *GlobalStatus
)

func getVersion() string {
	if util.Version != "" {
		return util.Version
	}
	return "dev"
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

func SchemaReadOutdated(project string, branch string, collection string) {
	if SchemaMetrics != nil {
		SchemaMetrics.Tagged(GetProjectBranchCollTags(project, branch, collection)).Counter("read_outdated").Inc(1)
	}
}

func SchemaUpdateRepaired(project string, branch string, collection string) {
	if SchemaMetrics != nil {
		SchemaMetrics.Tagged(GetProjectBranchCollTags(project, branch, collection)).Counter("update_repaired").Inc(1)
	}
}

func InitializeMetrics() func() {
	var closer io.Closer
	if cfg := config.DefaultConfig.Metrics; cfg.Enabled {
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

		if cfg.Requests.Enabled {
			// Request level metrics (HTTP and GRPC)
			Requests = root.SubScope("requests")
			initializeRequestScopes()
		}
		if cfg.Fdb.Enabled {
			// FDB level metrics
			FdbMetrics = root.SubScope("fdb")
			initializeFdbScopes()
		}
		if cfg.Search.Enabled {
			// Search level metrics
			SearchMetrics = root.SubScope("search")
			initializeSearchScopes()
		}
		if cfg.Session.Enabled {
			// Session level metrics
			SessionMetrics = root.SubScope("session")
			initializeSessionScopes()
		}
		if cfg.Size.Enabled {
			// Size metrics
			SizeMetrics = root.SubScope("size")
			initializeSizeScopes()
		}
		if cfg.Network.Enabled {
			// Network metrics
			NetworkMetrics = root.SubScope("net")
			initializeNetworkScopes()
		}
		if cfg.Auth.Enabled {
			// Auth metrics
			AuthMetrics = root.SubScope("auth")
			initializeAuthScopes()
		}

		if cfg.SecondaryIndex.Enabled {
			// Secondary Index metrics
			SecondaryIndexMetrics = root.SubScope("secondary_index")
			initializeSecondaryIndexScopes()
		}

		if cfg.Queue.Enabled {
			QueueMetrics = root.SubScope("queue")
			initializeQueueScopes()
		}

		// Metrics for Metronome - external billing service
		MetronomeMetrics = root.SubScope("metronome")
		initializeMetronomeScopes()

		initializeQuotaScopes()

		SchemaMetrics = root.SubScope("schema")
		GlobalSt = NewGlobalStatus()
	}

	return func() {
		if closer != nil {
			ulog.E(closer.Close())
		}
	}
}
