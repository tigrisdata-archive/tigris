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
	"github.com/tigrisdata/tigris/server/config"
	"io"
	"time"

	prom "github.com/m3db/prometheus_client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
)

var (
	root     tally.Scope
	Reporter promreporter.Reporter
	server   tally.Scope
	// GRPC and HTTP related metric scopes
	Requests         tally.Scope
	ErrorRequests    tally.Scope
	RequestsRespTime tally.Scope
	// Fdb related metric scopes
	FdbMetrics tally.Scope
)

func GetGlobalTags() map[string]string {
	return map[string]string{
		"service": config.DefaultConfig.Tags.Service,
		"env":     config.DefaultConfig.Tags.Environment,
		"version": config.DefaultConfig.Tags.Version,
	}
}

func InitializeMetrics() io.Closer {
	var closer io.Closer
	log.Debug().Msg("Initializing metrics")
	registry := prom.NewRegistry()
	Reporter = promreporter.NewReporter(promreporter.Options{Registerer: registry})
	root, closer = tally.NewRootScope(tally.ScopeOptions{
		Tags:           GetGlobalTags(),
		CachedReporter: Reporter,
		// Panics with .
		Separator: promreporter.DefaultSeparator,
	}, 1*time.Second)
	// Request level metrics (HTTP and GRPC)
	// metric names: tigris_server
	if config.DefaultConfig.Metrics.Grpc.Enabled {
		InitializeRequestScopes()
		// Request level metrics are initialized during GRPC server registration
	}
	// FDB level metrics
	if config.DefaultConfig.Metrics.Fdb.Enabled {
		FdbMetrics = root.SubScope("fdb")
		InitializeFdbScopes()
		InitializeFdbMetrics()
	}
	return closer
}
