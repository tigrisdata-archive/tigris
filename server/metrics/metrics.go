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

	prom "github.com/m3db/prometheus_client_golang/prometheus"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
)

var (
	Root     tally.Scope
	Reporter promreporter.Reporter
	// Both counters and histograms are initialized during server startup in muxer.go
	// method name and counter name
	ServerRequestCounters map[string]map[string]*ServerRequestCounter
	// method name and histogram name
	ServerRequestHistograms        map[string]map[string]*ServerRequestHistogram
	ServerRequestErrorCodeCounters map[string]map[string]map[string]*ServerRequestCounter
)

func InitializeMetrics() io.Closer {
	var closer io.Closer
	registry := prom.NewRegistry()
	Reporter = promreporter.NewReporter(promreporter.Options{Registerer: registry})
	Root, closer = tally.NewRootScope(tally.ScopeOptions{
		Prefix:         "tigris",
		Tags:           map[string]string{},
		CachedReporter: Reporter,
		Separator:      promreporter.DefaultSeparator,
	}, 1*time.Second)

	// Request level metrics (HTTP and GRPC)
	// These are populated at the time of initializing the GRPC servers
	ServerRequestCounters = make(map[string]map[string]*ServerRequestCounter)
	ServerRequestHistograms = make(map[string]map[string]*ServerRequestHistogram)
	// Error code counters are created once and on the fly, then stored here
	// By full method name, error type and code
	ServerRequestErrorCodeCounters = make(map[string]map[string]map[string]*ServerRequestCounter)

	return closer
}
