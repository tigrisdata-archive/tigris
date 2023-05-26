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
	"strconv"

	"github.com/tigrisdata/tigris/server/config"
	"github.com/uber-go/tally"
)

var (
	MetronomeRequestOk         tally.Scope
	MetronomeRequestError      tally.Scope
	MetronomeResponseTime      tally.Scope
	MetronomeErrorResponseTime tally.Scope
	MetronomeIngest            tally.Scope
)

func initializeMetronomeScopes() {
	MetronomeRequestOk = MetronomeMetrics.SubScope("count")
	MetronomeRequestError = MetronomeMetrics.SubScope("count")
	MetronomeResponseTime = MetronomeMetrics.SubScope("response")
	MetronomeErrorResponseTime = MetronomeMetrics.SubScope("error_response")
	MetronomeIngest = MetronomeMetrics.SubScope("ingest")
}

func getMetronomeTagKeys() []string {
	return []string{
		"env",
		"operation",
		"response_code",
	}
}

func GetMetronomeBaseTags(operation string) map[string]string {
	return map[string]string{
		"env":       config.GetEnvironment(),
		"operation": operation,
	}
}

func GetMetronomeResponseCodeTags(code int) map[string]string {
	return map[string]string{
		"response_code": strconv.Itoa(code),
	}
}

func GetMetronomeIngestEventTags(eventType string) map[string]string {
	return map[string]string{
		"event_type": eventType,
	}
}
