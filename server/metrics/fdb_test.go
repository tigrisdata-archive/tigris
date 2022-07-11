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
	"github.com/uber-go/tally"
	"testing"
)

func TestFdbMetrics(t *testing.T) {
	config.DefaultConfig.Metrics.Fdb.Enabled = true
	InitializeMetrics()
	testNormalTags := []map[string]string{
		GetFdbReqTags("Commit", false),
		GetFdbReqTags("Insert", false),
		GetFdbReqTags("Insert", true),
	}

	testKnownErrorTags := []map[string]string{
		GetFdbReqSpecificErrorTags("Commit", "1", false),
		GetFdbReqSpecificErrorTags("Insert", "2", false),
		GetFdbReqSpecificErrorTags("Insert", "3", true),
	}

	config.DefaultConfig.Metrics.Fdb.Counters = true
	t.Run("Test FDB counters", func(t *testing.T) {
		for _, tags := range testNormalTags {
			FdbRequests.Tagged(tags).Counter("ok").Inc(1)
			FdbErrorRequests.Tagged(tags).Counter("unknown").Inc(1)
		}
		for _, tags := range testKnownErrorTags {
			FdbErrorRequests.Tagged(tags).Counter("specific").Inc(1)
		}
	})

	config.DefaultConfig.Metrics.Fdb.ResponseTime = true
	t.Run("Test FDB histograms", func(t *testing.T) {
		testHistogramTags := GetFdbReqTags("Insert", true)
		defer FdbMetrics.Tagged(testHistogramTags).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
	})
}
