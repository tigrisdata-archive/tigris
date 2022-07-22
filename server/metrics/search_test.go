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
	"context"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/uber-go/tally"
	"testing"
)

func TestSearchMetrics(t *testing.T) {
	config.DefaultConfig.Metrics.Search.Enabled = true
	InitializeMetrics()

	ctx := context.Background()

	testNormalTags := []map[string]string{
		GetSearchTags(ctx, "CreateCollection"),
		GetSearchTags(ctx, "UpdateCollection"),
		GetSearchTags(ctx, "DropCollection"),
		GetSearchTags(ctx, "IndexDocuments"),
		GetSearchTags(ctx, "DeleteDocuments"),
		GetSearchTags(ctx, "Search"),
	}

	config.DefaultConfig.Metrics.Search.Counters = true
	t.Run("Test Search counters", func(t *testing.T) {
		for _, tags := range testNormalTags {
			SearchRequests.Tagged(tags).Counter("ok").Inc(1)
			SearchErrorRequests.Tagged(tags).Counter("unknown").Inc(1)
		}
	})

	config.DefaultConfig.Metrics.Search.ResponseTime = true
	t.Run("Test Search histograms", func(t *testing.T) {
		testHistogramTags := GetSearchTags(ctx, "IndexDocuments")
		defer SearchMetrics.Tagged(testHistogramTags).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
	})
}
