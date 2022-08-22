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
	"testing"

	"github.com/tigrisdata/tigris/server/config"
)

func TestSearchMetrics(t *testing.T) {
	config.DefaultConfig.Tracing.Enabled = true
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

	t.Run("Test Search counters", func(t *testing.T) {
		for _, tags := range testNormalTags {
			SearchOkRequests.Tagged(tags).Counter("ok").Inc(1)
			SearchErrorRequests.Tagged(tags).Counter("unknown").Inc(1)
		}
	})

	t.Run("Test Search timers", func(t *testing.T) {
		testTimerTags := GetSearchTags(ctx, "IndexDocuments")
		defer SearchRespTime.Tagged(testTimerTags).Timer("time").Start().Stop()
	})
}
