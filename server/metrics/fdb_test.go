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

	"github.com/stretchr/testify/assert"

	"github.com/tigrisdata/tigris/server/config"
)

func TestFdbMetrics(t *testing.T) {
	config.DefaultConfig.Tracing.Enabled = true
	InitializeMetrics()

	ctx := context.Background()

	testNormalTags := []map[string]string{
		GetFdbOkTags(ctx, "Commit"),
		GetFdbOkTags(ctx, "Insert"),
		GetFdbOkTags(ctx, "Insert"),
	}

	testKnownErrorTags := []map[string]string{
		GetFdbErrorTags(ctx, "Commit", "1"),
		GetFdbErrorTags(ctx, "Insert", "2"),
		GetFdbErrorTags(ctx, "Insert", "3"),
	}

	t.Run("Test fdb tags", func(t *testing.T) {
		assert.Greater(t, len(getFdbOkTagKeys()), 2)
		assert.Greater(t, len(getFdbTimerTagKeys()), 2)
		assert.Greater(t, len(getFdbErrorTagKeys()), 2)
	})

	t.Run("Test FDB counters", func(t *testing.T) {
		for _, tags := range testNormalTags {
			FdbOkRequests.Tagged(tags).Counter("ok").Inc(1)
			FdbErrorRequests.Tagged(tags).Counter("unknown").Inc(1)
		}
		for _, tags := range testKnownErrorTags {
			FdbErrorRequests.Tagged(tags).Counter("specific").Inc(1)
		}
	})

	t.Run("Test FDB timers", func(t *testing.T) {
		testTimerTags := GetFdbOkTags(ctx, "Insert")
		defer FdbMetrics.Tagged(testTimerTags).Timer("time").Start().Stop()
	})
}
