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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tigrisdata/tigris/server/config"
)

func TestSearchMetrics(t *testing.T) {
	config.DefaultConfig.Tracing.Enabled = true
	InitializeMetrics()

	testNormalTags := []map[string]string{
		GetSearchTags("CreateCollection"),
		GetSearchTags("UpdateCollection"),
		GetSearchTags("DropCollection"),
		GetSearchTags("IndexDocuments"),
		GetSearchTags("DeleteDocuments"),
		GetSearchTags("Search"),
	}

	t.Run("Test search tags", func(t *testing.T) {
		assert.Greater(t, len(getSearchOkTagKeys()), 2)
		assert.Greater(t, len(getSearchErrorTagKeys()), 2)
	})

	t.Run("Test Search counters", func(t *testing.T) {
		for _, tags := range testNormalTags {
			SearchOkCount.Tagged(tags).Counter("ok").Inc(1)
			SearchErrorCount.Tagged(tags).Counter("unknown").Inc(1)
		}
	})

	t.Run("Test Search timers", func(t *testing.T) {
		testTimerTags := GetSearchTags("IndexDocuments")
		defer SearchRespTime.Tagged(testTimerTags).Timer("time").Start().Stop()
	})
}
