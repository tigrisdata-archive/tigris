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

	"github.com/tigrisdata/tigris/server/config"
)

func TestSessionMetrics(t *testing.T) {
	config.DefaultConfig.Tracing.Enabled = true
	InitializeMetrics()

	testTags := []map[string]string{
		GetSessionTags("Create"),
		GetSessionTags("Get"),
		GetSessionTags("Remove"),
		GetSessionTags("Execute"),
		GetSessionTags("executeWithRetry"),
	}

	t.Run("Test Session counters", func(t *testing.T) {
		for _, tags := range testTags {
			SessionOkRequests.Tagged(tags).Counter("ok").Inc(1)
			SessionErrorRequests.Tagged(tags).Counter("error").Inc(1)
		}
	})

	t.Run("Test Session timers", func(t *testing.T) {
		tags := GetSessionTags("Create")
		defer SessionRespTime.Tagged(tags).Timer("time").Start().Stop()
	})
}
