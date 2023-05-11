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
	"testing"

	"github.com/tigrisdata/tigris/server/config"
)

func TestQueueMetrics(t *testing.T) {
	config.DefaultConfig.Tracing.Enabled = true
	config.DefaultConfig.Metrics.Enabled = true
	InitializeMetrics()

	t.Run("enabled", func(t *testing.T) {
		SetQueueSize(10)
		IncFailedJobError()
		IncFailedWorkerError()
	})

	t.Run("disabled", func(t *testing.T) {
		save := QueueMetrics
		t.Cleanup(func() { QueueMetrics = save })

		QueueMetrics = nil
		SetQueueSize(10)
		IncFailedJobError()
		IncFailedWorkerError()
	})
}
