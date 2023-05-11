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
	"github.com/uber-go/tally"
)

var (
	QueueMetrics tally.Scope
	QueueErrors  tally.Scope
	QueueOk      tally.Scope
)

func initializeQueueScopes() {
	QueueErrors = QueueMetrics.SubScope("errors")
	QueueOk = QueueMetrics.SubScope("ok")
}

func SetQueueSize(size int) {
	if QueueMetrics == nil {
		return
	}
	QueueOk.Gauge("size").Update(float64(size))
}

func IncFailedJobError() {
	if QueueMetrics == nil {
		return
	}
	QueueErrors.Counter("job").Inc(1)
}

func IncFailedWorkerError() {
	if QueueMetrics == nil {
		return
	}
	QueueErrors.Counter("worker").Inc(1)
}
