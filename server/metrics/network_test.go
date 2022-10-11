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

func TestNetworkMetrics(t *testing.T) {
	config.DefaultConfig.Metrics.Enabled = true
	InitializeMetrics()
	testMeasurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())

	t.Run("Test bytes send", func(t *testing.T) {
		testMeasurement.CountSentBytes(BytesSent, testMeasurement.GetNetworkTags(), 100)
	})

	t.Run("Test bytes received", func(t *testing.T) {
		testMeasurement.CountReceivedBytes(BytesReceived, testMeasurement.GetNetworkTags(), 100)
	})
}
