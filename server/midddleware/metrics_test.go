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

package middleware

import (
	"testing"

	"github.com/tigrisdata/tigris/server/metrics"
)

func TestGRPCMetrics(t *testing.T) {
	metrics.InitializeMetrics()
	callData := newGrpcReqMetrics("/tigrisdata.v1.Tigris/TestMethod", "unary")

	t.Run("Increase test_counter", func(t *testing.T) {
		testCounter := callData.getGrpcCounter("test_counter")
		callData.increaseGrpcCounter(testCounter, 1)
	})

	t.Run("Test message counters", func(t *testing.T) {
		callData.receiveMessage()
		callData.handleMessage()
		callData.errorMessage()
		callData.okMessage()
	})

	t.Run("Test histogram", func(t *testing.T) {
		testHistogram := callData.getTimeHistogram()
		stopWatch := testHistogram.Start()
		stopWatch.Stop()
	})

}
