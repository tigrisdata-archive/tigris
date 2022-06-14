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
	"google.golang.org/grpc"
)

func TestGrpcMetrics(t *testing.T) {
	metrics.InitializeMetrics()
	svcName := "tigrisdata.v1.Tigris"
	methodName := "TestMethod"
	methodInfo := grpc.MethodInfo{
		Name:           methodName,
		IsServerStream: false,
		IsClientStream: false,
	}
	fullMethodName := "/tigrisdata.v1.Tigris/TestMethod"

	metrics.InitServerRequestCounters(svcName, methodInfo)
	metrics.InitServerRequestHistograms(svcName, methodInfo)

	t.Run("Test counters", func(t *testing.T) {
		receiveMessage(fullMethodName)
		handleMessage(fullMethodName)
		errorMessage(fullMethodName)
		okMessage(fullMethodName)
	})

	t.Run("Test histogram", func(t *testing.T) {
		getTimeHistogram(fullMethodName)
	})
}
