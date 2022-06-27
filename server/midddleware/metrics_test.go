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
	methodType := "unary"
	methodInfo := grpc.MethodInfo{
		Name:           methodName,
		IsServerStream: false,
		IsClientStream: false,
	}
	fullMethodName := "/tigrisdata.v1.Tigris/TestMethod"

	metrics.InitServerRequestMetrics(svcName, methodInfo)

	t.Run("Test tigris server counters", func(t *testing.T) {
		countReceivedMessage(fullMethodName, methodType)
		countHandledMessage(fullMethodName, methodType)
		countUnknownErrorMessage(fullMethodName, methodType)
		countOkMessage(fullMethodName, methodType)
		countSpecificErrorMessage(fullMethodName, methodType, "test_source", "test_code")
	})
}

func TestFdbMetrics(t *testing.T) {
	metrics.InitializeFdbMetrics()

	testNormalTags := []map[string]string{
		metrics.GetFdbReqTags("Commit", false),
		metrics.GetFdbReqTags("Insert", false),
		metrics.GetFdbReqTags("Insert", true),
	}

	testKnownErrorTags := []map[string]string{
		metrics.GetFdbReqSpecificErrorTags("Commit", "1", false),
		metrics.GetFdbReqSpecificErrorTags("Insert", "2", false),
		metrics.GetFdbReqSpecificErrorTags("Insert", "3", true),
	}

	t.Run("Test FDB counters", func(t *testing.T) {
		for _, tags := range testNormalTags {
			metrics.FdbRequests.Tagged(tags).Counter("ok").Inc(1)
			metrics.FdbErrorRequests.Tagged(tags).Counter("unknown").Inc(1)
		}
		for _, tags := range testKnownErrorTags {
			metrics.FdbErrorRequests.Tagged(tags).Counter("specific").Inc(1)
		}
	})
}
