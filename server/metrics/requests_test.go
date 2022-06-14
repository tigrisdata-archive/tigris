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
	"google.golang.org/grpc"
)

func TestGRPCMetrics(t *testing.T) {
	InitializeMetrics()
	svcName := "tigrisdata.v1.Tigris"
	unaryMethodName := "TestUnaryMethod"
	streamingMethodName := "TestStreamingMethod"

	unaryMethodInfo := grpc.MethodInfo{
		Name:           unaryMethodName,
		IsServerStream: false,
		IsClientStream: false,
	}
	streamingMethodInfo := grpc.MethodInfo{
		Name:           streamingMethodName,
		IsServerStream: true,
		IsClientStream: false,
	}

	unaryEndPointMetadata := getGrpcEndPointMetadata(svcName, unaryMethodInfo)
	streamingEndpointMetadata := getGrpcEndPointMetadata(svcName, streamingMethodInfo)

	t.Run("Test unary endpoint metadata", func(t *testing.T) {
		assert.Equal(t, unaryEndPointMetadata.grpcServiceName, svcName)
		assert.Equal(t, unaryEndPointMetadata.grpcMethodName, unaryMethodName)
		assert.Equal(t, unaryEndPointMetadata.grpcTypeName, "unary")
	})

	t.Run("Test streaming endpoint metadata", func(t *testing.T) {
		assert.Equal(t, streamingEndpointMetadata.grpcServiceName, svcName)
		assert.Equal(t, streamingEndpointMetadata.grpcMethodName, streamingMethodName)
		assert.Equal(t, streamingEndpointMetadata.grpcTypeName, "stream")
	})

	t.Run("Test tags", func(t *testing.T) {
		tags := unaryEndPointMetadata.getTags()
		for tagName, tagValue := range tags {
			switch tagName {
			case "grpc_method":
				assert.Equal(t, tagValue, "TestUnaryMethod")
			case "grpc_service":
				assert.Equal(t, tagValue, "tigrisdata.v1.Tigris")
			case "grpc_type":
				assert.Equal(t, tagValue, "unary")
			}
		}
	})

	t.Run("Test streaming tags", func(t *testing.T) {
		tags := streamingEndpointMetadata.getTags()
		for tagName, tagValue := range tags {
			switch tagName {
			case "grpc_method":
				assert.Equal(t, tagValue, "TestStreamingMethod")
			case "grpc_service":
				assert.Equal(t, tagValue, "tigrisdata.v1.Tigris")
			case "grpc_type":
				assert.Equal(t, tagValue, "stream")
			}
		}
	})

	t.Run("Test unary metrics", func(t *testing.T) {
		InitServerRequestCounters(svcName, unaryMethodInfo)
		InitServerRequestHistograms(svcName, unaryMethodInfo)

		for _, counterName := range ServerRequestsCounterNames {
			GetServerRequestCounter(unaryEndPointMetadata.getFullMethod(), counterName)
		}

		for _, histogramName := range ServerRequestsHistogramNames {
			GetServerRequestHistogram(unaryEndPointMetadata.getFullMethod(), histogramName)
		}
	})

	t.Run("Test streaming metrics", func(t *testing.T) {
		InitServerRequestCounters(svcName, streamingMethodInfo)
		InitServerRequestHistograms(svcName, streamingMethodInfo)

		for _, counterName := range ServerRequestsCounterNames {
			GetServerRequestCounter(streamingEndpointMetadata.getFullMethod(), counterName)
		}

		for _, histogramName := range ServerRequestsHistogramNames {
			GetServerRequestHistogram(streamingEndpointMetadata.getFullMethod(), histogramName)
		}
	})
}
