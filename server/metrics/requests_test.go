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

	fullMethod := "/tigrisdata.v1.Tigris/TestMethod"

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

		for _, counterName := range InitializedServerRequestsCounterNames {
			GetServerRequestCounter(unaryEndPointMetadata.getFullMethod(), counterName)
		}

		for _, histogramName := range ServerRequestsHistogramNames {
			GetServerRequestHistogram(unaryEndPointMetadata.getFullMethod(), histogramName)
		}
	})

	t.Run("Test streaming metrics", func(t *testing.T) {
		InitServerRequestCounters(svcName, streamingMethodInfo)
		InitServerRequestHistograms(svcName, streamingMethodInfo)

		for _, counterName := range InitializedServerRequestsCounterNames {
			GetServerRequestCounter(streamingEndpointMetadata.getFullMethod(), counterName)
		}

		for _, histogramName := range ServerRequestsHistogramNames {
			GetServerRequestHistogram(streamingEndpointMetadata.getFullMethod(), histogramName)
		}
	})

	t.Run("Test specific error tags", func(t *testing.T) {
		error_tags := unaryEndPointMetadata.getSpecificErrorTags("test_source", "test_error")
		assert.Equal(t, error_tags["source"], "test_source")
		assert.Equal(t, error_tags["code"], "test_error")
	})

	t.Run("Test endpoint metadata from full method", func(t *testing.T) {
		unaryFromFullMethod := getGrpcEndPointMetadataFromFullMethod(fullMethod, "unary")
		assert.Equal(t, unaryFromFullMethod.grpcServiceName, svcName)
		assert.Equal(t, unaryFromFullMethod.grpcMethodName, "TestMethod")
		assert.Equal(t, unaryFromFullMethod.grpcTypeName, "unary")

		streamingFromFullMethod := getGrpcEndPointMetadataFromFullMethod(fullMethod, "stream")
		assert.Equal(t, streamingFromFullMethod.grpcServiceName, svcName)
		assert.Equal(t, streamingFromFullMethod.grpcMethodName, "TestMethod")
		assert.Equal(t, streamingFromFullMethod.grpcTypeName, "stream")
	})

	t.Run("Test specific error counter", func(t *testing.T) {
		errorCounterUnary := GetSpecificErrorCounter(fullMethod, "unary", "test_err_source", "test_err_code")
		assert.Equal(t, errorCounterUnary.Name, ServerRequestsSpecificErrorTotal)
		assert.Equal(t, errorCounterUnary.Tags["method"], "TestMethod")
		assert.Equal(t, errorCounterUnary.Tags["service"], svcName)
		assert.Equal(t, errorCounterUnary.Tags["type"], "unary")
		assert.Equal(t, errorCounterUnary.Tags["source"], "test_err_source")
		assert.Equal(t, errorCounterUnary.Tags["code"], "test_err_code")

		errorCounterStreaming := GetSpecificErrorCounter(fullMethod, "stream", "test_err_source", "test_err_code")
		assert.Equal(t, errorCounterStreaming.Name, ServerRequestsSpecificErrorTotal)
		assert.Equal(t, errorCounterStreaming.Tags["method"], "TestMethod")
		assert.Equal(t, errorCounterStreaming.Tags["service"], svcName)
		assert.Equal(t, errorCounterStreaming.Tags["type"], "stream")
		assert.Equal(t, errorCounterStreaming.Tags["source"], "test_err_source")
		assert.Equal(t, errorCounterStreaming.Tags["code"], "test_err_code")
	})
}
