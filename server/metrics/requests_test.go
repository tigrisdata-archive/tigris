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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/server/config"
	"google.golang.org/grpc"
)

func TestGRPCMetrics(t *testing.T) {
	InitializeMetrics()
	config.DefaultConfig.Tracing.Enabled = true

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

	ctx := context.Background()

	unaryEndPointMetadata := newRequestEndpointMetadata(ctx, svcName, unaryMethodInfo)
	streamingEndpointMetadata := newRequestEndpointMetadata(ctx, svcName, streamingMethodInfo)

	t.Run("Test GetGrpcEndPointMetadataFromFullMethod", func(t *testing.T) {
		unaryMetadata := GetGrpcEndPointMetadataFromFullMethod(ctx, unaryEndPointMetadata.getFullMethod(), "unary")
		assert.Equal(t, unaryMetadata, unaryEndPointMetadata)

		streamMetadata := GetGrpcEndPointMetadataFromFullMethod(ctx, streamingEndpointMetadata.getFullMethod(), "stream")
		assert.Equal(t, streamMetadata, streamingEndpointMetadata)
	})

	t.Run("Test full method names", func(t *testing.T) {
		assert.Equal(t, unaryEndPointMetadata.getFullMethod(), fmt.Sprintf("/%s/%s", svcName, unaryMethodName))
		assert.Equal(t, streamingEndpointMetadata.getFullMethod(), fmt.Sprintf("/%s/%s", svcName, streamingMethodName))
	})

	t.Run("Test unary endpoint metadata", func(t *testing.T) {
		assert.Equal(t, unaryEndPointMetadata.serviceName, svcName)
		assert.Equal(t, unaryEndPointMetadata.methodInfo.Name, unaryMethodName)
		assert.False(t, unaryEndPointMetadata.methodInfo.IsServerStream)
	})

	t.Run("Test streaming endpoint metadata", func(t *testing.T) {
		assert.Equal(t, streamingEndpointMetadata.serviceName, svcName)
		assert.Equal(t, streamingEndpointMetadata.methodInfo.Name, streamingMethodName)
		assert.True(t, streamingEndpointMetadata.methodInfo.IsServerStream)
	})

	t.Run("Test unary method preinitialized tags", func(t *testing.T) {
		tags := unaryEndPointMetadata.GetTags()
		for tagName, tagValue := range tags {
			switch tagName {
			case "grpc_method":
				assert.Equal(t, tagValue, "TestUnaryMethod")
			case "grpc_service":
				assert.Equal(t, tagValue, "tigrisdata.v1.Tigris")
			}
		}
	})

	t.Run("Test streaming method preinitialized tags", func(t *testing.T) {
		tags := streamingEndpointMetadata.GetTags()
		for tagName, tagValue := range tags {
			switch tagName {
			case "grpc_method":
				assert.Equal(t, tagValue, "TestStreamingMethod")
			case "grpc_service":
				assert.Equal(t, tagValue, "tigrisdata.v1.Tigris")
			}
		}
	})
}
