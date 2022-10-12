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
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
)

func TestGRPCMetrics(t *testing.T) {
	config.DefaultConfig.Tracing.Enabled = true
	config.DefaultConfig.Metrics.Enabled = true
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

	ctx := context.Background()

	unaryEndPointMetadata := request.NewRequestEndpointMetadata(ctx, svcName, unaryMethodInfo)
	streamingEndpointMetadata := request.NewRequestEndpointMetadata(ctx, svcName, streamingMethodInfo)

	t.Run("Test GetGrpcEndPointMetadataFromFullMethod", func(t *testing.T) {
		unaryMetadata := request.GetGrpcEndPointMetadataFromFullMethod(ctx, unaryEndPointMetadata.GetFullMethod(), "unary")
		assert.Equal(t, unaryMetadata, unaryEndPointMetadata)

		streamMetadata := request.GetGrpcEndPointMetadataFromFullMethod(ctx, streamingEndpointMetadata.GetFullMethod(), "stream")
		assert.Equal(t, streamMetadata, streamingEndpointMetadata)
	})

	t.Run("Test full method names", func(t *testing.T) {
		assert.Equal(t, unaryEndPointMetadata.GetFullMethod(), fmt.Sprintf("/%s/%s", svcName, unaryMethodName))
		assert.Equal(t, streamingEndpointMetadata.GetFullMethod(), fmt.Sprintf("/%s/%s", svcName, streamingMethodName))
	})

	t.Run("Test unary endpoint metadata", func(t *testing.T) {
		assert.Equal(t, unaryEndPointMetadata.GetServiceName(), svcName)
		assert.Equal(t, unaryEndPointMetadata.GetMethodInfo().Name, unaryMethodName)
		assert.False(t, unaryEndPointMetadata.GetMethodInfo().IsServerStream)
	})

	t.Run("Test streaming endpoint metadata", func(t *testing.T) {
		assert.Equal(t, streamingEndpointMetadata.GetServiceName(), svcName)
		assert.Equal(t, streamingEndpointMetadata.GetMethodInfo().Name, streamingMethodName)
		assert.True(t, streamingEndpointMetadata.GetMethodInfo().IsServerStream)
	})

	t.Run("Test unary method preinitialized tags", func(t *testing.T) {
		tags := unaryEndPointMetadata.GetInitialTags()
		for tagName, tagValue := range tags {
			if tagName == "grpc_method" {
				assert.Equal(t, tagValue, "TestUnaryMethod")
			}
		}
	})

	t.Run("Test streaming method preinitialized tags", func(t *testing.T) {
		tags := streamingEndpointMetadata.GetInitialTags()
		for tagName, tagValue := range tags {
			if tagName == "grpc_method" {
				assert.Equal(t, tagValue, "TestStreamingMethod")
			}
		}
	})
}
