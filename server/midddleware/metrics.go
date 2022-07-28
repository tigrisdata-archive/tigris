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
	"context"
	"errors"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
)

// There is no need to handle metrics configuration in the interceptors themselves, because they are not
// loaded if the metrics are not enabled. It is more efficient this way, but if we want dynamic reloading,
// we may want to change this in the future.

func countOkMessage(grpcMeta metrics.RequestEndpointMetadata) {
	// tigris_server_requests_ok
	metrics.Requests.Tagged(grpcMeta.GetTags()).Counter("ok").Inc(1)
}

func countUnknownErrorMessage(grpcMeta metrics.RequestEndpointMetadata) {
	// tigris_server_requests_error
	metrics.ErrorRequests.Tagged(grpcMeta.GetTags()).Counter("unknown").Inc(1)
}

func countSpecificErrorMessage(grpcMeta metrics.RequestEndpointMetadata, errSource string, errCode string) {
	// tigris_server_requests_error
	// For specific errors the tags are not pre-initialized because it has the error code in it
	metrics.ErrorRequests.Tagged(grpcMeta.GetSpecificErrorTags(errSource, errCode)).Counter("specific").Inc(1)
}

func metricsUnaryServerInterceptorResponseTime() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		grpcMeta := metrics.GetGrpcEndPointMetadataFromFullMethod(ctx, info.FullMethod, "unary")
		defer metrics.RequestsRespTime.Tagged(grpcMeta.GetTags()).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
		resp, err := handler(ctx, req)
		return resp, err
	}
}

func metricsUnaryServerInterceptorCounters() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		grpcMeta := metrics.GetGrpcEndPointMetadataFromFullMethod(ctx, info.FullMethod, "unary")
		if err != nil {
			var terr *api.TigrisError
			var ferr fdb.Error
			if errors.As(err, &terr) {
				countSpecificErrorMessage(grpcMeta, "tigris_server", terr.Code.String())
			} else if errors.As(err, &ferr) {
				countSpecificErrorMessage(grpcMeta, "fdb_server", strconv.Itoa(ferr.Code))
			} else {
				countUnknownErrorMessage(grpcMeta)
			}
		} else {
			countOkMessage(grpcMeta)
		}
		return resp, err
	}
}

func metricsStreamServerInterceptorResponseTime() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		grpcMeta := metrics.GetGrpcEndPointMetadataFromFullMethod(stream.Context(), info.FullMethod, "stream")
		defer metrics.RequestsRespTime.Tagged(grpcMeta.GetTags()).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
		wrapper := &recvWrapper{stream}
		err := handler(srv, wrapper)
		return err
	}
}

func metricsStreamServerInterceptorCounter() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		grpcMeta := metrics.GetGrpcEndPointMetadataFromFullMethod(stream.Context(), info.FullMethod, "stream")
		wrapper := &recvWrapper{stream}
		err := handler(srv, wrapper)
		if err != nil {
			var terr *api.TigrisError
			var ferr fdb.Error
			if errors.As(err, &terr) {
				countSpecificErrorMessage(grpcMeta, "tigris_server", terr.Code.String())
			} else if errors.As(err, &ferr) {
				countSpecificErrorMessage(grpcMeta, "fdb_server", strconv.Itoa(ferr.Code))
			} else {
				countUnknownErrorMessage(grpcMeta)
			}
		} else {
			countOkMessage(grpcMeta)
		}
		return err
	}
}
