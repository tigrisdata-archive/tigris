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
	"github.com/tigrisdata/tigris/util"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/tigrisdata/tigris/server/metrics"
	"google.golang.org/grpc"
)

const (
	TraceSpanType string = "rpc"
)

func traceUnary() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var finisher func()
		grpcMeta := metrics.GetGrpcEndPointMetadataFromFullMethod(ctx, info.FullMethod, "unary")
		spanMeta := metrics.NewSpanMeta(util.Service, info.FullMethod, TraceSpanType, grpcMeta.GetTags())
		ctx, finisher = spanMeta.StartTracing(ctx, false)
		defer finisher()
		resp, err := handler(ctx, req)
		if err != nil {
			spanMeta.FinishWithError(err)
		}
		return resp, err
	}
}

func traceStream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		var finisher func()
		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = stream.Context()
		grpcMeta := metrics.GetGrpcEndPointMetadataFromFullMethod(wrapped.WrappedContext, info.FullMethod, "stream")
		spanMeta := metrics.NewSpanMeta(util.Service, info.FullMethod, TraceSpanType, grpcMeta.GetTags())
		wrapped.WrappedContext, finisher = spanMeta.StartTracing(wrapped.WrappedContext, false)
		defer finisher()
		wrapper := &recvWrapper{wrapped}
		err := handler(srv, wrapper)
		if err != nil {
			spanMeta.FinishWithError(err)
		}
		return err
	}
}
