// Copyright 2022-2023 Tigris Data, Inc.
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

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
)

func metadataExtractorUnary() func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		reqMetadata := request.GetGrpcEndPointMetadataFromFullMethod(ctx, info.FullMethod, "unary", req)
		ctx = reqMetadata.SaveToContext(ctx)
		resp, err := handler(ctx, req)
		return resp, err
	}
}

func metadataExtractorStream() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := &wrappedStream{WrappedServerStream: middleware.WrapServerStream(stream)}
		wrapped.WrappedContext = stream.Context()
		reqMetadata := request.GetGrpcEndPointMetadataFromFullMethod(wrapped.WrappedContext, info.FullMethod, "stream", nil)
		wrapped.WrappedContext = reqMetadata.SaveToContext(wrapped.WrappedContext)
		err := handler(srv, wrapped)
		return err
	}
}
