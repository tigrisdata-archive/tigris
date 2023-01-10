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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/quota"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type quotaStream struct {
	namespace string
	*middleware.WrappedServerStream
}

func quotaUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ns, _ := request.GetNamespace(ctx)

		if m := info.FullMethod; m != api.HealthMethodName && !request.IsAdminApi(m) {
			if err := quota.Allow(ctx, ns, proto.Size(req.(proto.Message)), request.IsWrite(ctx)); err != nil {
				return nil, err
			}
		}

		return handler(ctx, req)
	}
}

func quotaStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if m := info.FullMethod; m != api.HealthMethodName && !request.IsAdminApi(m) {
			ns, _ := request.GetNamespace(stream.Context())
			wrapped := &quotaStream{
				WrappedServerStream: middleware.WrapServerStream(stream),
				namespace:           ns,
			}
			return handler(srv, wrapped)
		}

		return handler(srv, stream)
	}
}

func (w *quotaStream) RecvMsg(req interface{}) error {
	if err := quota.Allow(w.Context(), w.namespace, proto.Size(req.(proto.Message)), request.IsWrite(w.Context())); err != nil {
		return err
	}

	return w.ServerStream.RecvMsg(req)
}

func (w *quotaStream) SendMsg(req interface{}) error {
	if err := quota.Wait(w.Context(), w.namespace, proto.Size(req.(proto.Message)), request.IsWrite(w.Context())); err != nil {
		return err
	}

	return w.ServerStream.SendMsg(req)
}
