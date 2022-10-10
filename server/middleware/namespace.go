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

	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
)

var (
	excludedMethods = container.NewHashSet(
		api.HealthMethodName,
		"/tigrisdata.admin.v1.Admin/createNamespace",
		"/tigrisdata.admin.v1.Admin/listNamespaces",
	)

	namespaceExtractor = &request.AccessTokenNamespaceExtractor{}
)

func namespaceSetterUnaryServerInterceptor(enabled bool) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		namespace := request.DefaultNamespaceName
		if enabled && !excludedMethods.Contains(info.FullMethod) {
			var err error
			if namespace, err = namespaceExtractor.Extract(ctx); err != nil {
				// We know that getAccessToken with app_id/app_secret credentials doesn't have namespace set.
				// Mark it as default_namespace instead of unknown.
				if info.FullMethod != "/tigrisdata.auth.v1.Auth/getAccessToken" {
					// We return and error when the token is set, but namespace is empty
					return nil, err
				}
				namespace = request.DefaultNamespaceName
			}
		}

		return handler(request.SetNamespace(ctx, namespace), req)
	}
}

func namespaceSetterStreamServerInterceptor(enabled bool) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		namespace := request.DefaultNamespaceName
		if enabled {
			var err error
			if namespace, err = namespaceExtractor.Extract(stream.Context()); err != nil {
				return err
			}
		}

		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = request.SetNamespace(stream.Context(), namespace)

		return handler(srv, wrapped)
	}
}
