package middleware

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/lib/set"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"google.golang.org/grpc"
)

type RequestMetadataSetter struct {
	tenantManager            *metadata.TenantManager
	namespaceExtractor       NamespaceExtractor
	namespaceExemptedMethods set.HashSet
	adminMethods             set.HashSet
	config                   *config.Config
}

func (r *RequestMetadataSetter) NamespaceSetterUnaryServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

		if r.adminMethods.Contains(info.FullMethod) {
			ctx = setIsAdminApi(ctx, true)
		} else {
			ctx = setIsAdminApi(ctx, false)
		}

		if !r.config.Auth.EnableNamespaceIsolation || r.namespaceExemptedMethods.Contains(info.FullMethod) {
			return handler(setNamespace(ctx, metadata.DefaultNamespaceName), req)
		} else {
			namespace, err := r.namespaceExtractor.Extract(ctx)
			if err != nil {
				return nil, err
			}
			if namespace == "" {
				return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "Could not find namespace")
			}
			return handler(setNamespace(ctx, namespace), req)
		}
	}
}

func (r *RequestMetadataSetter) NamespaceSetterStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		if r.adminMethods.Contains(info.FullMethod) {
			ctx = setIsAdminApi(ctx, true)
		} else {
			ctx = setIsAdminApi(ctx, false)
		}

		if !r.config.Auth.EnableNamespaceIsolation || r.namespaceExemptedMethods.Contains(info.FullMethod) {
			wrapped := middleware.WrapServerStream(stream)
			wrapped.WrappedContext = setNamespace(ctx, metadata.DefaultNamespaceName)
			return handler(srv, wrapped)
		} else {
			namespace, err := r.namespaceExtractor.Extract(ctx)
			if err != nil {
				return err
			}
			if namespace == "" {
				return api.Errorf(api.Code_INVALID_ARGUMENT, "Could not find namespace")
			}
			wrapped := middleware.WrapServerStream(stream)
			wrapped.WrappedContext = setNamespace(ctx, namespace)
			return handler(srv, wrapped)
		}
	}
}
