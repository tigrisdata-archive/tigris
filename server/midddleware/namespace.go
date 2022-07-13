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

type NamespaceSetter struct {
	tenantManager      *metadata.TenantManager
	namespaceExtractor NamespaceExtractor
	excludedMethods    set.HashSet
	config             *config.Config
}

func (r *NamespaceSetter) NamespaceSetterUnaryServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !r.config.Auth.EnableNamespaceIsolation {
			return handler(setNamespace(ctx, metadata.DefaultNamespaceName), req)
		} else {
			namespace, err := r.namespaceExtractor.Extract(ctx)
			if err != nil {
				return nil, err
			}
			if namespace == "" {
				return handler(setNamespace(ctx, "unknown"), req)
			}
			return handler(setNamespace(ctx, namespace), req)
		}
	}
}

func (r *NamespaceSetter) NamespaceSetterStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !r.config.Auth.EnableNamespaceIsolation {
			wrapped := middleware.WrapServerStream(stream)
			wrapped.WrappedContext = setNamespace(stream.Context(), metadata.DefaultNamespaceName)
			return handler(srv, wrapped)
		} else {
			namespace, err := r.namespaceExtractor.Extract(stream.Context())
			if err != nil {
				return err
			}
			if namespace == "" {
				return api.Errorf(api.Code_INVALID_ARGUMENT, "Could not find namespace")
			}
			wrapped := middleware.WrapServerStream(stream)
			wrapped.WrappedContext = setNamespace(stream.Context(), namespace)
			return handler(srv, wrapped)
		}
	}
}
