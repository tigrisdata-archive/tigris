package middleware

import (
	"context"

	"github.com/tigrisdata/tigris/server/quota"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func quotaUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ns, _ := request.GetNamespace(ctx)
		if err := quota.Allow(ctx, ns, proto.Size(req.(proto.Message))); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func quotaStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ns, _ := request.GetNamespace(stream.Context())
		if err := quota.Allow(stream.Context(), ns, 0); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}
