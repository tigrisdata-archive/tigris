package middleware

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/quota"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type quotaStream struct {
	namespace string
	quota     *quota.State
	*middleware.WrappedServerStream
}

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

		wrapped := &quotaStream{
			WrappedServerStream: middleware.WrapServerStream(stream),
			namespace:           ns,
			quota:               quota.GetState(ns),
		}

		return handler(srv, wrapped)
	}
}

func (w *quotaStream) RecvMsg(req interface{}) error {
	// Account incoming read request toward write throughput quota
	if err := quota.Allow(w.Context(), w.namespace, proto.Size(req.(proto.Message))); err != nil {
		return err
	}
	return w.ServerStream.RecvMsg(req)
}

func (w *quotaStream) SendMsg(req interface{}) error {
	if config.DefaultConfig.Quota.Enabled {
		// Limit read throughput by waiting when quota tokens are available
		if err := w.quota.ReadThroughput.WaitN(w.Context(), proto.Size(req.(proto.Message))); err != nil {
			return err
		}
	}
	return w.ServerStream.SendMsg(req)
}
