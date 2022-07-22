package middleware

import (
	"context"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"google.golang.org/grpc"
)

type reqValidator interface {
	Validate() error
}

func validate(req interface{}) error {
	if v, ok := req.(reqValidator); ok {
		if err := v.Validate(); err != nil {
			return api.Errorf(api.Code_INVALID_ARGUMENT, err.Error())
		}
	}

	return nil
}

// validatorUnaryServerInterceptor returns a new unary server interceptor that validates incoming messages.
func validatorUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if tx := api.GetTransaction(ctx); !api.IsTxSupported(ctx) && tx != nil {
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "interactive tx not supported but transaction token found")
		}

		if err := validate(req); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// validatorStreamServerInterceptor returns a new streaming server interceptor that validates incoming messages.
func validatorStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if tx := api.GetTransaction(stream.Context()); !api.IsTxSupported(stream.Context()) && tx != nil {
			return api.Errorf(api.Code_INVALID_ARGUMENT, "interactive tx not supported but transaction token found")
		}
		wrapper := &recvWrapper{stream}
		return handler(srv, wrapper)
	}
}

type recvWrapper struct {
	grpc.ServerStream
}

// RecvMsg wrapper to validate individual stream messages
func (s *recvWrapper) RecvMsg(m interface{}) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	if err := validate(m); err != nil {
		return err
	}

	return nil
}
