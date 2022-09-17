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

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"google.golang.org/grpc"
)

type reqValidator interface {
	Validate() error
}

func validate(req interface{}) error {
	if v, ok := req.(reqValidator); ok {
		if err := v.Validate(); err != nil {
			return errors.InvalidArgument(err.Error())
		}
	}

	return nil
}

// validatorUnaryServerInterceptor returns a new unary server interceptor that validates incoming messages.
func validatorUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if tx := api.GetTransaction(ctx); !api.IsTxSupported(ctx) && tx != nil {
			return nil, errors.InvalidArgument("interactive tx not supported but transaction token found")
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
			return errors.InvalidArgument("interactive tx not supported but transaction token found")
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
