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
	"fmt"
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	OutgoingHeaders       = metadata.New(map[string]string{})
	OutgoingStreamHeaders = metadata.New(map[string]string{})
)

const (
	CookieMaxAgeKey = "Expires"
)

func headersUnaryServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		callHeaders := metadata.New(map[string]string{})

		// add cookie header for sticky routing for interactive transactional operations
		if ty, ok := resp.(*api.BeginTransactionResponse); ok {
			expirationTime := time.Now().Add(MaximumTimeout + 2*time.Second)
			callHeaders.Append(api.SetCookie, fmt.Sprintf("%s=%s;%s=%s", api.HeaderTxID, ty.GetTxCtx().GetId(), CookieMaxAgeKey, expirationTime.Format(time.RFC1123)))
		}
		if err := grpc.SendHeader(ctx, metadata.Join(OutgoingHeaders, callHeaders)); err != nil {
			return nil, err
		}

		return resp, err
	}
}

func headersStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := grpc.SendHeader(stream.Context(), OutgoingStreamHeaders); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}
