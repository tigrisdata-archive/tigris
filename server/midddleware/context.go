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
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"google.golang.org/grpc"
)

type key string

const (
	token key = "token"
)

func TxCtxUnaryServerInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (iface interface{}, err error) {
		ctx = context.WithValue(ctx, token, api.GetHeader(ctx, string(token)))
		return handler(ctx, req)
	}
}
