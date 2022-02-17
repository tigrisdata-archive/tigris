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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	DefaultTimeout = 5 * time.Second
	MaximumTimeout = 300 * time.Second
)

// TimeoutUnaryServerInterceptor returns a new unary server interceptor
// that sets request timeout if it's not set in the context
func TimeoutUnaryServerInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (iface interface{}, err error) {
		var cancel context.CancelFunc

		//FIXME: pass and use HTTP timeout

		d, ok := ctx.Deadline()

		if ok && time.Until(d) > MaximumTimeout {
			timeout = MaximumTimeout
			ok = false
		}

		if !ok {
			ctx, cancel = context.WithDeadline(ctx, time.Now().Add(timeout))
		}

		defer func() {
			if cancel != nil {
				cancel()
			}
			if ctx.Err() == context.DeadlineExceeded {
				err = status.Errorf(codes.DeadlineExceeded, "Timeout")
			}
		}()

		return handler(ctx, req)
	}
}
