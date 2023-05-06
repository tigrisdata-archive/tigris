// Copyright 2022-2023 Tigris Data, Inc.
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
	"strconv"
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"google.golang.org/grpc"
)

var (
	DefaultTimeout     = 10 * time.Second
	MaximumTimeout     = 30 * time.Second
	LongRunningTimeout = 1 * time.Hour
)

// timeoutUnaryServerInterceptor returns a new unary server interceptor
// that sets request timeout if it's not set in the context.
func timeoutUnaryServerInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (iface any, err error) {
		var cancel context.CancelFunc

		ctx, cancel = setDeadlineUsingHeader(ctx)

		d, ok := ctx.Deadline()
		if ok && !isLongRunningAPI(info.FullMethod) && time.Until(d) > MaximumTimeout {
			timeout = MaximumTimeout
			ok = false
		}

		if !ok && isLongRunningAPI(info.FullMethod) {
			timeout = LongRunningTimeout
		}

		if !ok {
			ctx, cancel = context.WithDeadline(ctx, time.Now().Add(timeout))
		}

		defer func() {
			if cancel != nil {
				cancel()
			}
			if ctx.Err() == context.DeadlineExceeded {
				err = errors.DeadlineExceeded("the server is taking too long to process the request")
			}
		}()

		return handler(ctx, req)
	}
}

func isLongRunningAPI(method string) bool {
	return method == api.IndexCollection || method == api.SearchIndexCollectionMethodName
}

func setDeadlineUsingHeader(ctx context.Context) (context.Context, context.CancelFunc) {
	value := api.GetHeader(ctx, api.HeaderRequestTimeout)
	if len(value) == 0 {
		return ctx, nil
	}

	// header is set for timeout
	parsedV, err := strconv.ParseFloat(value, 64)
	if err != nil {
		// use the default timeout
		return ctx, nil
	}

	milliseconds := int64(parsedV * 1000)
	if _, ok := ctx.Deadline(); !ok {
		return context.WithDeadline(ctx, time.Now().Add(time.Duration(milliseconds)*time.Millisecond))
	}
	return ctx, nil
}
