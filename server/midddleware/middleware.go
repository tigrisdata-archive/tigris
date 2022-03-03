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
	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_ratelimit "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/ratelimit"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

func Get() (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	// adding all the middlewares for the server stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.
	stream := middleware.ChainStreamServer(
		grpc_ratelimit.StreamServerInterceptor(&RateLimiter{}),
		grpc_auth.StreamServerInterceptor(AuthFunc),
		grpc_logging.StreamServerInterceptor(grpc_zerolog.InterceptorLogger(log.Logger), []grpc_logging.Option{}...),
		// grpc_logging.PayloadStreamServerInterceptor(grpc_zerolog.InterceptorLogger(log.Logger), alwaysLoggingDeciderServer, time.RFC3339), // To log payload
		grpc_opentracing.StreamServerInterceptor(),
		grpc_recovery.StreamServerInterceptor(),
	)

	// adding all the middlewares for the unary stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.
	unary := middleware.ChainUnaryServer(
		PprofUnaryServerInterceptor(),
		grpc_ratelimit.UnaryServerInterceptor(&RateLimiter{}),
		grpc_auth.UnaryServerInterceptor(AuthFunc),
		grpc_logging.UnaryServerInterceptor(grpc_zerolog.InterceptorLogger(log.Logger)),
		// grpc_logging.PayloadUnaryServerInterceptor(grpc_zerolog.InterceptorLogger(log.Logger), alwaysLoggingDeciderServer, time.RFC3339), //To log payload
		TimeoutUnaryServerInterceptor(DefaultTimeout),
		grpc_opentracing.UnaryServerInterceptor(),
		grpc_recovery.UnaryServerInterceptor(),
	)

	return unary, stream
}
