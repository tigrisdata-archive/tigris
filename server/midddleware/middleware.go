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

	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_ratelimit "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/ratelimit"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	"google.golang.org/grpc"
	grpctrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
)

func Get(config *config.Config) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	jwtValidator := GetJWTValidator(config)
	// inline closure to access the state of jwtValidator
	authFunction := func(ctx context.Context) (context.Context, error) {
		return AuthFunction(ctx, jwtValidator, config)
	}

	// adding all the middlewares for the server stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.
	stream := middleware.ChainStreamServer(
		forwarderStreamServerInterceptor(),
		grpc_ratelimit.StreamServerInterceptor(&RateLimiter{}),
		grpc_auth.StreamServerInterceptor(authFunction),
		grpctrace.StreamServerInterceptor(grpctrace.WithServiceName("tigris-server")),
		grpc_logging.StreamServerInterceptor(grpc_zerolog.InterceptorLogger(log.Logger), []grpc_logging.Option{}...),
		validatorStreamServerInterceptor(),
		metricsStreamServerInterceptor(),
		grpc_opentracing.StreamServerInterceptor(),
		grpc_recovery.StreamServerInterceptor(),
		headersStreamServerInterceptor(),
	)

	// adding all the middlewares for the unary stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.
	unary := middleware.ChainUnaryServer(
		forwarderUnaryServerInterceptor(),
		pprofUnaryServerInterceptor(),
		grpc_ratelimit.UnaryServerInterceptor(&RateLimiter{}),
		grpc_auth.UnaryServerInterceptor(authFunction),
		grpctrace.UnaryServerInterceptor(grpctrace.WithServiceName("tigris-server")),
		grpc_logging.UnaryServerInterceptor(grpc_zerolog.InterceptorLogger(log.Logger)),
		validatorUnaryServerInterceptor(),
		timeoutUnaryServerInterceptor(DefaultTimeout),
		metricsUnaryServerInterceptor(),
		grpc_opentracing.UnaryServerInterceptor(),
		grpc_recovery.UnaryServerInterceptor(),
		headersUnaryServerInterceptor(),
	)

	return unary, stream
}
