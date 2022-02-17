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
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

func Get() (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	stream := middleware.ChainStreamServer(
		grpc_ratelimit.StreamServerInterceptor(&RateLimiter{}),
		grpc_auth.StreamServerInterceptor(AuthFunc),
		grpc_logging.StreamServerInterceptor(grpc_zerolog.InterceptorLogger(log.Logger)),
		grpc_validator.StreamServerInterceptor(false),
		grpc_opentracing.StreamServerInterceptor(),
		grpc_recovery.StreamServerInterceptor(),
	)

	unary := middleware.ChainUnaryServer(
		PprofUnaryServerInterceptor(),
		grpc_ratelimit.UnaryServerInterceptor(&RateLimiter{}),
		grpc_auth.UnaryServerInterceptor(AuthFunc),
		grpc_logging.UnaryServerInterceptor(grpc_zerolog.InterceptorLogger(log.Logger)),
		grpc_validator.UnaryServerInterceptor(false),
		TimeoutUnaryServerInterceptor(DefaultTimeout),
		grpc_opentracing.UnaryServerInterceptor(),
		grpc_recovery.UnaryServerInterceptor(),
	)

	return unary, stream
}
