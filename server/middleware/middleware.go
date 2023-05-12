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
	"fmt"
	"runtime/debug"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpcLogging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcRecovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util"
	"google.golang.org/grpc"
)

func Get(cfg *config.Config) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	authFunc := getAuthFunction(cfg)

	// adding all the middlewares for the server stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.
	sampler := zerolog.BasicSampler{N: uint32(1 / config.DefaultConfig.Log.SampleRate)}
	sampledTaggedLogger := log.Logger.Sample(&sampler).With().
		Str("env", config.GetEnvironment()).
		Str("service", util.Service).
		Str("version", util.Version).
		Logger()

	// The order of the interceptors matter with optional elements in them
	streamInterceptors := []grpc.StreamServerInterceptor{
		headersStreamServerInterceptor(),
		metadataExtractorStream(),
	}

	if cfg.Metrics.Enabled || cfg.Tracing.Enabled {
		streamInterceptors = append(streamInterceptors, measureStream())
	}

	streamInterceptors = append(streamInterceptors, forwarderStreamServerInterceptor())

	if authFunc != nil {
		streamInterceptors = append(streamInterceptors, grpcAuth.StreamServerInterceptor(authFunc))
	}

	if cfg.Auth.Authz.Enabled {
		streamInterceptors = append(streamInterceptors, authzStreamServerInterceptor())
	}

	streamInterceptors = append(streamInterceptors, []grpc.StreamServerInterceptor{
		namespaceSetterStreamServerInterceptor(cfg.Auth.EnableNamespaceIsolation),
		quotaStreamServerInterceptor(),
		grpcLogging.StreamServerInterceptor(grpcZerolog.InterceptorLogger(sampledTaggedLogger), []grpcLogging.Option{}...),
		validatorStreamServerInterceptor(),
		grpcRecovery.StreamServerInterceptor(grpcRecovery.WithRecoveryHandler(recoveryHandler)),
	}...)
	stream := middleware.ChainStreamServer(streamInterceptors...)

	// adding all the middlewares for the unary stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.

	// The order of the interceptors matter with optional elements in them
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		headersUnaryServerInterceptor(),
		metadataExtractorUnary(),
	}

	if cfg.Metrics.Enabled || cfg.Tracing.Enabled {
		unaryInterceptors = append(unaryInterceptors, measureUnary())
	}

	unaryInterceptors = append(unaryInterceptors, forwarderUnaryServerInterceptor())

	if authFunc != nil {
		unaryInterceptors = append(unaryInterceptors, grpcAuth.UnaryServerInterceptor(authFunc))
	}

	if cfg.Auth.Authz.Enabled {
		unaryInterceptors = append(unaryInterceptors, authzUnaryServerInterceptor())
	}

	unaryInterceptors = append(unaryInterceptors, []grpc.UnaryServerInterceptor{
		namespaceSetterUnaryServerInterceptor(cfg.Auth.EnableNamespaceIsolation),
		pprofUnaryServerInterceptor(),
		quotaUnaryServerInterceptor(),
		grpcLogging.UnaryServerInterceptor(grpcZerolog.InterceptorLogger(sampledTaggedLogger)),
		validatorUnaryServerInterceptor(),
		timeoutUnaryServerInterceptor(DefaultTimeout),
		grpcRecovery.UnaryServerInterceptor(grpcRecovery.WithRecoveryHandler(recoveryHandler)),
	}...)
	unary := middleware.ChainUnaryServer(unaryInterceptors...)

	return unary, stream
}

func recoveryHandler(p any) error {
	err, ok := p.(error)
	if !ok {
		err = fmt.Errorf("internal server error")
	}

	log.Error().Str("stack", string(debug.Stack())).Err(err).Msg("panic")

	return errors.Internal("Internal server error")
}
