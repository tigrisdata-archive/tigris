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
	"fmt"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	pkgErrors "github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	tigrisconfig "github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util"
	"google.golang.org/grpc"
)

func Get(config *config.Config) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	authFunc := getAuthFunction(config)

	// adding all the middlewares for the server stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.
	sampler := zerolog.BasicSampler{N: uint32(1 / tigrisconfig.DefaultConfig.Log.SampleRate)}
	sampledTaggedLogger := log.Logger.Sample(&sampler).With().
		Str("env", tigrisconfig.GetEnvironment()).
		Str("service", util.Service).
		Str("version", util.Version).
		Logger()

	// The order of the interceptors matter with optional elements in them
	streamInterceptors := []grpc.StreamServerInterceptor{
		metadataExtractorStream(),
	}

	if config.Metrics.Enabled || config.Tracing.Enabled {
		streamInterceptors = append(streamInterceptors, measureStream())
	}

	streamInterceptors = append(streamInterceptors, forwarderStreamServerInterceptor())

	if authFunc != nil {
		streamInterceptors = append(streamInterceptors, grpc_auth.StreamServerInterceptor(authFunc))
	}

	streamInterceptors = append(streamInterceptors, []grpc.StreamServerInterceptor{
		namespaceSetterStreamServerInterceptor(config.Auth.EnableNamespaceIsolation),
		quotaStreamServerInterceptor(),
		grpc_logging.StreamServerInterceptor(grpc_zerolog.InterceptorLogger(sampledTaggedLogger), []grpc_logging.Option{}...),
		validatorStreamServerInterceptor(),
		grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(recoveryHandler)),
		headersStreamServerInterceptor(),
	}...)
	stream := middleware.ChainStreamServer(streamInterceptors...)

	// adding all the middlewares for the unary stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.

	// The order of the interceptors matter with optional elements in them
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		metadataExtractorUnary(),
	}

	if config.Metrics.Enabled || config.Tracing.Enabled {
		unaryInterceptors = append(unaryInterceptors, measureUnary())
	}

	unaryInterceptors = append(unaryInterceptors, forwarderUnaryServerInterceptor())

	if authFunc != nil {
		unaryInterceptors = append(unaryInterceptors, grpc_auth.UnaryServerInterceptor(authFunc))
	}

	unaryInterceptors = append(unaryInterceptors, []grpc.UnaryServerInterceptor{
		namespaceSetterUnaryServerInterceptor(config.Auth.EnableNamespaceIsolation),
		pprofUnaryServerInterceptor(),
		quotaUnaryServerInterceptor(),
		grpc_logging.UnaryServerInterceptor(grpc_zerolog.InterceptorLogger(sampledTaggedLogger)),
		validatorUnaryServerInterceptor(),
		timeoutUnaryServerInterceptor(DefaultTimeout),
		grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(recoveryHandler)),
		headersUnaryServerInterceptor(),
	}...)
	unary := middleware.ChainUnaryServer(unaryInterceptors...)

	return unary, stream
}

func recoveryHandler(p interface{}) error {
	err, ok := p.(error)
	if !ok {
		err = fmt.Errorf("internal server error")
	}

	err = pkgErrors.WithStack(err)
	log.Error().Stack().Err(err).Msg("panic")

	return errors.Internal("Internal server error")
}
