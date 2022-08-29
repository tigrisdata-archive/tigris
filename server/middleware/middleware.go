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
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	lru "github.com/hashicorp/golang-lru"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/lib/set"
	"github.com/tigrisdata/tigris/server/config"
	tigrisconfig "github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/util"
	"google.golang.org/grpc"
)

func Get(config *config.Config, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	var authFunction func(ctx context.Context) (context.Context, error)

	if config.Auth.Enabled {
		jwtValidator := GetJWTValidator(config)
		lruCache, err := lru.New(config.Auth.TokenCacheSize)
		if err != nil {
			panic("Failed to setup token cache")
		}
		// inline closure to access the state of jwtValidator
		if config.Tracing.Enabled {
			authFunction = func(ctx context.Context) (context.Context, error) {
				return MeasuredAuthFunction(ctx, jwtValidator, config, lruCache)
			}
		} else {
			authFunction = func(ctx context.Context) (context.Context, error) {
				return AuthFunction(ctx, jwtValidator, config, lruCache)
			}
		}
	}

	excludedMethods := set.New()
	excludedMethods.Insert("/HealthAPI/Health")
	excludedMethods.Insert("/tigrisdata.admin.v1.Admin/createNamespace")
	excludedMethods.Insert("/tigrisdata.admin.v1.Admin/listNamespaces")
	namespaceInitializer := NamespaceSetter{
		tenantManager:      tenantMgr,
		namespaceExtractor: &request.AccessTokenNamespaceExtractor{},
		excludedMethods:    excludedMethods,
		config:             config,
	}
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
		forwarderStreamServerInterceptor(),
	}

	if config.Auth.Enabled {
		streamInterceptors = append(streamInterceptors, grpc_auth.StreamServerInterceptor(authFunction))
	}

	streamInterceptors = append(streamInterceptors, namespaceInitializer.NamespaceSetterStreamServerInterceptor())

	if config.Tracing.Enabled {
		streamInterceptors = append(streamInterceptors, traceStream())
	}

	streamInterceptors = append(streamInterceptors, []grpc.StreamServerInterceptor{
		quotaStreamServerInterceptor(),
		grpc_logging.StreamServerInterceptor(grpc_zerolog.InterceptorLogger(sampledTaggedLogger), []grpc_logging.Option{}...),
		validatorStreamServerInterceptor(),
		grpc_recovery.StreamServerInterceptor(),
		headersStreamServerInterceptor(),
	}...)
	stream := middleware.ChainStreamServer(streamInterceptors...)

	// adding all the middlewares for the unary stream
	//
	// Note: we don't add validate here and rather call it in server code because the validator interceptor returns gRPC
	// error which is not convertible to the internal rest error code.

	// The order of the interceptors matter with optional elements in them
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		forwarderUnaryServerInterceptor(),
	}
	if config.Auth.Enabled {
		unaryInterceptors = append(unaryInterceptors, grpc_auth.UnaryServerInterceptor(authFunction))
	}

	unaryInterceptors = append(unaryInterceptors, namespaceInitializer.NamespaceSetterUnaryServerInterceptor())

	if config.Tracing.Enabled {
		unaryInterceptors = append(unaryInterceptors, traceUnary())
	}

	unaryInterceptors = append(unaryInterceptors, []grpc.UnaryServerInterceptor{
		pprofUnaryServerInterceptor(),
		quotaUnaryServerInterceptor(),
		grpc_logging.UnaryServerInterceptor(grpc_zerolog.InterceptorLogger(sampledTaggedLogger)),
		validatorUnaryServerInterceptor(),
		timeoutUnaryServerInterceptor(DefaultTimeout),
		grpc_recovery.UnaryServerInterceptor(),
		headersUnaryServerInterceptor(),
	}...)
	unary := middleware.ChainUnaryServer(unaryInterceptors...)

	return unary, stream
}
