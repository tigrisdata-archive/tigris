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
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	headerAuthorize   = "authorization"
	grpcGatewayPrefix = "grpc-gateway-"
)

func getHeader(ctx context.Context, header string) string {
	if val := metautils.ExtractIncoming(ctx).Get(header); val != "" {
		return val
	}

	return metautils.ExtractIncoming(ctx).Get(grpcGatewayPrefix + header)
}

func AuthFromMD(ctx context.Context, expectedScheme string) (string, error) {
	val := getHeader(ctx, headerAuthorize)
	if val == "" {
		return "", status.Errorf(codes.Unauthenticated, "Request unauthenticated with "+expectedScheme)
	}
	splits := strings.SplitN(val, " ", 2)
	if len(splits) < 2 {
		return "", status.Errorf(codes.Unauthenticated, "Bad authorization string")
	}
	if !strings.EqualFold(splits[0], expectedScheme) {
		return "", status.Errorf(codes.Unauthenticated, "Request unauthenticated with "+expectedScheme)
	}
	return splits[1], nil
}

func AuthFunc(ctx context.Context) (context.Context, error) {
	_, err := AuthFromMD(ctx, "bearer")
	log.Debug().Str("error", err.Error()).Msg("auth interceptor")
	return context.WithValue(ctx, key("role"), "admin"), nil
}
