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

package api

import (
	"context"
	"net/textproto"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

const (
	// HeaderRequestTimeout is an end-to-end request header that indicates the maximum time that a client is
	// prepared to await a response. The value of the header is in seconds. A client is allowed to send fractional
	// values. For ex, 0.1 means 100milliseconds.
	HeaderRequestTimeout = "Request-Timeout"

	HeaderAccessControlAllowOrigin = "Access-Control-Allow-Origin"
	HeaderAuthorization            = "authorization"

	SetCookie = "Set-Cookie"
	Cookie    = "Cookie"

	HeaderPrefix = "Tigris-"

	HeaderTxID          = "Tigris-Tx-Id"
	HeaderTxOrigin      = "Tigris-Tx-Origin"
	grpcGatewayPrefix   = "Grpc-Gateway-"
	HeaderSchemaSignOff = "Tigris-Schema-Sign-Off"
)

func CustomMatcher(key string) (string, bool) {
	key = textproto.CanonicalMIMEHeaderKey(key)
	switch key {
	case HeaderRequestTimeout, HeaderAccessControlAllowOrigin, SetCookie, Cookie:
		return key, true
	default:
		if strings.HasPrefix(key, HeaderPrefix) {
			return key, true
		}
		return runtime.DefaultHeaderMatcher(key)
	}
}

func GetHeader(ctx context.Context, header string) string {
	if val := metautils.ExtractIncoming(ctx).Get(header); val != "" {
		return val
	}

	return metautils.ExtractIncoming(ctx).Get(grpcGatewayPrefix + header)
}
