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

	HeaderPrefix = "Tigris-"

	HeaderTxID     = "Tigris-Tx-Id"
	HeaderTxOrigin = "Tigris-Tx-Origin"

	grpcGatewayPrefix = "Grpc-Gateway-"
)

func CustomMatcher(key string) (string, bool) {
	key = textproto.CanonicalMIMEHeaderKey(key)
	switch key {
	case HeaderRequestTimeout, HeaderAccessControlAllowOrigin:
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
