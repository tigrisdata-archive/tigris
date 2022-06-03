package middleware

import (
	"context"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

const (
	// requestTimeoutHeader is an end-to-end request header that indicates the maximum time that a client is
	// prepared to await a response. The value of the header is in seconds. A client is allowed to send fractional
	// values. For ex, 0.1 means 100milliseconds.
	requestTimeoutHeader = "Request-Timeout"

	grpcGatewayPrefix = "grpc-gateway-"
)

func CustomMatcher(key string) (string, bool) {
	switch key {
	case requestTimeoutHeader:
		return key, true
	default:
		if strings.HasPrefix(key, api.HeaderPrefix) {
			return key, true
		}
		return runtime.DefaultHeaderMatcher(key)
	}
}

func getHeader(ctx context.Context, header string) string {
	if val := metautils.ExtractIncoming(ctx).Get(header); val != "" {
		return val
	}

	return metautils.ExtractIncoming(ctx).Get(grpcGatewayPrefix + header)
}
