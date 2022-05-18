package middleware

import "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

const (
	// RequestTimeoutHeader is an end-to-end request header that indicates the maximum time that a client is
	// prepared to await a response. The value of the header is in seconds. A client is allowed to send fractional
	// values. For ex, 0.1 means 100milliseconds.
	RequestTimeoutHeader = "Request-Timeout"
)

func CustomMatcher(key string) (string, bool) {
	switch key {
	case RequestTimeoutHeader:
		return key, true
	default:
		return runtime.DefaultHeaderMatcher(key)
	}
}
