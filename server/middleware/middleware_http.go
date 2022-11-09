package middleware

import (
	"context"
	"net/http"

	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
)

func HTTPAuthMiddleware(config *config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			if authFunc := getAuthFunction(config); authFunc != nil {
				authFunc(context.TODO())
			}
			next.ServeHTTP(w, r)
		}

		return http.HandlerFunc(fn)
	}
}

func HTTPMetadataExtractorMiddleware(_ *config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			reqMetadata := request.NewRequestMetadata(r.Context())
			r = r.WithContext(reqMetadata.SaveToContext(r.Context()))
			next.ServeHTTP(w, r)
		}

		return http.HandlerFunc(fn)
	}
}