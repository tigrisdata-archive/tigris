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
	"net/http"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
)

func HTTPAuthMiddleware(config *config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			if authFunc := getAuthFunction(config); authFunc != nil {
				ctx, err := authFunc(r.Context())
				if err != nil {
					switch e := err.(type) {
					case *api.TigrisError:
						http.Error(w, e.Message, api.ToHTTPCode(e.Code))
					default:
						http.Error(w, err.Error(), http.StatusInternalServerError)
					}
					return
				}

				r = r.WithContext(ctx)
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
