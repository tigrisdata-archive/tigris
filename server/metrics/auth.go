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

package metrics

import (
	"context"

	"github.com/uber-go/tally"
)

var (
	AuthOkRequests    tally.Scope
	AuthErrorRequests tally.Scope
	AuthRespTime      tally.Scope
)

func getAuthOkTagKeys() []string {
	return []string{
		"grpc_method",
		"env",
		"service",
		"version",
	}
}

func getAuthTimerTagKeys() []string {
	return []string{
		"grpc_method",
		"env",
		"service",
		"version",
	}
}

func getAuthErrorTagKeys() []string {
	return []string{
		"grpc_method",
		"env",
		"service",
		"version",
		"error_source",
		"error_code",
	}
}

func GetAuthBaseTags(ctx context.Context) map[string]string {
	return getGrpcTagsFromContext(ctx)
}

func initializeAuthScopes() {
	AuthOkRequests = AuthMetrics.SubScope("count")
	AuthErrorRequests = AuthMetrics.SubScope("count")
	AuthRespTime = AuthMetrics.SubScope("response")
}
