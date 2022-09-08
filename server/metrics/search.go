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
	"github.com/uber-go/tally"
)

var (
	SearchOkRequests    tally.Scope
	SearchErrorRequests tally.Scope
	SearchRespTime      tally.Scope
)

func getSearchOkTagKeys() []string {
	return []string{
		"grpc_method",
		"grpc_service",
		"tigris_tenant",
		"grpc_service_type",
		"env",
		"db",
		"collection",
		"search_method",
	}
}

func getSearchTimerTagKeys() []string {
	return []string{
		"grpc_method",
		"grpc_service",
		"tigris_tenant",
		"grpc_service_type",
		"env",
		"db",
		"collection",
		"search_method",
	}
}

func getSearchErrorTagKeys() []string {
	return []string{
		"grpc_method",
		"grpc_service",
		"tigris_tenant",
		"grpc_service_type",
		"env",
		"db",
		"collection",
		"error_code",
		"error_value",
		"search_method",
	}
}

func initializeSearchScopes() {
	SearchOkRequests = SearchMetrics.SubScope("count")
	SearchErrorRequests = SearchMetrics.SubScope("count")
	SearchRespTime = SearchMetrics.SubScope("response")
}

func GetSearchTags(reqMethodName string) map[string]string {
	return map[string]string{
		"search_method": reqMethodName,
	}
}
