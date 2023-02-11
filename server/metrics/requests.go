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

package metrics

import (
	"github.com/uber-go/tally"
)

var (
	RequestsOkCount       tally.Scope
	RequestsErrorCount    tally.Scope
	RequestsRespTime      tally.Scope
	RequestsErrorRespTime tally.Scope
)

func getRequestOkTagKeys() []string {
	return []string{
		"grpc_method",
		"tigris_tenant",
		"tigris_tenant_name",
		"env",
		"project",
		"db",
		"branch",
		"collection",
		"read_type",
		"search_type",
		"write_type",
		"sort",
		"sub",
	}
}

func getRequestErrorTagKeys() []string {
	return []string{
		"grpc_method",
		"tigris_tenant",
		"tigris_tenant_name",
		"env",
		"db",
		"project",
		"branch",
		"collection",
		"error_source",
		"error_value",
		"read_type",
		"search_type",
		"write_type",
		"sort",
		"sub",
	}
}

func initializeRequestScopes() {
	RequestsOkCount = Requests.SubScope("count")
	RequestsErrorCount = Requests.SubScope("count")
	RequestsRespTime = Requests.SubScope("response")
	RequestsErrorRespTime = Requests.SubScope("error_response")
}
