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
	SecondaryIndexOkCount       tally.Scope
	SecondaryIndexErrorCount    tally.Scope
	SecondaryIndexRespTime      tally.Scope
	SecondaryIndexErrorRespTime tally.Scope
)

func getSecondaryIndexOkTagKeys() []string {
	return []string{
		"grpc_method",
		"tigris_tenant",
		"tigris_tenant_name",
		"env",
		"project",
		"db",
		"branch",
		"collection",
		"secondary_index_method",
	}
}

func getSecondaryIndexErrorTagKeys() []string {
	return []string{
		"grpc_method",
		"tigris_tenant",
		"tigris_tenant_name",
		"env",
		"project",
		"db",
		"collection",
		"error_source",
		"error_value",
		"secondary_index_method",
	}
}

func initializeSecondaryIndexScopes() {
	SecondaryIndexOkCount = SecondaryIndexMetrics.SubScope("count_ok")
	SecondaryIndexErrorCount = SecondaryIndexMetrics.SubScope("count_error")
	SecondaryIndexRespTime = SecondaryIndexMetrics.SubScope("response")
	SecondaryIndexErrorRespTime = SecondaryIndexMetrics.SubScope("error_response")
}

func GetSecondaryIndexTags(reqMethodName string) map[string]string {
	return map[string]string{
		"secondary_index_method": reqMethodName,
	}
}
