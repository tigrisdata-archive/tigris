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
	FdbOkCount       tally.Scope
	FdbErrorCount    tally.Scope
	FdbRespTime      tally.Scope
	FdbErrorRespTime tally.Scope
)

func getFdbOkTagKeys() []string {
	return []string{
		"grpc_method",
		"tigris_tenant",
		"env",
		"db",
		"collection",
		"fdb_method",
	}
}

func getFdbErrorTagKeys() []string {
	return []string{
		"grpc_method",
		"tigris_tenant",
		"env",
		"db",
		"collection",
		"fdb_method",
		"error_source",
		"error_value",
	}
}

func GetFdbOkTags(reqMethodName string) map[string]string {
	return map[string]string{
		"fdb_method": reqMethodName,
	}
}

func GetFdbErrorTags(reqMethodName string, code string) map[string]string {
	return map[string]string{
		"fdb_method":   reqMethodName,
		"error_source": "fdb",
		"error_value":  code,
	}
}

func GetFdbBaseTags(reqMethodName string) map[string]string {
	return GetFdbOkTags(reqMethodName)
}

func initializeFdbScopes() {
	FdbOkCount = FdbMetrics.SubScope("count")
	FdbErrorCount = FdbMetrics.SubScope("count")
	FdbRespTime = FdbMetrics.SubScope("response")
	FdbErrorRespTime = FdbMetrics.SubScope("error_response")
}
