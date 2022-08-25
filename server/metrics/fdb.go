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
	FdbOkRequests    tally.Scope
	FdbErrorRequests tally.Scope
	FdbRespTime      tally.Scope
)

func getFdbOkTagKeys() []string {
	return []string{
		"grpc_method",
		"grpc_service",
		"tigris_tenant",
		"grpc_service_type",
		"env",
		"db",
		"collection",
		"fdb_method",
	}
}

func getFdbTimerTagKeys() []string {
	return []string{
		"grpc_method",
		"grpc_service",
		"tigris_tenant",
		"grpc_service_type",
		"env",
		"db",
		"collection",
		"fdb_method",
	}
}

func getFdbErrorTagKeys() []string {
	return []string{
		"grpc_method",
		"grpc_service",
		"tigris_tenant",
		"grpc_service_type",
		"env",
		"db",
		"collection",
		"fdb_method",
		"error_source",
		"error_value",
	}
}

func getFdbReqOkTags(reqMethodName string) map[string]string {
	return map[string]string{
		"fdb_method":    reqMethodName,
		"tigris_tenant": UnknownValue,
	}
}

func getFdbReqErrorTags(reqMethodName string, code string) map[string]string {
	return map[string]string{
		"fdb_method":    reqMethodName,
		"error_source":  "fdb",
		"error_value":   code,
		"tigris_tenant": UnknownValue,
	}
}

func GetFdbOkTags(ctx context.Context, reqMethodName string) map[string]string {
	return addTigrisTenantToTags(ctx, getFdbReqOkTags(reqMethodName))
}

func GetFdbErrorTags(ctx context.Context, reqMethodName string, code string) map[string]string {
	return addTigrisTenantToTags(ctx, getFdbReqErrorTags(reqMethodName, code))
}

func initializeFdbScopes() {
	FdbOkRequests = FdbMetrics.SubScope("count")
	FdbErrorRequests = FdbMetrics.SubScope("count")
	FdbRespTime = FdbMetrics.SubScope("response")
}