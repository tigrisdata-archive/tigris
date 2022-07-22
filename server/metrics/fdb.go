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
	FdbRequests         tally.Scope
	FdbErrorRequests    tally.Scope
	MeasuredFdbRequests = []string{
		"Delete",
		"DeleteRange",
		"CreateTable",
		"DropTable",
		"SetVersionstampedValue",
		"SetVersionstampedKey",
		"Get",
		"Insert",
		"Replace",
		"Read",
		"ReadRange",
		"Update",
		"UpdateRange",
		"BeginTx",
	}
)

func getFdbReqTags(reqMethodName string) map[string]string {
	return map[string]string{
		"method":        reqMethodName,
		"tigris_tenant": DefaultReportedTigrisTenant,
	}
}

func getFdbReqSpecificErrorTags(reqMethodName string, code string) map[string]string {
	return map[string]string{
		"method":        reqMethodName,
		"error_code":    code,
		"tigris_tenant": DefaultReportedTigrisTenant,
	}
}

func GetFdbTags(ctx context.Context, reqMethodName string) map[string]string {
	return addTigrisTenantToTags(ctx, getFdbReqTags(reqMethodName))
}

func GetFdbSpecificErrorTags(ctx context.Context, reqMethodName string, code string) map[string]string {
	return addTigrisTenantToTags(ctx, getFdbReqSpecificErrorTags(reqMethodName, code))
}

func InitializeFdbScopes() {
	FdbRequests = FdbMetrics.SubScope("requests")
	FdbErrorRequests = FdbRequests.SubScope("error")
}
