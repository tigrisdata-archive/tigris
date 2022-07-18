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
	SearchRequests      tally.Scope
	SearchErrorRequests tally.Scope
)

func InitializeSearchScopes() {
	SearchRequests = SearchMetrics.SubScope("requests")
	SearchErrorRequests = SearchRequests.SubScope("error")
}

func GetSearchTags(ctx context.Context, reqMethodName string) map[string]string {
	return addTigrisTenantToTags(ctx, getSearchReqTags(reqMethodName))
}

func getSearchReqTags(reqMethodName string) map[string]string {
	return map[string]string{
		"method":        reqMethodName,
		"tigris_tenant": DefaultReportedTigrisTenant,
	}
}
