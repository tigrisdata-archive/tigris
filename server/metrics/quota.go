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
	QuotaUsage     tally.Scope
	QuotaThrottled tally.Scope
	QuotaSet       tally.Scope
)

func initializeQuotaScopes() {
	QuotaMetrics = root.SubScope("quota")
	QuotaUsage = QuotaMetrics.SubScope("usage")
	QuotaThrottled = QuotaMetrics.SubScope("throttled")
	QuotaSet = QuotaMetrics.SubScope("set_node")
}

func getQuotaUsageTags(namespaceName string) map[string]string {
	return map[string]string{
		"tigris_tenant": namespaceName,
	}
}

func UpdateQuotaUsage(namespaceName string, value int, isWrite bool) {
	if QuotaUsage == nil {
		return
	}

	counter := "read_units"
	if isWrite {
		counter = "write_units"
	}

	QuotaUsage.Tagged(getQuotaUsageTags(namespaceName)).Counter(counter).Inc(int64(value))
}

func UpdateQuotaRateThrottled(namespaceName string, value int, isWrite bool) {
	if QuotaThrottled == nil {
		return
	}

	counter := "read_units"
	if isWrite {
		counter = "write_units"
	}

	QuotaThrottled.Tagged(getQuotaUsageTags(namespaceName)).Counter(counter).Inc(int64(value))
}

func UpdateQuotaStorageThrottled(namespaceName string, value int) {
	if QuotaThrottled == nil {
		return
	}

	QuotaThrottled.Tagged(getQuotaUsageTags(namespaceName)).Counter("storage").Inc(int64(value))
}

func UpdateQuotaCurrentNodeLimit(namespaceName string, value int, isWrite bool) {
	if QuotaSet == nil {
		return
	}

	counter := "read_limit"
	if isWrite {
		counter = "write_limit"
	}

	QuotaSet.Tagged(getQuotaUsageTags(namespaceName)).Gauge(counter).Update(float64(value))
}
