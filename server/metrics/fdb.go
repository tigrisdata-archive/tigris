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
	"strconv"
)

var (
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
		"GetInternalDatabase",
	}
)

func GetFdbReqTags(reqMethodName string, tx bool) map[string]string {
	return map[string]string{
		"fdb_request_method": reqMethodName,
		"fdb_tx":             strconv.FormatBool(tx),
	}
}

func GetFdbReqSpecificErrorTags(reqMethodName string, code string, tx bool) map[string]string {
	return map[string]string{
		"fdb_request_method": reqMethodName,
		"fdb_error_code":     code,
		"fdb_tx":             strconv.FormatBool(tx),
	}
}

func InitializeFdbMetrics() {
	FdbErrorRequests = FdbRequests.SubScope("error")
	for _, reqMethodName := range MeasuredFdbRequests {
		non_tx_tags := GetFdbReqTags(reqMethodName, false)
		tx_tags := GetFdbReqTags(reqMethodName, true)
		// Counter for ok requests
		FdbRequests.Tagged(non_tx_tags).Counter("ok")
		FdbRequests.Tagged(tx_tags).Counter("ok")

		// Counter for unknown errors
		FdbErrorRequests.Tagged(non_tx_tags).Counter("unknown")
		FdbErrorRequests.Tagged(tx_tags).Counter("unknown")

		// Counters for known errors are only initialized for the first occurrence.
		// The error code is part of the tags.

		// Response time histograms
		FdbRequests.Tagged(non_tx_tags).Histogram("histogram", tally.DefaultBuckets)
		FdbRequests.Tagged(tx_tags).Histogram("histogram", tally.DefaultBuckets)
	}
}
