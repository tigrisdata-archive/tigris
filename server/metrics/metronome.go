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
	"strconv"

	"github.com/uber-go/tally"
)

var (
	MetronomeCreateAccount tally.Scope
	MetronomeAddPlan       tally.Scope
	MetronomeIngest        tally.Scope
	MetronomeListInvoices  tally.Scope
	MetronomeGetInvoice    tally.Scope
	MetronomeGetUsage      tally.Scope
	MetronomeGetCustomer   tally.Scope
)

func initializeMetronomeScopes() {
	MetronomeCreateAccount = MetronomeMetrics.SubScope("create_account")
	MetronomeAddPlan = MetronomeMetrics.SubScope("add_plan")
	MetronomeIngest = MetronomeMetrics.SubScope("ingest")
	MetronomeListInvoices = MetronomeMetrics.SubScope("list_invoices")
	MetronomeGetInvoice = MetronomeMetrics.SubScope("get_invoice")
	MetronomeGetUsage = MetronomeMetrics.SubScope("get_usage")
	MetronomeGetCustomer = MetronomeMetrics.SubScope("get_customer")
}

func GetResponseCodeTags(code int) map[string]string {
	return map[string]string{
		"response_code": strconv.Itoa(code),
	}
}

func GetErrorCodeTags(err error) map[string]string {
	return map[string]string{
		"error_value": err.Error(),
	}
}

func GetIngestEventTags(eventType string) map[string]string {
	return map[string]string{
		"event_type": eventType,
	}
}
