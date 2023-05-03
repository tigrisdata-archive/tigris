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

package billing

import (
	"context"
	"time"

	"github.com/google/uuid"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type AccountId = uuid.UUID

type Provider interface {
	CreateAccount(ctx context.Context, namespaceId string, name string) (AccountId, error)
	AddDefaultPlan(ctx context.Context, accountId AccountId) (bool, error)
	AddPlan(ctx context.Context, accountId AccountId, planId uuid.UUID) (bool, error)
	PushUsageEvents(ctx context.Context, events []*UsageEvent) error
	PushStorageEvents(ctx context.Context, events []*StorageEvent) error
	GetInvoices(ctx context.Context, accountId AccountId, r *api.ListInvoicesRequest) (*api.ListInvoicesResponse, error)
	GetInvoiceById(ctx context.Context, accountId AccountId, invoiceId string) (*api.ListInvoicesResponse, error)
	GetUsage(ctx context.Context, id AccountId, r *UsageRequest) (*UsageAggregate, error)
}

type UsageRequest struct {
	BillableMetric *[]string
	StartTime      *time.Time
	EndTime        *time.Time
	NextPage       *string
}

type Usage struct {
	StartTime time.Time
	EndTime   time.Time
	Value     *float32
}

type UsageAggregate struct {
	Data     map[string][]*Usage
	NextPage *string
}

func NewProvider() Provider {
	if config.DefaultConfig.Billing.Metronome.Enabled {
		svc, err := NewMetronomeProvider(config.DefaultConfig.Billing.Metronome)
		if !ulog.E(err) {
			return svc
		}
	}
	return &noop{}
}
