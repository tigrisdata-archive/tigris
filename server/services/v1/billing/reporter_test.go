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
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

const (
	createAccountFailure  = "createAccountFail"
	addDefaultPlanFailure = "95b84feb-e217-46a0-a1de-406dbd466a6a"
	pushUsageFailure      = "pushUsageEventsFail"
)

func TestUsageReporter_Push(t *testing.T) {
	t.Run("pushes all valid events", func(t *testing.T) {
		namespaces := map[string]metadata.NamespaceMetadata{
			"ns1": {
				Id:    1,
				StrId: "ns1",
				Name:  "no metronome integration",
			},
			"ns2": {
				Id:    2,
				StrId: "ns2",
				Name:  "with metronome integration",
				Accounts: metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: true,
						Id:      "m2",
					},
				},
			},
			"ns3": {
				Id:    3,
				StrId: "ns3",
				Name:  "with metronome disabled",
				Accounts: metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: false,
						Id:      "m3",
					},
				},
			},
		}
		tenantMgr := &MockTenantManager{data: namespaces}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants: map[string]*metrics.TenantStatus{
				"ns1": {
					ReadUnits:   11,
					WriteUnits:  12,
					SearchUnits: 13,
				},
				"ns2": {
					ReadUnits:   21,
					WriteUnits:  22,
					SearchUnits: 23,
				},
				"ns3": {
					ReadUnits:   31,
					WriteUnits:  32,
					SearchUnits: 33,
				},
				"doesNotExist": { // this should be ignored and other events should be processed as normal
					ReadUnits:   1,
					WriteUnits:  2,
					SearchUnits: 3,
				},
			},
		}}
		billing := &MockBillingSvc{
			createAccountCalls:  map[string]int{},
			defaultPlanCalls:    map[string]int{},
			pushUsageEventCalls: map[string][]*UsageEvent{},
		}

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, billing)
		err := reporter.push()
		require.NoError(t, err)

		// refreshNamespaceAccounts is only called once
		require.Equal(t, 1, tenantMgr.refreshNamespaceCalls)

		// CreateAccount call only for "ns1" and "ns3"
		require.Len(t, billing.createAccountCalls, 2)
		require.Equal(t, billing.createAccountCalls["ns1"], 1)
		require.Equal(t, billing.createAccountCalls["ns3"], 1)

		// AddDefaultPlan call only for "ns1" and "ns3"
		require.Len(t, billing.defaultPlanCalls, 2)
		require.Equal(t, billing.defaultPlanCalls[namespaces["ns1"].Accounts.Metronome.Id], 1)
		require.Equal(t, billing.defaultPlanCalls[namespaces["ns3"].Accounts.Metronome.Id], 1)

		for _, ns := range namespaces {
			// validate UpdateNamespaceMetadata adds metronome integration to all
			id, enabled := ns.Accounts.GetMetronomeId()
			require.Equal(t, true, enabled)
			require.NotNil(t, id)

			// validate usage events - transactionId, timestamp, dbUnits, searchUnits
			require.Len(t, billing.pushUsageEventCalls[ns.StrId], 1)
			event := billing.pushUsageEventCalls[ns.StrId][0]
			require.Equal(t, ns.StrId+"_1672531200", event.TransactionId)
			require.Equal(t, "2023-01-01T00:00:00Z", event.Timestamp)
			require.Equal(t, EventTypeUsage, event.EventType)

			stats := glbStatus.data.Tenants[ns.StrId]
			require.Equal(t, &map[string]interface{}{
				"search_units":   stats.SearchUnits,
				"database_units": stats.ReadUnits + stats.WriteUnits,
			}, event.Properties)
		}
	})

	t.Run("push empty events", func(t *testing.T) {
		tenantMgr := &MockTenantManager{data: map[string]metadata.NamespaceMetadata{}}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants:   map[string]*metrics.TenantStatus{},
		}}
		metronome := &MockBillingSvc{
			createAccountCalls:  map[string]int{},
			defaultPlanCalls:    map[string]int{},
			pushUsageEventCalls: map[string][]*UsageEvent{},
		}

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, metronome)
		err := reporter.push()
		require.NoError(t, err)

		// refreshNamespaceAccounts is never called
		require.Equal(t, 0, tenantMgr.refreshNamespaceCalls)

		// no calls should be made to metronome service
		require.Empty(t, metronome.createAccountCalls)
		require.Empty(t, metronome.defaultPlanCalls)
		require.Empty(t, metronome.pushUsageEventCalls)
	})

	t.Run("fails to create metronome account", func(t *testing.T) {
		namespaces := map[string]metadata.NamespaceMetadata{
			createAccountFailure: {
				Id:    4,
				StrId: createAccountFailure,
				Name:  "metronome account creation fails for this user",
			},
		}
		tenantMgr := &MockTenantManager{data: namespaces}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants: map[string]*metrics.TenantStatus{
				createAccountFailure: {
					ReadUnits:   4,
					WriteUnits:  5,
					SearchUnits: 6,
				},
			},
		}}
		metronome := &MockBillingSvc{
			createAccountCalls:  map[string]int{},
			defaultPlanCalls:    map[string]int{},
			pushUsageEventCalls: map[string][]*UsageEvent{},
		}

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, metronome)
		err := reporter.push()
		require.NoError(t, err)

		// refreshNamespaceAccounts is only called once
		require.Equal(t, 1, tenantMgr.refreshNamespaceCalls)

		require.Equal(t, 1, metronome.createAccountCalls[createAccountFailure])
		require.Empty(t, metronome.defaultPlanCalls)
		// should still publish events
		require.Contains(t, metronome.pushUsageEventCalls, createAccountFailure)
	})

	t.Run("fails to add default plan to metronome account", func(t *testing.T) {
		namespaces := map[string]metadata.NamespaceMetadata{
			addDefaultPlanFailure: {
				Id:    4,
				StrId: addDefaultPlanFailure,
				Name:  "Adding default plan fails for this user",
			},
		}
		tenantMgr := &MockTenantManager{data: namespaces}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants: map[string]*metrics.TenantStatus{
				addDefaultPlanFailure: {
					ReadUnits:   4,
					WriteUnits:  5,
					SearchUnits: 6,
				},
			},
		}}
		metronome := &MockBillingSvc{
			createAccountCalls:  map[string]int{},
			defaultPlanCalls:    map[string]int{},
			pushUsageEventCalls: map[string][]*UsageEvent{},
		}

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, metronome)
		err := reporter.push()
		require.NoError(t, err)

		// refreshNamespaceAccounts is only called once
		require.Equal(t, 1, tenantMgr.refreshNamespaceCalls)

		require.Equal(t, 1, metronome.createAccountCalls[addDefaultPlanFailure])
		require.Equal(t, 1, metronome.defaultPlanCalls[addDefaultPlanFailure])
		// should still publish events
		require.Contains(t, metronome.pushUsageEventCalls, addDefaultPlanFailure)
	})

	t.Run("fails to push events", func(t *testing.T) {
		namespaces := map[string]metadata.NamespaceMetadata{
			pushUsageFailure: {
				Id:    5,
				StrId: pushUsageFailure,
				Name:  "Failure to push events for this namespace",
			},
		}
		tenantMgr := &MockTenantManager{data: namespaces}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants: map[string]*metrics.TenantStatus{
				pushUsageFailure: {
					ReadUnits:   4,
					WriteUnits:  5,
					SearchUnits: 6,
				},
			},
		}}
		metronome := &MockBillingSvc{
			createAccountCalls:  map[string]int{},
			defaultPlanCalls:    map[string]int{},
			pushUsageEventCalls: map[string][]*UsageEvent{},
		}

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, metronome)
		err := reporter.push()
		require.Error(t, err)
	})
}

type MockTenantManager struct {
	data                  map[string]metadata.NamespaceMetadata
	refreshNamespaceCalls int
}

func (mock *MockTenantManager) GetTenant(_ context.Context, id string) (*metadata.Tenant, error) {
	return nil, fmt.Errorf("invalid tenant id %s", id)
}

func (mock *MockTenantManager) GetNamespaceMetadata(_ context.Context, namespaceId string) *metadata.NamespaceMetadata {
	if ns, ok := mock.data[namespaceId]; ok {
		return &ns
	}
	return nil
}

func (mock *MockTenantManager) UpdateNamespaceMetadata(_ context.Context, meta metadata.NamespaceMetadata) error {
	mock.data[meta.StrId] = meta
	return nil
}

func (mock *MockTenantManager) RefreshNamespaceAccounts(_ context.Context) error {
	mock.refreshNamespaceCalls += 1
	return nil
}

type MockGlobalStatus struct {
	data metrics.TenantStatusTimeChunk
}

func (mock *MockGlobalStatus) Flush() metrics.TenantStatusTimeChunk {
	return mock.data
}

type MockBillingSvc struct {
	createAccountCalls  map[string]int
	defaultPlanCalls    map[string]int
	pushUsageEventCalls map[string][]*UsageEvent
}

func (mock *MockBillingSvc) CreateAccount(_ context.Context, namespaceId string, _ string) (MetronomeId, error) {
	if count, ok := mock.createAccountCalls[namespaceId]; !ok {
		mock.createAccountCalls[namespaceId] = 1
	} else {
		mock.createAccountCalls[namespaceId] = count + 1
	}

	if namespaceId == createAccountFailure {
		return uuid.Nil, fmt.Errorf("metronome failure")
	}

	switch namespaceId {
	case createAccountFailure:
		return uuid.Nil, fmt.Errorf("metronome failure")
	case addDefaultPlanFailure:
		return uuid.Parse(addDefaultPlanFailure)
	}
	return uuid.New(), nil
}

func (mock *MockBillingSvc) AddDefaultPlan(_ context.Context, accountId MetronomeId) (bool, error) {
	if count, ok := mock.defaultPlanCalls[accountId.String()]; !ok {
		mock.defaultPlanCalls[accountId.String()] = 1
	} else {
		mock.defaultPlanCalls[accountId.String()] = count + 1
	}

	if accountId.String() == addDefaultPlanFailure {
		return false, fmt.Errorf("metronome failure")
	}

	return true, nil
}

func (mock *MockBillingSvc) AddPlan(_ context.Context, _ MetronomeId, _ uuid.UUID) (bool, error) {
	return false, nil
}

func (mock *MockBillingSvc) PushUsageEvents(_ context.Context, events []*UsageEvent) error {
	for _, e := range events {
		if _, ok := mock.pushUsageEventCalls[e.CustomerId]; !ok {
			mock.pushUsageEventCalls[e.CustomerId] = []*UsageEvent{e}
		} else {
			mock.pushUsageEventCalls[e.CustomerId] = append(mock.pushUsageEventCalls[e.CustomerId], e)
		}

		if e.CustomerId == pushUsageFailure {
			return fmt.Errorf("metronome failure")
		}
	}

	return nil
}

func (mock *MockBillingSvc) PushStorageEvents(_ context.Context, _ []*StorageEvent) error {
	panic("implement me")
}

func (mock *MockBillingSvc) GetInvoices(_ context.Context, _ MetronomeId, _ *api.ListInvoicesRequest) (*api.ListInvoicesResponse, error) {
	panic("implement me")
}

func (mock *MockBillingSvc) GetInvoiceById(_ context.Context, _ MetronomeId, _ string) (*api.ListInvoicesResponse, error) {
	panic("implement me")
}
