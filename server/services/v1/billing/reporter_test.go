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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

var mockKvStore kv.TxStore

func TestUsageReporter_pushUsage(t *testing.T) {
	mockTxMgr := transaction.NewManager(mockKvStore)
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
				Id:       3,
				StrId:    "ns3",
				Name:     "with metronome disabled",
				Accounts: metadata.AccountIntegrations{},
			},
			"ns4": {
				Id:    4,
				StrId: "ns4",
				Name:  "with empty metronome id",
				Accounts: metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: true,
						Id:      "",
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
				"ns4": {
					ReadUnits:   41,
					WriteUnits:  42,
					SearchUnits: 43,
				},
				"doesNotExist": { // this should be ignored and other events should be processed as normal
					ReadUnits:   1,
					WriteUnits:  2,
					SearchUnits: 3,
				},
			},
		}}

		mockProvider := NewMockProvider(t)

		// CreateAccount and AddDefaultPlan calls only for "ns1" and "ns3"
		for _, n := range []string{"ns1", "ns3", "ns4"} {
			mId := uuid.New()
			mockProvider.EXPECT().CreateAccount(mock.Anything, namespaces[n].StrId, namespaces[n].Name).
				Return(mId, nil).
				Once()
			mockProvider.EXPECT().AddDefaultPlan(mock.Anything, mId).
				Return(true, nil).
				Once()
		}

		// Usage events pushed for each namespace
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Len(t, events, 4)
				for _, e := range events {
					require.Contains(t, []string{"ns1", "ns2", "ns3", "ns4"}, e.CustomerId)

					expected, actual := glbStatus.data.Tenants[e.CustomerId], *e.Properties
					require.Equal(t, expected.WriteUnits+expected.ReadUnits, actual[UsageDbUnits])
					require.Equal(t, expected.SearchUnits, actual[UsageSearchUnits])
				}
				return nil
			}).Once()

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, tenantMgr, mockProvider, mockTxMgr)
		err := reporter.pushUsage()
		require.NoError(t, err)

		// refreshNamespaceAccounts is only called once
		require.Equal(t, 1, tenantMgr.refreshNamespaceCalls)

		for _, ns := range namespaces {
			// validate UpdateNamespaceMetadata adds metronome integration to all
			id, enabled := ns.Accounts.GetMetronomeId()
			require.Equal(t, true, enabled)
			require.NotNil(t, id)
		}
	})

	t.Run("push empty events", func(t *testing.T) {
		tenantMgr := &MockTenantManager{data: map[string]metadata.NamespaceMetadata{}}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants:   map[string]*metrics.TenantStatus{},
		}}

		mockProvider := NewMockProvider(t)
		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, tenantMgr, mockProvider, mockTxMgr)

		err := reporter.pushUsage()
		require.NoError(t, err)
		// refreshNamespaceAccounts is never called
		require.Equal(t, 0, tenantMgr.refreshNamespaceCalls)
		// no calls should be made to metronome provider
		require.Empty(t, mockProvider.Calls)
	})

	t.Run("fails to create metronome account", func(t *testing.T) {
		nsId := "createAccountFails"
		namespaces := map[string]metadata.NamespaceMetadata{
			nsId: {
				Id:    4,
				StrId: nsId,
				Name:  "metronome account creation fails for this user",
			},
		}
		tenantMgr := &MockTenantManager{data: namespaces}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants: map[string]*metrics.TenantStatus{
				nsId: {
					ReadUnits:   4,
					WriteUnits:  5,
					SearchUnits: 6,
				},
			},
		}}

		mockProvider := NewMockProvider(t)
		// create account fails
		mockProvider.EXPECT().CreateAccount(mock.Anything, nsId, mock.Anything).
			Return(uuid.Nil, fmt.Errorf("failed to create account")).
			Once()
		// should still push events
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Len(t, events, 1)
				require.Equal(t, nsId, events[0].CustomerId)
				return nil
			}).
			Once()

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, tenantMgr, mockProvider, mockTxMgr)
		err := reporter.pushUsage()
		require.NoError(t, err)

		// refreshNamespaceAccounts is only called once
		require.Equal(t, 1, tenantMgr.refreshNamespaceCalls)
	})

	t.Run("fails to add default plan to metronome account", func(t *testing.T) {
		nsId := "someId"
		namespaces := map[string]metadata.NamespaceMetadata{
			nsId: {
				Id:    4,
				StrId: nsId,
				Name:  "Adding default plan fails for this user",
			},
		}
		tenantMgr := &MockTenantManager{data: namespaces}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants: map[string]*metrics.TenantStatus{
				nsId: {
					ReadUnits:   4,
					WriteUnits:  5,
					SearchUnits: 6,
				},
			},
		}}
		mockProvider, mId := NewMockProvider(t), uuid.New()
		// create account succeeds
		mockProvider.EXPECT().CreateAccount(mock.Anything, nsId, namespaces[nsId].Name).
			Return(mId, nil).
			Once()
		// add default plan fails
		mockProvider.EXPECT().AddDefaultPlan(mock.Anything, mId).
			Return(false, fmt.Errorf("failed to add account")).
			Once()
		// should still push events
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Len(t, events, 1)
				require.Equal(t, nsId, events[0].CustomerId)

				props := *events[0].Properties
				require.Equal(t, int64(9), props[UsageDbUnits])
				require.Equal(t, int64(6), props[UsageSearchUnits])
				return nil
			}).
			Once()

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, tenantMgr, mockProvider, mockTxMgr)

		err := reporter.pushUsage()
		require.NoError(t, err)
		require.Equal(t, 1, tenantMgr.refreshNamespaceCalls)
	})

	t.Run("fails to push events", func(t *testing.T) {
		nsId := "PushUsageEvent_fails"
		namespaces := map[string]metadata.NamespaceMetadata{
			nsId: {
				Id:    5,
				StrId: nsId,
				Name:  "Failure to pushUsage events for this namespace",
				Accounts: metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: true,
						Id:      uuid.New().String(),
					},
				},
			},
		}
		tenantMgr := &MockTenantManager{data: namespaces}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Time{},
			Tenants: map[string]*metrics.TenantStatus{
				nsId: {
					ReadUnits:   4,
					WriteUnits:  5,
					SearchUnits: 6,
				},
			},
		}}

		mockProvider := NewMockProvider(t)
		// push usage call fails
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Len(t, events, 1)
				require.Equal(t, nsId, events[0].CustomerId)
				require.Equal(t, int64(9), (*events[0].Properties)[UsageDbUnits])
				require.Equal(t, int64(6), (*events[0].Properties)[UsageSearchUnits])

				return fmt.Errorf("failed to push usage events")
			}).Once()

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, tenantMgr, mockProvider, mockTxMgr)

		err := reporter.pushUsage()
		require.ErrorContains(t, err, "failed to push usage events")
	})

	t.Run("tenant does not exist", func(t *testing.T) {
		tenantMgr := &MockTenantManager{data: map[string]metadata.NamespaceMetadata{}}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Time{},
			Tenants: map[string]*metrics.TenantStatus{
				"ns1": {
					SearchUnits: 6,
				},
			},
		}}
		mockProvider := NewMockProvider(t)
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Empty(t, events)
				return nil
			}).Once()
		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, tenantMgr, mockProvider, mockTxMgr)
		err := reporter.pushUsage()
		require.NoError(t, err)
	})

	t.Run("metronome gets disabled for invalid namespaceId", func(t *testing.T) {
		nsId := ""
		namespaces := map[string]metadata.NamespaceMetadata{
			nsId: {
				Id:    1,
				StrId: nsId,
				Accounts: metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: true,
						Id:      uuid.New().String(),
					},
				},
			},
		}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Time{},
			Tenants: map[string]*metrics.TenantStatus{
				nsId: {
					SearchUnits: 6,
				},
			},
		}}
		tenantMgr := &MockTenantManager{data: namespaces}

		mockProvider := NewMockProvider(t)
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Empty(t, events)
				return nil
			}).Once()

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, tenantMgr, mockProvider, mockTxMgr)
		err := reporter.pushUsage()
		require.NoError(t, err)
		updated := tenantMgr.GetNamespaceMetadata(context.TODO(), nsId)
		require.False(t, updated.Accounts.Metronome.Enabled)
	})

	t.Run("tenant is skipped if metronome is disabled", func(t *testing.T) {
		nsId := "ns1"
		namespaces := map[string]metadata.NamespaceMetadata{
			nsId: {
				Id:    1,
				StrId: nsId,
				Accounts: metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: false,
						Id:      uuid.New().String(),
					},
				},
			},
		}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Time{},
			Tenants: map[string]*metrics.TenantStatus{
				nsId: {
					SearchUnits: 6,
				},
			},
		}}
		tenantMgr := &MockTenantManager{data: namespaces}

		mockProvider := NewMockProvider(t)
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Empty(t, events)
				return nil
			}).Once()

		reporter, _ := NewUsageReporter(glbStatus, tenantMgr, tenantMgr, mockProvider, mockTxMgr)
		err := reporter.pushUsage()
		require.NoError(t, err)
	})
}

type MockTenantManager struct {
	data                  map[string]metadata.NamespaceMetadata
	refreshNamespaceCalls int
}

func (mock *MockTenantManager) GetTenant(_ context.Context, id string) (*metadata.Tenant, error) {
	return nil, fmt.Errorf("invalid tenant id %s", id)
}

func (mock *MockTenantManager) AllTenants(_ context.Context) []*metadata.Tenant {
	return []*metadata.Tenant{}
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
