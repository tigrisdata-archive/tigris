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
	"github.com/tigrisdata/tigris/server/defaults"
)

var mockKvStore kv.TxStore

func TestUsageReporter_pushUsage(t *testing.T) {
	mockTxMgr := transaction.NewManager(mockKvStore)
	t.Run("pushes all valid events", func(t *testing.T) {
		namespaces := map[string]metadata.NamespaceMetadata{
			"ns1": {
				Id:    1,
				StrId: "ns1",
				Name:  "enabled metronome account",
				Accounts: &metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: true,
						Id:      "m1",
					},
				},
			},
			"ns2": {
				Id:    2,
				StrId: "ns2",
				Name:  "with metronome integration",
				Accounts: &metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: true,
						Id:      "m2",
					},
				},
			},
			"ns3": {
				Id:    3,
				StrId: "ns3",
				Name:  "disabled metronome account",
				Accounts: &metadata.AccountIntegrations{
					Metronome: &metadata.Metronome{
						Enabled: false,
						Id:      "m3",
					},
				},
			},
		}
		//tenantMgr := &MockTenantManager{data: namespaces}
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

		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		reporter, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)

		// Usage events pushed for each namespace
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Len(t, events, 2)
				for _, e := range events {
					require.Contains(t, []string{"ns1", "ns2"}, e.CustomerId)

					expected, actual := glbStatus.data.Tenants[e.CustomerId], *e.Properties
					require.Equal(t, expected.WriteUnits+expected.ReadUnits, actual[UsageDbUnits])
					require.Equal(t, expected.SearchUnits, actual[UsageSearchUnits])
				}
				return nil
			}).Once()

		mockTenantMgr.EXPECT().GetNamespaceMetadata(reporter.ctx, mock.Anything).
			RunAndReturn(func(ctx context.Context, s string) *metadata.NamespaceMetadata {
				if n, ok := namespaces[s]; ok {
					return &n
				}
				return nil
			}).
			Times(2 * len(glbStatus.data.Tenants))
		mockTenantMgr.EXPECT().RefreshNamespaceAccounts(reporter.ctx).Return(nil).Once()

		err := reporter.pushUsage()
		require.NoError(t, err)
	})

	t.Run("push empty events", func(t *testing.T) {
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			Tenants:   map[string]*metrics.TenantStatus{},
		}}

		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		reporter, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)

		err := reporter.pushUsage()
		require.NoError(t, err)
		// mocks not called
		require.Empty(t, mockTenantMgr.Calls)
		require.Empty(t, mockProvider.Calls)
	})

	t.Run("fails to push events", func(t *testing.T) {
		ns := &metadata.NamespaceMetadata{
			Id:    5,
			StrId: "PushUsageEvent_fails",
			Name:  "Failure to pushUsage events for this namespace",
			Accounts: &metadata.AccountIntegrations{
				Metronome: &metadata.Metronome{
					Enabled: true,
					Id:      uuid.New().String(),
				},
			},
		}
		glbStatus := &MockGlobalStatus{data: metrics.TenantStatusTimeChunk{
			StartTime: time.Time{},
			EndTime:   time.Time{},
			Tenants: map[string]*metrics.TenantStatus{
				ns.StrId: {
					ReadUnits:   4,
					WriteUnits:  5,
					SearchUnits: 6,
				},
			},
		}}

		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		reporter, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)

		mockTenantMgr.EXPECT().GetNamespaceMetadata(reporter.ctx, ns.StrId).
			Return(ns).
			Times(2)

		// push usage call fails
		mockProvider.EXPECT().PushUsageEvents(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, events []*UsageEvent) error {
				require.Len(t, events, 1)
				require.Equal(t, ns.StrId, events[0].CustomerId)
				require.Equal(t, int64(9), (*events[0].Properties)[UsageDbUnits])
				require.Equal(t, int64(6), (*events[0].Properties)[UsageSearchUnits])

				return fmt.Errorf("failed to push usage events")
			}).Once()

		err := reporter.pushUsage()
		require.ErrorContains(t, err, "failed to push usage events")
	})
}

func TestUsageReporter_setupBillingAccount(t *testing.T) {
	glbStatus := &MockGlobalStatus{}
	mockTxMgr := transaction.NewManager(mockKvStore)

	t.Run("with nil metadata", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)

		success := r.setupBillingAccount(nil)
		require.False(t, success)
	})

	t.Run("with default namespace", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)

		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: defaults.DefaultNamespaceName,
			Name:  "test namespace",
		}

		mockTenantMgr.EXPECT().UpdateNamespaceMetadata(r.ctx, mock.Anything).
			RunAndReturn(func(ctx context.Context, actual metadata.NamespaceMetadata) error {
				require.Equal(t, nsMeta.StrId, actual.StrId)
				require.Equal(t, nsMeta.Id, actual.Id)
				require.NotNil(t, actual.Accounts)
				require.NotNil(t, actual.Accounts.Metronome)
				require.False(t, actual.Accounts.Metronome.Enabled)
				require.Equal(t, actual.Accounts.Metronome.Id, "")
				return nil
			}).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.False(t, success)
	})

	t.Run("with empty namespaceId", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)

		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "",
			Name:  "test namespace",
			Accounts: &metadata.AccountIntegrations{Metronome: &metadata.Metronome{
				Enabled: true,
				Id:      "123",
			}},
		}

		mockTenantMgr.EXPECT().UpdateNamespaceMetadata(r.ctx, mock.Anything).
			RunAndReturn(func(ctx context.Context, actual metadata.NamespaceMetadata) error {
				require.Equal(t, "", actual.StrId)
				require.Equal(t, nsMeta.Id, actual.Id)
				require.NotNil(t, actual.Accounts)
				require.NotNil(t, actual.Accounts.Metronome)
				require.False(t, actual.Accounts.Metronome.Enabled)
				require.Equal(t, nsMeta.Accounts.Metronome.Id, actual.Accounts.Metronome.Id)
				return nil
			}).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.False(t, success)
	})

	t.Run("when metronome is disabled", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)

		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
			Accounts: &metadata.AccountIntegrations{Metronome: &metadata.Metronome{
				Enabled: false,
				Id:      "123",
			}},
		}

		success := r.setupBillingAccount(nsMeta)
		require.False(t, success)
	})

	t.Run("when metronome id exists", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)

		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
			Accounts: &metadata.AccountIntegrations{Metronome: &metadata.Metronome{
				Enabled: true,
				Id:      "123",
			}},
		}

		success := r.setupBillingAccount(nsMeta)
		require.True(t, success)
	})

	t.Run("when account creation succeeds", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)
		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
		}
		expectedAccountId := uuid.New()
		mockProvider.EXPECT().CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name).Return(expectedAccountId, nil).Once()
		mockProvider.EXPECT().AddDefaultPlan(r.ctx, expectedAccountId).Return(true, nil).Once()
		mockTenantMgr.EXPECT().UpdateNamespaceMetadata(r.ctx, mock.Anything).
			RunAndReturn(func(ctx context.Context, actual metadata.NamespaceMetadata) error {
				require.True(t, actual.Accounts.Metronome.Enabled)
				require.Equal(t, actual.Accounts.Metronome.Id, expectedAccountId.String())
				return nil
			}).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.True(t, success)
	})

	t.Run("when adding default plan fails", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)
		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
		}
		expectedAccountId := uuid.New()
		mockProvider.EXPECT().CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name).Return(expectedAccountId, nil).Once()
		mockProvider.EXPECT().AddDefaultPlan(r.ctx, expectedAccountId).
			Return(false, fmt.Errorf("failed to add default plan")).
			Once()
		mockTenantMgr.EXPECT().UpdateNamespaceMetadata(r.ctx, mock.Anything).
			RunAndReturn(func(ctx context.Context, actual metadata.NamespaceMetadata) error {
				require.True(t, actual.Accounts.Metronome.Enabled)
				require.Equal(t, actual.Accounts.Metronome.Id, expectedAccountId.String())
				return nil
			}).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.True(t, success)
	})

	t.Run("when creating account fails because of HTTP 409 - conflict", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)
		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
		}
		expectedAccountId := uuid.New()
		mockProvider.EXPECT().CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name).
			Return(uuid.Nil, NewMetronomeError(409, []byte("conflict"))).
			Once()
		mockProvider.EXPECT().GetAccountId(r.ctx, nsMeta.StrId).Return(expectedAccountId, nil).Once()
		mockTenantMgr.EXPECT().UpdateNamespaceMetadata(r.ctx, mock.Anything).
			RunAndReturn(func(ctx context.Context, actual metadata.NamespaceMetadata) error {
				require.True(t, actual.Accounts.Metronome.Enabled)
				require.Equal(t, actual.Accounts.Metronome.Id, expectedAccountId.String())
				return nil
			}).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.True(t, success)
	})

	t.Run("when creating account fails for Bad Request", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)
		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
		}
		mockProvider.EXPECT().CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name).
			Return(uuid.Nil, NewMetronomeError(400, []byte("bad request"))).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.False(t, success)
	})

	t.Run("when creating account fails for unexpected error", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)
		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
		}
		mockProvider.EXPECT().CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name).
			Return(uuid.Nil, fmt.Errorf("remote service error")).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.False(t, success)
	})

	t.Run("when getting account id from billing service fails", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)
		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
		}
		mockProvider.EXPECT().CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name).
			Return(uuid.Nil, NewMetronomeError(409, []byte("conflict"))).
			Once()
		mockProvider.EXPECT().GetAccountId(r.ctx, nsMeta.StrId).
			Return(uuid.Nil, NewMetronomeError(403, []byte("Unauthorized"))).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.True(t, success)
	})

	t.Run("when updating namespace metadata fails", func(t *testing.T) {
		mockProvider, mockTenantMgr, mockTenantGetter := NewMockProvider(t), metadata.NewMockNamespaceMetadataMgr(t), metadata.NewMockTenantGetter(t)
		r, _ := NewUsageReporter(glbStatus, mockTenantMgr, mockTenantGetter, mockProvider, mockTxMgr)
		nsMeta := &metadata.NamespaceMetadata{
			Id:    1,
			StrId: "ns123",
			Name:  "test namespace",
		}
		expectedAccountId := uuid.New()
		mockProvider.EXPECT().CreateAccount(r.ctx, nsMeta.StrId, nsMeta.Name).Return(expectedAccountId, nil).Once()
		mockProvider.EXPECT().AddDefaultPlan(r.ctx, expectedAccountId).Return(true, nil).Once()
		mockTenantMgr.EXPECT().UpdateNamespaceMetadata(r.ctx, mock.Anything).
			Return(fmt.Errorf("failed to update namespace metadata")).
			Once()

		success := r.setupBillingAccount(nsMeta)
		require.True(t, success)
	})
}

type MockTenantManager struct {
	data                  map[string]metadata.NamespaceMetadata
	refreshNamespaceCalls int
	updateNamespaceCalls  map[string]int
}

func (*MockTenantManager) GetTenant(_ context.Context, id string) (*metadata.Tenant, error) {
	return nil, fmt.Errorf("invalid tenant id %s", id)
}

func (*MockTenantManager) AllTenants(_ context.Context) []*metadata.Tenant {
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
	mock.refreshNamespaceCalls++
	return nil
}

type MockGlobalStatus struct {
	data metrics.TenantStatusTimeChunk
}

func (mock *MockGlobalStatus) Flush() metrics.TenantStatusTimeChunk {
	return mock.data
}
