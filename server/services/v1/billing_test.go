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

package v1

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/metadata"
	mMock "github.com/tigrisdata/tigris/server/metadata/mocks"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/billing"
	bMock "github.com/tigrisdata/tigris/server/services/v1/billing/mocks"
)

type billingServiceSuite struct {
	suite.Suite
	mockProvider     *bMock.Provider
	mockNameSpaceMgr *mMock.NamespaceMetadataMgr
	billing          *billingService
	ctx              context.Context
	nsMeta           metadata.NamespaceMetadata
	mId              billing.MetronomeId
}

func TestBillingServiceSuite(t *testing.T) {
	s := &billingServiceSuite{
		Suite:            suite.Suite{},
		mockProvider:     bMock.NewProvider(t),
		mockNameSpaceMgr: mMock.NewNamespaceMetadataMgr(t),
		billing:          nil,
		ctx:              context.TODO(),
		nsMeta:           metadata.NewNamespaceMetadata(1, "test_namespace", "test namespace"),
		mId:              uuid.New(),
	}
	suite.Run(t, s)
}

func (s *billingServiceSuite) SetupSuite() {
	s.billing = newBillingService(s.mockProvider, s.mockNameSpaceMgr)
}

func (s *billingServiceSuite) SetupTest() {
	md := request.Metadata{}
	md.SetNamespace(s.ctx, s.nsMeta.StrId)
	s.ctx = context.WithValue(context.TODO(), request.MetadataCtxKey{}, &md)

	mockNS := metadata.NewNamespaceMetadata(1, s.nsMeta.StrId, "test namespace")
	mockNS.Accounts.AddMetronome(s.mId.String())
	s.mockNameSpaceMgr.EXPECT().GetNamespaceMetadata(s.ctx, s.nsMeta.StrId).Return(&mockNS).Once()
}

func (s *billingServiceSuite) Test_ListInvoices_WithInvoiceId_Succeeds() {
	mockReq, mockResp := &api.ListInvoicesRequest{InvoiceId: "xyz"}, &api.ListInvoicesResponse{}
	s.mockProvider.On("GetInvoiceById", s.ctx, s.mId, mockReq.InvoiceId).Return(mockResp, nil)

	resp, err := s.billing.ListInvoices(s.ctx, mockReq)
	require.NoError(s.T(), err)
	require.Equal(s.T(), mockResp, resp)

	s.mockNameSpaceMgr.AssertCalled(s.T(), "GetNamespaceMetadata", s.ctx, s.nsMeta.StrId)
	s.mockProvider.AssertNumberOfCalls(s.T(), "GetInvoiceById", 1)
	s.mockProvider.AssertNotCalled(s.T(), "GetInvoices")
}

func (s *billingServiceSuite) Test_ListInvoices_ForInvoices_Succeeds() {
	mockReq, mockResp := &api.ListInvoicesRequest{}, &api.ListInvoicesResponse{}
	s.mockProvider.On("GetInvoices", s.ctx, s.mId, mockReq).Return(mockResp, nil)

	resp, err := s.billing.ListInvoices(s.ctx, mockReq)
	require.NoError(s.T(), err)
	require.Equal(s.T(), mockResp, resp)

	s.mockNameSpaceMgr.AssertCalled(s.T(), "GetNamespaceMetadata", s.ctx, s.nsMeta.StrId)
	s.mockProvider.AssertNumberOfCalls(s.T(), "GetInvoices", 1)
	s.mockProvider.AssertNotCalled(s.T(), "GetInvoiceById")
}

func (s *billingServiceSuite) Test_ListInvoices_WithNoNamespace_Fails() {
	_ = s.mockNameSpaceMgr.GetNamespaceMetadata(s.ctx, s.nsMeta.StrId)

	mockReq := &api.ListInvoicesRequest{}
	r, err := s.billing.ListInvoices(context.TODO(), mockReq)
	require.ErrorContains(s.T(), err, "namespace not found")
	require.Nil(s.T(), r)

	s.mockProvider.AssertNotCalled(s.T(), "GetInvoices")
	s.mockProvider.AssertNotCalled(s.T(), "GetInvoiceById")
}

type getMetronomeIdSuite struct {
	suite.Suite
	mockProvider     *bMock.Provider
	mockNameSpaceMgr *mMock.NamespaceMetadataMgr
	billing          *billingService
	ctx              context.Context
}

func TestGetMetronomeIdSuite(t *testing.T) {
	s := &getMetronomeIdSuite{
		Suite:            suite.Suite{},
		mockProvider:     bMock.NewProvider(t),
		mockNameSpaceMgr: mMock.NewNamespaceMetadataMgr(t),
		billing:          nil,
		ctx:              context.TODO(),
	}
	suite.Run(t, s)
}

func (s *getMetronomeIdSuite) SetupSuite() {
	s.billing = newBillingService(s.mockProvider, s.mockNameSpaceMgr)
}

func (s *getMetronomeIdSuite) TestNoNameSpaceFound() {
	ns := "invalid_namespace"
	s.mockNameSpaceMgr.EXPECT().GetNamespaceMetadata(s.ctx, ns).Return(nil).Once()
	id, err := s.billing.getMetronomeId(s.ctx, ns)
	require.ErrorContains(s.T(), err, "invalid_namespace not found")
	require.Equal(s.T(), uuid.Nil, id)
}

func (s *getMetronomeIdSuite) TestMetronomeNotEnabled() {
	nsMeta := metadata.NewNamespaceMetadata(1, "test_namespace", "test namespace")
	s.mockNameSpaceMgr.On("GetNamespaceMetadata", s.ctx, nsMeta.StrId).Return(&nsMeta).Once()

	id, err := s.billing.getMetronomeId(s.ctx, nsMeta.StrId)
	require.ErrorContains(s.T(), err, "No account linked")
	require.Equal(s.T(), id, uuid.Nil)
}

func (s *getMetronomeIdSuite) TestMetronomeIdInvalid() {
	nsMeta := metadata.NewNamespaceMetadata(1, "test_namespace", "test namespace")
	nsMeta.Accounts.AddMetronome("xyz")
	s.mockNameSpaceMgr.On("GetNamespaceMetadata", s.ctx, nsMeta.StrId).Return(&nsMeta).Once()

	id, err := s.billing.getMetronomeId(s.ctx, nsMeta.StrId)
	require.ErrorContains(s.T(), err, "invalid UUID length")
	require.Equal(s.T(), id, uuid.Nil)
}

func (s *getMetronomeIdSuite) TestSucceeds() {
	nsMeta := metadata.NewNamespaceMetadata(1, "test_namespace", "test namespace")
	expectedId := uuid.New()
	nsMeta.Accounts.AddMetronome(expectedId.String())
	s.mockNameSpaceMgr.On("GetNamespaceMetadata", s.ctx, nsMeta.StrId).Return(&nsMeta).Once()

	actualId, err := s.billing.getMetronomeId(s.ctx, nsMeta.StrId)
	require.NoError(s.T(), err)
	require.Equal(s.T(), expectedId, actualId)
}
