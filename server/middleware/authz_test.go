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

package middleware

import (
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

func TestAuthzOwnerRole(t *testing.T) {
	require.True(t, isAuthorizedOperation(api.BeginTransactionMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CommitTransactionMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.RollbackTransactionMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.InsertMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ReplaceMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ReadMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CountMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.BuildCollectionIndexMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ExplainMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ImportMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateCollectionMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateCollectionsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DropCollectionMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListProjectsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListCollectionsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateProjectMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteProjectMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeDatabaseMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeCollectionMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateBranchMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteBranchMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListBranchesMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListAppKeysMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.RotateAppKeySecretMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateGlobalAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateGlobalAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteGlobalAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListGlobalAppKeysMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.RotateGlobalAppKeySecretMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.IndexCollection, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchIndexCollectionMethodName, ownerRoleName))

	// auth
	require.True(t, isAuthorizedOperation(api.GetAccessTokenMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateInvitationsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteInvitationsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListInvitationsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListUsersMethodName, ownerRoleName))

	// billing
	require.True(t, isAuthorizedOperation(api.ListInvoicesMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.BillingGetUsageMethodName, ownerRoleName))

	// cache
	require.True(t, isAuthorizedOperation(api.CreateCacheMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListCachesMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteCacheMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SetMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.GetSetMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.GetMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DelMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.KeysMethodName, ownerRoleName))

	// health
	require.True(t, isAuthorizedOperation(api.HealthMethodName, ownerRoleName))

	// management
	require.True(t, isAuthorizedOperation(api.InsertUserMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.GetUserMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateUserMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.InsertNamespaceMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.GetNamespaceMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateNamespaceMetadataMethodName, ownerRoleName))

	// observability
	require.True(t, isAuthorizedOperation(api.QueryTimeSeriesMetricsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaLimitsMetricsMethodName, ownerRoleName))

	// realtime
	require.True(t, isAuthorizedOperation(api.PresenceMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.GetRTChannelMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.GetRTChannelsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ReadMessagesMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.MessagesMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListSubscriptionsMethodName, ownerRoleName))

	// search
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateIndexMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.GetIndexMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteIndexMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.ListIndexesMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchGetMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreateById, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreate, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreateOrReplace, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchUpdate, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchDeleteByQuery, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchSearch, ownerRoleName))

	// negative
	require.False(t, isAuthorizedOperation(api.VerifyInvitationMethodName, ownerRoleName))
	require.False(t, isAuthorizedOperation(api.CreateNamespaceMethodName, ownerRoleName))
	require.False(t, isAuthorizedOperation(api.ListNamespacesMethodName, ownerRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteNamespaceMethodName, ownerRoleName))
}

func TestAuthzEditorRole(t *testing.T) {
	// db
	require.True(t, isAuthorizedOperation(api.BeginTransactionMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.CommitTransactionMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.RollbackTransactionMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.InsertMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ReplaceMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ReadMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.CountMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.BuildCollectionIndexMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ExplainMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ImportMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateCollectionMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateCollectionsMethodName, ownerRoleName))
	require.True(t, isAuthorizedOperation(api.DropCollectionMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ListProjectsMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ListCollectionsMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateProjectMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteProjectMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeDatabaseMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeCollectionMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateBranchMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteBranchMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ListBranchesMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateAppKeyMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateAppKeyMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteAppKeyMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ListAppKeysMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.RotateAppKeySecretMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.IndexCollection, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchIndexCollectionMethodName, editorRoleName))

	// auth
	require.True(t, isAuthorizedOperation(api.GetAccessTokenMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateInvitationsMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteInvitationsMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ListInvitationsMethodName, editorRoleName))

	// cache
	require.True(t, isAuthorizedOperation(api.CreateCacheMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ListCachesMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteCacheMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SetMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.GetSetMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.GetMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DelMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.KeysMethodName, editorRoleName))

	// health
	require.True(t, isAuthorizedOperation(api.HealthMethodName, editorRoleName))

	// management
	require.True(t, isAuthorizedOperation(api.InsertUserMetadataMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.GetUserMetadataMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateUserMetadataMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.InsertNamespaceMetadataMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.GetNamespaceMetadataMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateNamespaceMetadataMethodName, editorRoleName))

	// observability
	require.True(t, isAuthorizedOperation(api.QueryTimeSeriesMetricsMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaLimitsMetricsMethodName, editorRoleName))

	// realtime
	require.True(t, isAuthorizedOperation(api.PresenceMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.GetRTChannelMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.GetRTChannelsMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ReadMessagesMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.MessagesMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ListSubscriptionsMethodName, editorRoleName))

	// search
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateIndexMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.GetIndexMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteIndexMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.ListIndexesMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchGetMethodName, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreateById, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreate, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreateOrReplace, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchUpdate, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchDeleteByQuery, editorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchSearch, editorRoleName))

	// negative
	require.False(t, isAuthorizedOperation(api.ListUsersMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.VerifyInvitationMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.CreateNamespaceMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.ListNamespacesMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteNamespaceMethodName, editorRoleName))

	require.False(t, isAuthorizedOperation(api.CreateGlobalAppKeyMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateGlobalAppKeyMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteGlobalAppKeyMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.ListGlobalAppKeysMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.RotateGlobalAppKeySecretMethodName, editorRoleName))
	require.False(t, isAuthorizedOperation(api.BillingGetUsageMethodName, editorRoleName))
}

func TestAuthzReadOnlyRole(t *testing.T) {
	// db
	require.True(t, isAuthorizedOperation(api.ReadMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.CountMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.ExplainMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.SearchMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.ListProjectsMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeDatabaseMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeCollectionMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.ListBranchesMethodName, readOnlyRoleName))

	// auth
	require.True(t, isAuthorizedOperation(api.ListAppKeysMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.GetAccessTokenMethodName, readOnlyRoleName))

	// cache
	require.True(t, isAuthorizedOperation(api.ListCachesMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.GetMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.KeysMethodName, readOnlyRoleName))

	// health
	require.True(t, isAuthorizedOperation(api.HealthMethodName, readOnlyRoleName))

	// management
	require.True(t, isAuthorizedOperation(api.GetUserMetadataMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.InsertUserMetadataMethodName, readOnlyRoleName)) // exception for user preferences
	require.True(t, isAuthorizedOperation(api.UpdateUserMetadataMethodName, readOnlyRoleName)) // exception for user preferences

	// observability
	require.True(t, isAuthorizedOperation(api.QueryTimeSeriesMetricsMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaLimitsMetricsMethodName, readOnlyRoleName))

	// realtime
	require.True(t, isAuthorizedOperation(api.ReadMessagesMethodName, readOnlyRoleName))

	// search
	require.True(t, isAuthorizedOperation(api.GetIndexMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.ListIndexesMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.SearchGetMethodName, readOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.SearchSearch, readOnlyRoleName))

	// negative
	require.False(t, isAuthorizedOperation(api.BeginTransactionMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CommitTransactionMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.RollbackTransactionMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.InsertMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateProjectMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateOrUpdateCollectionMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateOrUpdateCollectionsMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteProjectMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DropCollectionMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateCacheMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SetMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.GetSetMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DelMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateBranchMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteBranchMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.RotateAppKeySecretMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateInvitationsMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteInvitationsMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.IndexCollection, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchIndexCollectionMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.ListUsersMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.VerifyInvitationMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateNamespaceMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.ListNamespacesMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteNamespaceMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateGlobalAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateGlobalAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteGlobalAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.ListGlobalAppKeysMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.RotateGlobalAppKeySecretMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.BillingGetUsageMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.ListInvoicesMethodName, readOnlyRoleName))

	// search
	require.False(t, isAuthorizedOperation(api.CreateOrUpdateIndexMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteIndexMethodName, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchCreateById, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchCreate, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchCreateOrReplace, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchUpdate, readOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchDeleteByQuery, readOnlyRoleName))
}
