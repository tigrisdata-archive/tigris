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
	"github.com/tigrisdata/tigris/server/services/v1/auth"
)

func TestAuthzOwnerRole(t *testing.T) {
	require.True(t, isAuthorizedOperation(api.BeginTransactionMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CommitTransactionMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.RollbackTransactionMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.InsertMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ReplaceMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ReadMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CountMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.BuildCollectionIndexMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ExplainMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ImportMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateCollectionMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateCollectionsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DropCollectionMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListProjectsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateProjectMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListCollectionsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateProjectMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteProjectMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeDatabaseMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeCollectionMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateBranchMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteBranchMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListBranchesMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateAppKeyMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateAppKeyMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteAppKeyMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListAppKeysMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.RotateAppKeySecretMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateGlobalAppKeyMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateGlobalAppKeyMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteGlobalAppKeyMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListGlobalAppKeysMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.RotateGlobalAppKeySecretMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.IndexCollection, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchIndexCollectionMethodName, auth.OwnerRoleName))

	// auth
	require.True(t, isAuthorizedOperation(api.GetAccessTokenMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.CreateInvitationsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteInvitationsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListInvitationsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListUsersMethodName, auth.OwnerRoleName))

	// billing
	require.True(t, isAuthorizedOperation(api.ListInvoicesMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.BillingGetUsageMethodName, auth.OwnerRoleName))

	// cache
	require.True(t, isAuthorizedOperation(api.CreateCacheMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListCachesMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteCacheMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SetMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.GetSetMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.GetMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DelMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.KeysMethodName, auth.OwnerRoleName))

	// health
	require.True(t, isAuthorizedOperation(api.HealthMethodName, auth.OwnerRoleName))

	// management
	require.True(t, isAuthorizedOperation(api.InsertUserMetadataMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.GetUserMetadataMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateUserMetadataMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.InsertNamespaceMetadataMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.GetNamespaceMetadataMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateNamespaceMetadataMethodName, auth.OwnerRoleName))

	// observability
	require.True(t, isAuthorizedOperation(api.QueryTimeSeriesMetricsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaLimitsMetricsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaUsageMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.GetInfoMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.WhoAmIMethodName, auth.OwnerRoleName))

	// realtime
	require.True(t, isAuthorizedOperation(api.PresenceMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.GetRTChannelMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.GetRTChannelsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ReadMessagesMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.MessagesMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListSubscriptionsMethodName, auth.OwnerRoleName))

	// search
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateIndexMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.GetIndexMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteIndexMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.ListIndexesMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchGetMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreateById, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreate, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreateOrReplace, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchUpdate, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchDeleteByQuery, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.SearchSearch, auth.OwnerRoleName))

	// negative
	require.False(t, isAuthorizedOperation(api.VerifyInvitationMethodName, auth.OwnerRoleName))
	require.False(t, isAuthorizedOperation(api.CreateNamespaceMethodName, auth.OwnerRoleName))
	require.False(t, isAuthorizedOperation(api.ListNamespacesMethodName, auth.OwnerRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteNamespaceMethodName, auth.OwnerRoleName))
}

func TestAuthzEditorRole(t *testing.T) {
	// db
	require.True(t, isAuthorizedOperation(api.BeginTransactionMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.CommitTransactionMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.RollbackTransactionMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.InsertMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ReplaceMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ReadMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.CountMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.BuildCollectionIndexMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ExplainMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ImportMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateCollectionMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateCollectionsMethodName, auth.OwnerRoleName))
	require.True(t, isAuthorizedOperation(api.DropCollectionMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ListProjectsMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ListCollectionsMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateProjectMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateProjectMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteProjectMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeDatabaseMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeCollectionMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateBranchMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteBranchMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ListBranchesMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateAppKeyMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateAppKeyMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteAppKeyMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ListAppKeysMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.RotateAppKeySecretMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.IndexCollection, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchIndexCollectionMethodName, auth.EditorRoleName))

	// auth
	require.True(t, isAuthorizedOperation(api.GetAccessTokenMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.CreateInvitationsMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteInvitationsMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ListInvitationsMethodName, auth.EditorRoleName))

	// cache
	require.True(t, isAuthorizedOperation(api.CreateCacheMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ListCachesMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteCacheMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SetMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.GetSetMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.GetMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DelMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.KeysMethodName, auth.EditorRoleName))

	// health
	require.True(t, isAuthorizedOperation(api.HealthMethodName, auth.EditorRoleName))

	// management
	require.True(t, isAuthorizedOperation(api.InsertUserMetadataMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.GetUserMetadataMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateUserMetadataMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.InsertNamespaceMetadataMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.GetNamespaceMetadataMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.UpdateNamespaceMetadataMethodName, auth.EditorRoleName))

	// observability
	require.True(t, isAuthorizedOperation(api.QueryTimeSeriesMetricsMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaLimitsMetricsMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaUsageMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.GetInfoMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.WhoAmIMethodName, auth.EditorRoleName))

	// realtime
	require.True(t, isAuthorizedOperation(api.PresenceMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.GetRTChannelMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.GetRTChannelsMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ReadMessagesMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.MessagesMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ListSubscriptionsMethodName, auth.EditorRoleName))

	// search
	require.True(t, isAuthorizedOperation(api.CreateOrUpdateIndexMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.GetIndexMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.DeleteIndexMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.ListIndexesMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchGetMethodName, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreateById, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreate, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchCreateOrReplace, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchUpdate, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchDeleteByQuery, auth.EditorRoleName))
	require.True(t, isAuthorizedOperation(api.SearchSearch, auth.EditorRoleName))

	// negative
	require.False(t, isAuthorizedOperation(api.ListUsersMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.VerifyInvitationMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.CreateNamespaceMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.ListNamespacesMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteNamespaceMethodName, auth.EditorRoleName))

	require.False(t, isAuthorizedOperation(api.CreateGlobalAppKeyMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateGlobalAppKeyMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteGlobalAppKeyMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.ListGlobalAppKeysMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.RotateGlobalAppKeySecretMethodName, auth.EditorRoleName))
	require.False(t, isAuthorizedOperation(api.BillingGetUsageMethodName, auth.EditorRoleName))
}

func TestAuthzReadOnlyRole(t *testing.T) {
	// db
	require.True(t, isAuthorizedOperation(api.ReadMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.CountMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.ExplainMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.SearchMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.ListProjectsMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeDatabaseMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.DescribeCollectionMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.ListBranchesMethodName, auth.ReadOnlyRoleName))

	// auth
	require.True(t, isAuthorizedOperation(api.ListAppKeysMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.GetAccessTokenMethodName, auth.ReadOnlyRoleName))

	// cache
	require.True(t, isAuthorizedOperation(api.ListCachesMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.GetMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.KeysMethodName, auth.ReadOnlyRoleName))

	// health
	require.True(t, isAuthorizedOperation(api.HealthMethodName, auth.ReadOnlyRoleName))

	// management
	require.True(t, isAuthorizedOperation(api.GetUserMetadataMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.InsertUserMetadataMethodName, auth.ReadOnlyRoleName)) // exception for user preferences
	require.True(t, isAuthorizedOperation(api.UpdateUserMetadataMethodName, auth.ReadOnlyRoleName)) // exception for user preferences

	// observability
	require.True(t, isAuthorizedOperation(api.QueryTimeSeriesMetricsMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaLimitsMetricsMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.QuotaUsageMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.GetInfoMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.WhoAmIMethodName, auth.ReadOnlyRoleName))

	// realtime
	require.True(t, isAuthorizedOperation(api.ReadMessagesMethodName, auth.ReadOnlyRoleName))

	// search
	require.True(t, isAuthorizedOperation(api.GetIndexMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.ListIndexesMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.SearchGetMethodName, auth.ReadOnlyRoleName))
	require.True(t, isAuthorizedOperation(api.SearchSearch, auth.ReadOnlyRoleName))

	// negative
	require.False(t, isAuthorizedOperation(api.BeginTransactionMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CommitTransactionMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.RollbackTransactionMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.InsertMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateProjectMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateProjectMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateOrUpdateCollectionMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateOrUpdateCollectionsMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteProjectMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DropCollectionMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateCacheMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SetMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.GetSetMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DelMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateBranchMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteBranchMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateAppKeyMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateAppKeyMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteAppKeyMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.RotateAppKeySecretMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateInvitationsMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteInvitationsMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.IndexCollection, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchIndexCollectionMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.ListUsersMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.VerifyInvitationMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateNamespaceMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.ListNamespacesMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteNamespaceMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.CreateGlobalAppKeyMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.UpdateGlobalAppKeyMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteGlobalAppKeyMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.ListGlobalAppKeysMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.RotateGlobalAppKeySecretMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.BillingGetUsageMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.ListInvoicesMethodName, auth.ReadOnlyRoleName))

	// search
	require.False(t, isAuthorizedOperation(api.CreateOrUpdateIndexMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.DeleteIndexMethodName, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchCreateById, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchCreate, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchCreateOrReplace, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchUpdate, auth.ReadOnlyRoleName))
	require.False(t, isAuthorizedOperation(api.SearchDeleteByQuery, auth.ReadOnlyRoleName))
}
