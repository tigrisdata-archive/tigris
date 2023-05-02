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
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util/log"
)

func TestAuthzOwnerRole(t *testing.T) {
	require.True(t, isAuthorized(api.BeginTransactionMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.CommitTransactionMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.RollbackTransactionMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.InsertMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ReplaceMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DeleteMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.UpdateMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ReadMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.CountMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.BuildCollectionIndexMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ExplainMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.SearchMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ImportMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.CreateOrUpdateCollectionMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.CreateOrUpdateCollectionsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DropCollectionMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ListProjectsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ListCollectionsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.CreateProjectMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DeleteProjectMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DescribeDatabaseMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DescribeCollectionMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.CreateBranchMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DeleteBranchMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ListBranchesMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.CreateAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.UpdateAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DeleteAppKeyMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ListAppKeysMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.RotateAppKeySecretMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.IndexCollection, editorRoleName))
	require.True(t, isAuthorized(api.SearchIndexCollectionMethodName, editorRoleName))

	// auth
	require.True(t, isAuthorized(api.GetAccessTokenMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.CreateInvitationsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DeleteInvitationsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ListInvitationsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ListUsersMethodName, ownerRoleName))

	// billing
	require.True(t, isAuthorized(api.ListInvoicesMethodName, ownerRoleName))

	// cache
	require.True(t, isAuthorized(api.CreateCacheMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ListCachesMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DeleteCacheMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.SetMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.GetSetMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.GetMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DelMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.KeysMethodName, ownerRoleName))

	// health
	require.True(t, isAuthorized(api.HealthMethodName, ownerRoleName))

	// management
	require.True(t, isAuthorized(api.InsertUserMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.GetUserMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.UpdateUserMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.InsertNamespaceMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.GetNamespaceMetadataMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.UpdateNamespaceMetadataMethodName, ownerRoleName))

	// observability
	require.True(t, isAuthorized(api.QueryTimeSeriesMetricsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.QuotaLimitsMetricsMethodName, ownerRoleName))

	// realtime
	require.True(t, isAuthorized(api.PresenceMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.GetRTChannelMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.GetRTChannelsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ReadMessagesMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.MessagesMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.ListSubscriptionsMethodName, ownerRoleName))

	// negative
	require.False(t, isAuthorized(api.VerifyInvitationMethodName, ownerRoleName))
	require.False(t, isAuthorized(api.CreateNamespaceMethodName, ownerRoleName))
	require.False(t, isAuthorized(api.ListNamespacesMethodName, ownerRoleName))
	require.False(t, isAuthorized(api.DeleteNamespaceMethodName, ownerRoleName))
}

func TestAuthzEditorRole(t *testing.T) {
	// db
	require.True(t, isAuthorized(api.BeginTransactionMethodName, editorRoleName))
	require.True(t, isAuthorized(api.CommitTransactionMethodName, editorRoleName))
	require.True(t, isAuthorized(api.RollbackTransactionMethodName, editorRoleName))
	require.True(t, isAuthorized(api.InsertMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ReplaceMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DeleteMethodName, editorRoleName))
	require.True(t, isAuthorized(api.UpdateMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ReadMethodName, editorRoleName))
	require.True(t, isAuthorized(api.CountMethodName, editorRoleName))
	require.True(t, isAuthorized(api.BuildCollectionIndexMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ExplainMethodName, editorRoleName))
	require.True(t, isAuthorized(api.SearchMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ImportMethodName, editorRoleName))
	require.True(t, isAuthorized(api.CreateOrUpdateCollectionMethodName, editorRoleName))
	require.True(t, isAuthorized(api.CreateOrUpdateCollectionsMethodName, ownerRoleName))
	require.True(t, isAuthorized(api.DropCollectionMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ListProjectsMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ListCollectionsMethodName, editorRoleName))
	require.True(t, isAuthorized(api.CreateProjectMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DeleteProjectMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DescribeDatabaseMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DescribeCollectionMethodName, editorRoleName))
	require.True(t, isAuthorized(api.CreateBranchMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DeleteBranchMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ListBranchesMethodName, editorRoleName))
	require.True(t, isAuthorized(api.CreateAppKeyMethodName, editorRoleName))
	require.True(t, isAuthorized(api.UpdateAppKeyMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DeleteAppKeyMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ListAppKeysMethodName, editorRoleName))
	require.True(t, isAuthorized(api.RotateAppKeySecretMethodName, editorRoleName))
	require.True(t, isAuthorized(api.IndexCollection, editorRoleName))
	require.True(t, isAuthorized(api.SearchIndexCollectionMethodName, editorRoleName))

	// auth
	require.True(t, isAuthorized(api.GetAccessTokenMethodName, editorRoleName))
	require.True(t, isAuthorized(api.CreateInvitationsMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DeleteInvitationsMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ListInvitationsMethodName, editorRoleName))

	// cache
	require.True(t, isAuthorized(api.CreateCacheMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ListCachesMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DeleteCacheMethodName, editorRoleName))
	require.True(t, isAuthorized(api.SetMethodName, editorRoleName))
	require.True(t, isAuthorized(api.GetSetMethodName, editorRoleName))
	require.True(t, isAuthorized(api.GetMethodName, editorRoleName))
	require.True(t, isAuthorized(api.DelMethodName, editorRoleName))
	require.True(t, isAuthorized(api.KeysMethodName, editorRoleName))

	// health
	require.True(t, isAuthorized(api.HealthMethodName, editorRoleName))

	// management
	require.True(t, isAuthorized(api.InsertUserMetadataMethodName, editorRoleName))
	require.True(t, isAuthorized(api.GetUserMetadataMethodName, editorRoleName))
	require.True(t, isAuthorized(api.UpdateUserMetadataMethodName, editorRoleName))
	require.True(t, isAuthorized(api.InsertNamespaceMetadataMethodName, editorRoleName))
	require.True(t, isAuthorized(api.GetNamespaceMetadataMethodName, editorRoleName))
	require.True(t, isAuthorized(api.UpdateNamespaceMetadataMethodName, editorRoleName))

	// observability
	require.True(t, isAuthorized(api.QueryTimeSeriesMetricsMethodName, editorRoleName))
	require.True(t, isAuthorized(api.QuotaLimitsMetricsMethodName, editorRoleName))

	// realtime
	require.True(t, isAuthorized(api.PresenceMethodName, editorRoleName))
	require.True(t, isAuthorized(api.GetRTChannelMethodName, editorRoleName))
	require.True(t, isAuthorized(api.GetRTChannelsMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ReadMessagesMethodName, editorRoleName))
	require.True(t, isAuthorized(api.MessagesMethodName, editorRoleName))
	require.True(t, isAuthorized(api.ListSubscriptionsMethodName, editorRoleName))

	// negative
	require.False(t, isAuthorized(api.ListUsersMethodName, editorRoleName))
	require.False(t, isAuthorized(api.VerifyInvitationMethodName, editorRoleName))
	require.False(t, isAuthorized(api.CreateNamespaceMethodName, editorRoleName))
	require.False(t, isAuthorized(api.ListNamespacesMethodName, editorRoleName))
	require.False(t, isAuthorized(api.DeleteNamespaceMethodName, editorRoleName))
}

func TestAuthzReadOnlyRole(t *testing.T) {
	// db
	require.True(t, isAuthorized(api.ReadMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.CountMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.ExplainMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.SearchMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.ListProjectsMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.DescribeDatabaseMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.DescribeCollectionMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.ListBranchesMethodName, readOnlyRoleName))

	// auth
	require.True(t, isAuthorized(api.ListAppKeysMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.GetAccessTokenMethodName, readOnlyRoleName))

	// cache
	require.True(t, isAuthorized(api.ListCachesMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.GetMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.KeysMethodName, readOnlyRoleName))

	// health
	require.True(t, isAuthorized(api.HealthMethodName, readOnlyRoleName))

	// management
	require.True(t, isAuthorized(api.GetUserMetadataMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.InsertUserMetadataMethodName, readOnlyRoleName)) // exception for user preferences
	require.True(t, isAuthorized(api.UpdateUserMetadataMethodName, readOnlyRoleName)) // exception for user preferences

	// observability
	require.True(t, isAuthorized(api.QueryTimeSeriesMetricsMethodName, readOnlyRoleName))
	require.True(t, isAuthorized(api.QuotaLimitsMetricsMethodName, readOnlyRoleName))

	// realtime
	require.True(t, isAuthorized(api.ReadMessagesMethodName, readOnlyRoleName))

	// negative
	require.False(t, isAuthorized(api.BeginTransactionMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CommitTransactionMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.RollbackTransactionMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.InsertMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.UpdateMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.DeleteMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CreateProjectMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CreateOrUpdateCollectionMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CreateOrUpdateCollectionsMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.DeleteProjectMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.DropCollectionMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CreateCacheMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.SetMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.GetSetMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.DelMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CreateBranchMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.DeleteBranchMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CreateAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.UpdateAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.DeleteAppKeyMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.RotateAppKeySecretMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CreateInvitationsMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.DeleteInvitationsMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.IndexCollection, editorRoleName))
	require.False(t, isAuthorized(api.SearchIndexCollectionMethodName, editorRoleName))
	require.False(t, isAuthorized(api.ListUsersMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.VerifyInvitationMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.CreateNamespaceMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.ListNamespacesMethodName, readOnlyRoleName))
	require.False(t, isAuthorized(api.DeleteNamespaceMethodName, readOnlyRoleName))
}

func TestAdminNamespace(t *testing.T) {
	enforcedAuthConfig := config.Config{
		Server: config.ServerConfig{},
		Log: log.LogConfig{
			Level: "error",
		},
		Auth: config.AuthConfig{
			Validators:               []config.ValidatorConfig{},
			PrimaryAudience:          "",
			JWKSCacheTimeout:         0,
			LogOnly:                  false,
			EnableNamespaceIsolation: false,
			AdminNamespaces:          []string{"tigris-admin"},
		},
		FoundationDB: config.FoundationDBConfig{},
	}
	require.False(t, isAdminNamespace("tigris-non-admin", &enforcedAuthConfig))
	require.True(t, isAdminNamespace("tigris-admin", &enforcedAuthConfig))
}
