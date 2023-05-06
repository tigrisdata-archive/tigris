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
	"context"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
)

var (
	// role names.
	readOnlyRoleName     = "ro"
	editorRoleName       = "e"
	ownerRoleName        = "o"
	ClusterAdminRoleName = "cluster_admin"

	adminNamespaces = container.NewHashSet(config.DefaultConfig.Auth.AdminNamespaces...)
	readonlyMethods = container.NewHashSet(
		// db
		api.ReadMethodName,
		api.CountMethodName,
		api.ExplainMethodName,
		api.SearchMethodName,
		api.ListProjectsMethodName,
		api.DescribeDatabaseMethodName,
		api.DescribeCollectionMethodName,
		api.ListBranchesMethodName,

		// auth
		api.ListAppKeysMethodName,
		api.GetAccessTokenMethodName,

		// cache
		api.ListCachesMethodName,
		api.GetMethodName,
		api.KeysMethodName,

		// health
		api.HealthMethodName,

		// management
		api.GetUserMetadataMethodName,
		api.InsertUserMetadataMethodName, // exception for user preferences
		api.UpdateUserMetadataMethodName, // exception for user preferences

		// observability
		api.QueryTimeSeriesMetricsMethodName,
		api.QuotaLimitsMetricsMethodName,

		// realtime
		api.ReadMessagesMethodName,
	)

	// editor.
	editorMethods = container.NewHashSet(
		// db
		api.BeginTransactionMethodName,
		api.CommitTransactionMethodName,
		api.RollbackTransactionMethodName,
		api.InsertMethodName,
		api.ReplaceMethodName,
		api.DeleteMethodName,
		api.UpdateMethodName,
		api.ReadMethodName,
		api.CountMethodName,
		api.BuildCollectionIndexMethodName,
		api.ExplainMethodName,
		api.SearchMethodName,
		api.ImportMethodName,
		api.CreateOrUpdateCollectionMethodName,
		api.CreateOrUpdateCollectionsMethodName,
		api.DropCollectionMethodName,
		api.ListProjectsMethodName,
		api.ListCollectionsMethodName,
		api.CreateProjectMethodName,
		api.DeleteProjectMethodName,
		api.DescribeDatabaseMethodName,
		api.DescribeCollectionMethodName,
		api.CreateBranchMethodName,
		api.DeleteBranchMethodName,
		api.ListBranchesMethodName,
		api.CreateAppKeyMethodName,
		api.UpdateAppKeyMethodName,
		api.DeleteAppKeyMethodName,
		api.ListAppKeysMethodName,
		api.RotateAppKeySecretMethodName,
		api.IndexCollection,
		api.SearchIndexCollectionMethodName,

		// auth
		api.GetAccessTokenMethodName,
		api.CreateInvitationsMethodName,
		api.DeleteInvitationsMethodName,
		api.ListInvitationsMethodName,

		// cache
		api.CreateCacheMethodName,
		api.ListCachesMethodName,
		api.DeleteCacheMethodName,
		api.SetMethodName,
		api.GetSetMethodName,
		api.GetMethodName,
		api.DelMethodName,
		api.KeysMethodName,

		// health
		api.HealthMethodName,

		// management
		api.InsertUserMetadataMethodName,
		api.GetUserMetadataMethodName,
		api.UpdateUserMetadataMethodName,
		api.InsertNamespaceMetadataMethodName,
		api.GetNamespaceMetadataMethodName,
		api.UpdateNamespaceMetadataMethodName,

		// observability
		api.QueryTimeSeriesMetricsMethodName,
		api.QuotaLimitsMetricsMethodName,

		// realtime
		api.PresenceMethodName,
		api.GetRTChannelMethodName,
		api.GetRTChannelsMethodName,
		api.ReadMessagesMethodName,
		api.MessagesMethodName,
		api.ListSubscriptionsMethodName,
	)

	ownerMethods = container.NewHashSet(
		// db
		api.BeginTransactionMethodName,
		api.CommitTransactionMethodName,
		api.RollbackTransactionMethodName,
		api.InsertMethodName,
		api.ReplaceMethodName,
		api.DeleteMethodName,
		api.UpdateMethodName,
		api.ReadMethodName,
		api.CountMethodName,
		api.BuildCollectionIndexMethodName,
		api.ExplainMethodName,
		api.SearchMethodName,
		api.ImportMethodName,
		api.CreateOrUpdateCollectionMethodName,
		api.CreateOrUpdateCollectionsMethodName,

		api.DropCollectionMethodName,
		api.ListProjectsMethodName,
		api.ListCollectionsMethodName,
		api.CreateProjectMethodName,
		api.DeleteProjectMethodName,
		api.DescribeDatabaseMethodName,
		api.DescribeCollectionMethodName,
		api.CreateBranchMethodName,
		api.DeleteBranchMethodName,
		api.ListBranchesMethodName,
		api.CreateAppKeyMethodName,
		api.UpdateAppKeyMethodName,
		api.DeleteAppKeyMethodName,
		api.ListAppKeysMethodName,
		api.RotateAppKeySecretMethodName,
		api.CreateGlobalAppKeyMethodName,
		api.UpdateGlobalAppKeyMethodName,
		api.DeleteGlobalAppKeyMethodName,
		api.ListGlobalAppKeysMethodName,
		api.RotateGlobalAppKeySecretMethodName,
		api.IndexCollection,
		api.SearchIndexCollectionMethodName,

		// auth
		api.GetAccessTokenMethodName,
		api.CreateInvitationsMethodName,
		api.DeleteInvitationsMethodName,
		api.ListInvitationsMethodName,
		api.ListUsersMethodName,

		// billing
		api.ListInvoicesMethodName,

		// cache
		api.CreateCacheMethodName,
		api.ListCachesMethodName,
		api.DeleteCacheMethodName,
		api.SetMethodName,
		api.GetSetMethodName,
		api.GetMethodName,
		api.DelMethodName,
		api.KeysMethodName,

		// health
		api.HealthMethodName,

		// management
		api.InsertUserMetadataMethodName,
		api.GetUserMetadataMethodName,
		api.UpdateUserMetadataMethodName,
		api.InsertNamespaceMetadataMethodName,
		api.GetNamespaceMetadataMethodName,
		api.UpdateNamespaceMetadataMethodName,

		// observability
		api.QueryTimeSeriesMetricsMethodName,
		api.QuotaLimitsMetricsMethodName,

		// realtime
		api.PresenceMethodName,
		api.GetRTChannelMethodName,
		api.GetRTChannelsMethodName,
		api.ReadMessagesMethodName,
		api.MessagesMethodName,
		api.ListSubscriptionsMethodName,
	)
	clusterAdminMethods = container.NewHashSet(
		// db
		api.BeginTransactionMethodName,
		api.CommitTransactionMethodName,
		api.RollbackTransactionMethodName,
		api.InsertMethodName,
		api.ReplaceMethodName,
		api.DeleteMethodName,
		api.UpdateMethodName,
		api.ReadMethodName,
		api.CountMethodName,
		api.BuildCollectionIndexMethodName,
		api.ExplainMethodName,
		api.SearchMethodName,
		api.ImportMethodName,
		api.CreateOrUpdateCollectionMethodName,
		api.DropCollectionMethodName,
		api.ListProjectsMethodName,
		api.ListCollectionsMethodName,
		api.CreateProjectMethodName,
		api.DeleteProjectMethodName,
		api.DescribeDatabaseMethodName,
		api.DescribeCollectionMethodName,
		api.CreateBranchMethodName,
		api.DeleteBranchMethodName,
		api.ListBranchesMethodName,
		api.CreateAppKeyMethodName,
		api.UpdateAppKeyMethodName,
		api.DeleteAppKeyMethodName,
		api.ListAppKeysMethodName,
		api.RotateAppKeySecretMethodName,

		// auth
		api.GetAccessTokenMethodName,
		api.CreateInvitationsMethodName,
		api.DeleteInvitationsMethodName,
		api.ListInvitationsMethodName,
		api.VerifyInvitationMethodName,
		api.ListUsersMethodName,

		// billing
		api.ListInvitationsMethodName,

		// cache
		api.CreateCacheMethodName,
		api.ListCachesMethodName,
		api.DeleteCacheMethodName,
		api.SetMethodName,
		api.GetSetMethodName,
		api.GetMethodName,
		api.DelMethodName,
		api.KeysMethodName,

		// health
		api.HealthMethodName,

		// management
		api.InsertUserMetadataMethodName,
		api.GetUserMetadataMethodName,
		api.UpdateUserMetadataMethodName,
		api.InsertNamespaceMetadataMethodName,
		api.GetNamespaceMetadataMethodName,
		api.UpdateNamespaceMetadataMethodName,
		api.ListNamespacesMethodName,
		api.CreateNamespaceMethodName,
		api.DeleteNamespaceMethodName,

		// observability
		api.QueryTimeSeriesMetricsMethodName,
		api.QuotaLimitsMetricsMethodName,

		// realtime
		api.PresenceMethodName,
		api.GetRTChannelMethodName,
		api.GetRTChannelsMethodName,
		api.ReadMessagesMethodName,
		api.MessagesMethodName,
		api.ListSubscriptionsMethodName,
	)
)

func authzUnaryServerInterceptor() func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if config.DefaultConfig.Auth.Authz.Enabled {
			reqMetadata, _ := request.GetRequestMetadataFromContext(ctx)
			role := getRole(reqMetadata)

			// empty role check for transition purpose
			if role != "" {
				if !isAuthorized(reqMetadata.GetFullMethod(), role) {
					return nil, errors.PermissionDenied("You are not allowed to perform operation: %s", reqMetadata.GetFullMethod())
				}
			}
		}
		return handler(ctx, req)
	}
}

func authzStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		reqMetadata, _ := request.GetRequestMetadataFromContext(stream.Context())
		role := getRole(reqMetadata)
		// empty role check for transition purpose
		if role != "" {
			if !isAuthorized(reqMetadata.GetFullMethod(), role) {
				return errors.PermissionDenied("You are not allowed to perform operation: %s", reqMetadata.GetFullMethod())
			}
		}
		return handler(srv, stream)
	}
}

func isAuthorized(methodName string, role string) bool {
	allowed := false
	if methods := getMethodsForRole(role); methods != nil {
		allowed = methods.Contains(methodName)
	}

	if !allowed {
		log.Warn().
			Str("methodName", methodName).
			Str("role", role).
			Msg("Authz - not allowed")
	}
	return allowed
}

func getMethodsForRole(role string) *container.HashSet {
	switch role {
	case ClusterAdminRoleName:
		return &clusterAdminMethods
	case ownerRoleName:
		return &ownerMethods
	case editorRoleName:
		return &editorMethods
	case readOnlyRoleName:
		return &readonlyMethods
	}
	return nil
}

func getRole(reqMetadata *request.Metadata) string {
	if isAdminNamespace(reqMetadata.GetNamespace()) {
		return ClusterAdminRoleName
	}

	// empty role check for transition purpose
	if reqMetadata == nil && reqMetadata.GetRole() == "" {
		return ""
	}
	return reqMetadata.GetRole()
}

func isAdminNamespace(incomingNamespace string) bool {
	return adminNamespaces.Contains(incomingNamespace)
}
