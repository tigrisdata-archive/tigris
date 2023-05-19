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
	"github.com/tigrisdata/tigris/server/types"
	"google.golang.org/grpc"
)

const (
	Component = "component"
	Authz     = "authz"
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

		// search
		api.GetIndexMethodName,
		api.ListIndexesMethodName,
		api.SearchGetMethodName,
		api.SearchSearch,
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

		// search
		api.CreateOrUpdateIndexMethodName,
		api.GetIndexMethodName,
		api.DeleteIndexMethodName,
		api.ListIndexesMethodName,
		api.SearchGetMethodName,
		api.SearchCreateById,
		api.SearchCreate,
		api.SearchCreateOrReplace,
		api.SearchUpdate,
		api.SearchDeleteByQuery,
		api.SearchSearch,
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

		// search
		api.CreateOrUpdateIndexMethodName,
		api.GetIndexMethodName,
		api.DeleteIndexMethodName,
		api.ListIndexesMethodName,
		api.SearchGetMethodName,
		api.SearchCreateById,
		api.SearchCreate,
		api.SearchCreateOrReplace,
		api.SearchUpdate,
		api.SearchDeleteByQuery,
		api.SearchSearch,
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

		// search
		api.CreateOrUpdateIndexMethodName,
		api.GetIndexMethodName,
		api.DeleteIndexMethodName,
		api.ListIndexesMethodName,
		api.SearchGetMethodName,
		api.SearchCreateById,
		api.SearchCreate,
		api.SearchCreateOrReplace,
		api.SearchUpdate,
		api.SearchDeleteByQuery,
		api.SearchSearch,
	)
)

func authzUnaryServerInterceptor() func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if config.DefaultConfig.Auth.Authz.Enabled {
			err := authorize(ctx)
			if err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func authzStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := authorize(stream.Context())
		if err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func authorize(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			if config.DefaultConfig.Auth.Authz.LogOnly {
				err = nil
			}
		}
	}()

	reqMetadata, err := request.GetRequestMetadataFromContext(ctx)
	if err != nil {
		return errors.PermissionDenied("Couldn't read the requestMetadata, reason: %s", err.Error())
	}

	if BypassAuthForTheseMethods.Contains(reqMetadata.GetFullMethod()) {
		return nil
	}

	accessToken, err := request.GetAccessToken(ctx)
	if err != nil {
		return errors.PermissionDenied("Couldn't read the accessToken, reason: %s", err.Error())
	}
	role := getRole(reqMetadata)
	if role == "" {
		log.Warn().
			Str(Component, Authz).
			Str("sub", accessToken.Sub).
			Msg("Empty role allowed for transition purpose")
		return nil
	}
	var authorizationErr error
	if !isAuthorizedProject(reqMetadata, accessToken) {
		authorizationErr = errors.PermissionDenied("You are not allowed to perform operation: %s", reqMetadata.GetFullMethod())
	}
	if err == nil && !isAuthorizedOperation(reqMetadata.GetFullMethod(), role) {
		authorizationErr = errors.PermissionDenied("You are not allowed to perform operation: %s", reqMetadata.GetFullMethod())
	}

	if authorizationErr != nil {
		log.Warn().
			Err(authorizationErr).
			Str(Component, Authz).
			Bool("log_only", config.DefaultConfig.Auth.Authz.LogOnly).
			Str("sub", accessToken.Sub).
			Str("role", role).
			Str("operation", reqMetadata.GetFullMethod()).
			Str("project", reqMetadata.GetProject()).
			Msg("Authorization failed")
		return authorizationErr
	}
	return nil
}

func isAuthorizedProject(reqMetadata *request.Metadata, accessToken *types.AccessToken) bool {
	if accessToken.Project != "" && reqMetadata.GetProject() != accessToken.Project {
		return false
	}
	return true
}

func isAuthorizedOperation(method string, role string) bool {
	if methods := getMethodsForRole(role); methods != nil {
		return methods.Contains(method)
	}
	return false
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
	if reqMetadata == nil || reqMetadata.GetRole() == "" {
		return ""
	}
	return reqMetadata.GetRole()
}

func isAdminNamespace(incomingNamespace string) bool {
	return adminNamespaces.Contains(incomingNamespace)
}
