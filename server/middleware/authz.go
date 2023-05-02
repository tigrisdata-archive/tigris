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
	"fmt"

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

	realOnlyMethods = container.NewHashSet(
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
	clusterAdmin = container.NewHashSet(
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

func authzUnaryServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if config.DefaultConfig.Auth.Authz.Enabled {
			reqMetadata, _ := request.GetRequestMetadataFromContext(ctx)
			role := getRole(reqMetadata)

			// empty role check for transition purpose
			if role != "" {
				authorized := isAuthorized(reqMetadata.GetFullMethod(), role)
				if !authorized {
					return nil, errors.PermissionDenied(fmt.Sprintf("You are not allowed to perform operation: %s", reqMetadata.GetFullMethod()))
				}
			}
			resp, err := handler(ctx, req)
			return resp, err
		}
		return handler(ctx, req)
	}
}

func authzStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		reqMetadata, _ := request.GetRequestMetadataFromContext(stream.Context())
		role := getRole(reqMetadata)
		// empty role check for transition purpose
		if role != "" {
			authorized := isAuthorized(reqMetadata.GetFullMethod(), role)
			if !authorized {
				return errors.PermissionDenied(fmt.Sprintf("You are not allowed to perform operation: %s", reqMetadata.GetFullMethod()))
			}
		}
		return handler(srv, stream)
	}
}

func isAuthorized(methodName string, role string) bool {
	allowed := false
	switch role {
	case ClusterAdminRoleName:
		allowed = clusterAdmin.Contains(methodName)
	case ownerRoleName:
		allowed = ownerMethods.Contains(methodName)
	case editorRoleName:
		allowed = editorMethods.Contains(methodName)
	case readOnlyRoleName:
		allowed = realOnlyMethods.Contains(methodName)
	}
	if !allowed {
		log.Debug().
			Str("methodName", methodName).
			Str("role", role).
			Msg("Authz - not allowed")
	}
	return allowed
}

func getRole(reqMetadata *request.Metadata) string {
	isAdmin := isAdminNamespace(reqMetadata.GetNamespace(), &config.DefaultConfig)
	if isAdmin {
		return ClusterAdminRoleName
	}

	// empty role check for transition purpose
	if reqMetadata == nil && reqMetadata.GetRole() == "" {
		return ""
	}
	return reqMetadata.GetRole()
}

func isAdminNamespace(incomingNamespace string, config *config.Config) bool {
	for _, allowedAdminNamespace := range config.Auth.AdminNamespaces {
		if incomingNamespace == allowedAdminNamespace {
			return true
		}
	}
	return false
}
