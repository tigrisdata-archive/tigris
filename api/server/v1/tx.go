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

package api

import (
	"context"

	"google.golang.org/grpc"
)

const (
	apiMethodPrefix           = "/tigrisdata.v1.Tigris/"
	authMethodPrefix          = "/tigrisdata.auth.v1.Auth/"
	billingMethodPrefix       = "/tigrisdata.billing.v1.Billing/"
	cacheMethodPrefix         = "/tigrisdata.cache.v1.Cache/"
	ManagementMethodPrefix    = "/tigrisdata.management.v1.Management/"
	ObservabilityMethodPrefix = "/tigrisdata.observability.v1.Observability/"
	realtimeMethodPrefix      = "/tigrisdata.realtime.v1.Realtime/"
	searchMethodPrefix        = "/tigrisdata.search.v1.Search/"

	BeginTransactionMethodName    = apiMethodPrefix + "BeginTransaction"
	CommitTransactionMethodName   = apiMethodPrefix + "CommitTransaction"
	RollbackTransactionMethodName = apiMethodPrefix + "RollbackTransaction"

	InsertMethodName  = apiMethodPrefix + "Insert"
	ReplaceMethodName = apiMethodPrefix + "Replace"
	DeleteMethodName  = apiMethodPrefix + "Delete"
	UpdateMethodName  = apiMethodPrefix + "Update"
	ReadMethodName    = apiMethodPrefix + "Read"
	CountMethodName   = apiMethodPrefix + "Count"

	BuildCollectionIndexMethodName = apiMethodPrefix + "BuildCollectionIndex"
	ExplainMethodName              = apiMethodPrefix + "Explain"

	SearchMethodName = apiMethodPrefix + "Search"
	ImportMethodName = apiMethodPrefix + "Import"

	IndexCollection                 = apiMethodPrefix + "IndexCollection"
	SearchIndexCollectionMethodName = apiMethodPrefix + "BuildSearchIndex"

	CreateOrUpdateCollectionMethodName  = apiMethodPrefix + "CreateOrUpdateCollection"
	CreateOrUpdateCollectionsMethodName = apiMethodPrefix + "CreateOrUpdateCollections"

	DropCollectionMethodName = apiMethodPrefix + "DropCollection"

	ListProjectsMethodName    = apiMethodPrefix + "ListProjects"
	ListCollectionsMethodName = apiMethodPrefix + "ListCollections"
	CreateProjectMethodName   = apiMethodPrefix + "CreateProject"
	UpdateProjectMethodName   = apiMethodPrefix + "UpdateProject"

	DeleteProjectMethodName      = apiMethodPrefix + "DeleteProject"
	DescribeDatabaseMethodName   = apiMethodPrefix + "DescribeDatabase"
	DescribeCollectionMethodName = apiMethodPrefix + "DescribeCollection"

	CreateBranchMethodName = apiMethodPrefix + "CreateBranch"
	DeleteBranchMethodName = apiMethodPrefix + "DeleteBranch"
	ListBranchesMethodName = apiMethodPrefix + "ListBranches"

	CreateAppKeyMethodName       = apiMethodPrefix + "CreateAppKey"
	UpdateAppKeyMethodName       = apiMethodPrefix + "UpdateAppKey"
	DeleteAppKeyMethodName       = apiMethodPrefix + "DeleteAppKey"
	ListAppKeysMethodName        = apiMethodPrefix + "ListAppKeys"
	RotateAppKeySecretMethodName = apiMethodPrefix + "RotateAppKeySecret"

	CreateGlobalAppKeyMethodName       = apiMethodPrefix + "CreateGlobalAppKey"
	UpdateGlobalAppKeyMethodName       = apiMethodPrefix + "UpdateGlobalAppKey"
	DeleteGlobalAppKeyMethodName       = apiMethodPrefix + "DeleteGlobalAppKey"
	ListGlobalAppKeysMethodName        = apiMethodPrefix + "ListGlobalAppKeys"
	RotateGlobalAppKeySecretMethodName = apiMethodPrefix + "RotateGlobalAppKeySecret"

	// Auth.
	GetAccessTokenMethodName    = authMethodPrefix + "GetAccessToken"
	CreateInvitationsMethodName = authMethodPrefix + "CreateInvitations"
	DeleteInvitationsMethodName = authMethodPrefix + "DeleteInvitations"
	ListInvitationsMethodName   = authMethodPrefix + "ListInvitations"
	VerifyInvitationMethodName  = authMethodPrefix + "VerifyInvitation"
	ListUsersMethodName         = authMethodPrefix + "ListUsers"

	// Billing.
	ListInvoicesMethodName    = billingMethodPrefix + "ListInvoices"
	BillingGetUsageMethodName = billingMethodPrefix + "GetUsage"

	// Cache.
	CreateCacheMethodName = cacheMethodPrefix + "CreateCache"
	ListCachesMethodName  = cacheMethodPrefix + "ListCaches"
	DeleteCacheMethodName = cacheMethodPrefix + "DeleteCache"
	SetMethodName         = cacheMethodPrefix + "Set"
	GetSetMethodName      = cacheMethodPrefix + "GetSet"
	GetMethodName         = cacheMethodPrefix + "Get"
	DelMethodName         = cacheMethodPrefix + "Del"
	KeysMethodName        = cacheMethodPrefix + "Keys"

	// Health.
	HealthMethodName = "/HealthAPI/Health"

	// Management.
	CreateNamespaceMethodName         = ManagementMethodPrefix + "CreateNamespace"
	ListNamespacesMethodName          = ManagementMethodPrefix + "ListNamespaces"
	DeleteNamespaceMethodName         = ManagementMethodPrefix + "DeleteNamespace"
	InsertUserMetadataMethodName      = ManagementMethodPrefix + "InsertUserMetadata"
	GetUserMetadataMethodName         = ManagementMethodPrefix + "GetUserMetadata"
	UpdateUserMetadataMethodName      = ManagementMethodPrefix + "UpdateUserMetadata"
	InsertNamespaceMetadataMethodName = ManagementMethodPrefix + "InsertNamespaceMetadata"
	GetNamespaceMetadataMethodName    = ManagementMethodPrefix + "GetNamespaceMetadata"
	UpdateNamespaceMetadataMethodName = ManagementMethodPrefix + "UpdateNamespaceMetadata"

	// Observability.
	QueryTimeSeriesMetricsMethodName = ObservabilityMethodPrefix + "QueryTimeSeriesMetrics"
	QuotaLimitsMetricsMethodName     = ObservabilityMethodPrefix + "QuotaLimits"
	QuotaUsageMethodName             = ObservabilityMethodPrefix + "QuotaUsage"
	GetInfoMethodName                = ObservabilityMethodPrefix + "GetInfo"
	WhoAmIMethodName                 = ObservabilityMethodPrefix + "WhoAmI"

	// Realtime.
	PresenceMethodName          = realtimeMethodPrefix + "Presence"
	GetRTChannelMethodName      = realtimeMethodPrefix + "GetRTChannel"
	GetRTChannelsMethodName     = realtimeMethodPrefix + "GetRTChannels"
	ReadMessagesMethodName      = realtimeMethodPrefix + "ReadMessages"
	MessagesMethodName          = realtimeMethodPrefix + "Messages"
	ListSubscriptionsMethodName = realtimeMethodPrefix + "ListSubscriptions"

	// Search.
	CreateOrUpdateIndexMethodName = searchMethodPrefix + "CreateOrUpdateIndex"
	GetIndexMethodName            = searchMethodPrefix + "GetIndex"
	DeleteIndexMethodName         = searchMethodPrefix + "DeleteIndex"
	ListIndexesMethodName         = searchMethodPrefix + "ListIndexes"
	SearchGetMethodName           = searchMethodPrefix + "Get"
	SearchCreateById              = searchMethodPrefix + "CreateById"
	SearchCreate                  = searchMethodPrefix + "Create"
	SearchCreateOrReplace         = searchMethodPrefix + "CreateOrReplace"
	SearchUpdate                  = searchMethodPrefix + "Update"
	SearchDelete                  = searchMethodPrefix + "Delete"
	SearchDeleteByQuery           = searchMethodPrefix + "DeleteByQuery"
	SearchSearch                  = searchMethodPrefix + "Search"
)

func IsTxSupported(ctx context.Context) bool {
	m, _ := grpc.Method(ctx)
	switch m {
	case InsertMethodName, ReplaceMethodName, UpdateMethodName, DeleteMethodName, ReadMethodName,
		CommitTransactionMethodName, RollbackTransactionMethodName,
		DropCollectionMethodName, ListCollectionsMethodName, CreateOrUpdateCollectionMethodName:
		return true
	default:
		return false
	}
}

func GetTransaction(ctx context.Context) *TransactionCtx {
	origin := GetHeader(ctx, HeaderTxOrigin)
	if len(origin) == 0 {
		return nil
	}

	return &TransactionCtx{
		Id:     GetHeader(ctx, HeaderTxID),
		Origin: origin,
	}
}
