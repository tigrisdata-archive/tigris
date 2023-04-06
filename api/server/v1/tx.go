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
	HealthMethodName = "/HealthAPI/Health"

	apiMethodPrefix = "/tigrisdata.v1.Tigris/"

	InsertMethodName  = apiMethodPrefix + "Insert"
	ReplaceMethodName = apiMethodPrefix + "Replace"
	UpdateMethodName  = apiMethodPrefix + "Update"
	DeleteMethodName  = apiMethodPrefix + "Delete"
	ReadMethodName    = apiMethodPrefix + "Read"

	SearchMethodName = apiMethodPrefix + "Search"

	SubscribeMethodName = apiMethodPrefix + "Subscribe"

	EventsMethodName = apiMethodPrefix + "Events"

	CommitTransactionMethodName   = apiMethodPrefix + "CommitTransaction"
	RollbackTransactionMethodName = apiMethodPrefix + "RollbackTransaction"

	CreateOrUpdateCollectionMethodName = apiMethodPrefix + "CreateOrUpdateCollection"
	DropCollectionMethodName           = apiMethodPrefix + "DropCollection"

	DropDatabaseMethodName = apiMethodPrefix + "DropDatabase"

	ListDatabasesMethodName   = apiMethodPrefix + "ListDatabases"
	ListCollectionsMethodName = apiMethodPrefix + "ListCollections"

	DescribeDatabaseMethodName   = apiMethodPrefix + "DescribeDatabase"
	DescribeCollectionMethodName = apiMethodPrefix + "DescribeCollection"

	ObservabilityMethodPrefix = "/tigrisdata.observability.v1.Observability/"
	ManagementMethodPrefix    = "/tigrisdata.management.v1.Management/"
	CreateNamespaceMethodName = ManagementMethodPrefix + "CreateNamespace"
	ListNamespaceMethodName   = ManagementMethodPrefix + "ListNamespaces"
	DeleteNamespaceMethodName = ManagementMethodPrefix + "DeleteNamespace"

	AuthMethodPrefix         = "/tigrisdata.auth.v1.Auth/"
	GetAccessTokenMethodName = AuthMethodPrefix + "GetAccessToken"
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
