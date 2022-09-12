// Copyright 2022 Tigris Data, Inc.
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
	methodPrefix = "/tigrisdata.v1.Tigris/"

	InsertMethodName  = methodPrefix + "Insert"
	ReplaceMethodName = methodPrefix + "Replace"
	UpdateMethodName  = methodPrefix + "Update"
	DeleteMethodName  = methodPrefix + "Delete"
	ReadMethodName    = methodPrefix + "Read"

	SearchMethodName = methodPrefix + "Search"

	SubscribeMethodName = methodPrefix + "Subscribe"

	EventsMethodName = methodPrefix + "Events"

	CommitTransactionMethodName   = methodPrefix + "CommitTransaction"
	RollbackTransactionMethodName = methodPrefix + "RollbackTransaction"

	CreateOrUpdateCollectionMethodName = methodPrefix + "CreateOrUpdateCollection"
	DropCollectionMethodName           = methodPrefix + "DropCollection"

	ListDatabasesMethodName   = methodPrefix + "ListDatabases"
	ListCollectionsMethodName = methodPrefix + "ListCollections"

	DescribeDatabaseMethodName   = methodPrefix + "DescribeDatabase"
	DescribeCollectionMethodName = methodPrefix + "DescribeCollection"
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
