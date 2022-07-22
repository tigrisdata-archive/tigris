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

var (
	methodPrefix             = "/tigrisdata.v1.Tigris/"
	insert                   = methodPrefix + "Insert"
	replace                  = methodPrefix + "Replace"
	update                   = methodPrefix + "Update"
	delete                   = methodPrefix + "Delete"
	read                     = methodPrefix + "Read"
	commitTransaction        = methodPrefix + "CommitTransaction"
	rollbackTransaction      = methodPrefix + "RollbackTransaction"
	dropCollection           = methodPrefix + "DropCollection"
	listCollection           = methodPrefix + "ListCollections"
	createOrUpdateCollection = methodPrefix + "CreateOrUpdateCollection"
)

func IsTxSupported(ctx context.Context) bool {
	m, _ := grpc.Method(ctx)
	switch m {
	case insert, replace, update, delete, read, commitTransaction, rollbackTransaction,
		dropCollection, listCollection, createOrUpdateCollection:
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
