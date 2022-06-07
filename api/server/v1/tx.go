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

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	HeaderPrefix = "Tigris-"

	HeaderTxID     = "Tigris-Tx-Id"
	HeaderTxOrigin = "Tigris-Tx-Origin"

	grpcGatewayPrefix = "grpc-gateway-"
)

func IsTxSupported(ctx context.Context) bool {
	m, _ := grpc.Method(ctx)
	switch m {
	case "Insert", "Replace", "Update", "Delete", "Read",
		"CreateOrUpdateCollection", "DropCollection", "ListCollections":
		return true
	default:
		return false
	}
}

func getHeader(ctx context.Context, header string) string {
	if val := metautils.ExtractIncoming(ctx).Get(header); val != "" {
		return val
	}

	return metautils.ExtractIncoming(ctx).Get(grpcGatewayPrefix + header)
}

func GetTransaction(ctx context.Context, req proto.Message) *TransactionCtx {
	tx := &TransactionCtx{
		Id:     getHeader(ctx, HeaderTxID),
		Origin: getHeader(ctx, HeaderTxOrigin),
	}

	if tx.Id != "" {
		return tx
	}

	return GetTransactionLegacy(req)
}

func GetTransactionLegacy(req proto.Message) *TransactionCtx {
	switch r := req.(type) {
	case *InsertRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *ReplaceRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *UpdateRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *DeleteRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *ReadRequest:
		if r.GetOptions() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	case *CreateOrUpdateCollectionRequest:
		if r.GetOptions() == nil || r.GetOptions().GetTxCtx() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	case *DropCollectionRequest:
		if r.GetOptions() == nil || r.GetOptions().GetTxCtx() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	case *ListCollectionsRequest:
		if r.GetOptions() == nil || r.GetOptions().GetTxCtx() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	}
	return nil
}
