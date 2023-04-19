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

package v1

import (
	"context"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
)

const (
	NamespaceMetadataType = "namespace"
)

type NamespaceMetadataProvider interface {
	GetNamespaceMetadata(ctx context.Context, req *api.GetNamespaceMetadataRequest) (*api.GetNamespaceMetadataResponse, error)
	InsertNamespaceMetadata(ctx context.Context, req *api.InsertNamespaceMetadataRequest) (*api.InsertNamespaceMetadataResponse, error)
	UpdateNamespaceMetadata(ctx context.Context, req *api.UpdateNamespaceMetadataRequest) (*api.UpdateNamespaceMetadataResponse, error)
	DeleteNamespace(ctx context.Context, tx transaction.Tx, namespaceId uint32) error
}

type DefaultNamespaceMetadataProvider struct {
	namespaceStore *metadata.NamespaceSubspace
	txMgr          *transaction.Manager
	tenantMgr      *metadata.TenantManager
}

func (a *DefaultNamespaceMetadataProvider) GetNamespaceMetadata(ctx context.Context, req *api.GetNamespaceMetadataRequest) (*api.GetNamespaceMetadataResponse, error) {
	namespaceId, _, tx, err := metadataPrepareOperation(NamespaceMetadataType, "read", ctx, a.txMgr, a.tenantMgr)
	if err != nil {
		return nil, err
	}

	val, err := a.namespaceStore.GetNamespaceMetadata(ctx, tx, namespaceId, req.GetMetadataKey())
	if err != nil {
		ulog.E(tx.Rollback(ctx))

		if err != errors.ErrNotFound {
			return nil, errors.Internal("Failed to read namespace metadata.")
		}
	}

	return &api.GetNamespaceMetadataResponse{
		MetadataKey: req.GetMetadataKey(),
		NamespaceId: namespaceId,
		Value:       val,
	}, nil
}

func (a *DefaultNamespaceMetadataProvider) DeleteNamespace(ctx context.Context, tx transaction.Tx, namespaceId uint32) error {
	return a.namespaceStore.DeleteNamespace(ctx, tx, namespaceId)
}

func (a *DefaultNamespaceMetadataProvider) InsertNamespaceMetadata(ctx context.Context, req *api.InsertNamespaceMetadataRequest) (*api.InsertNamespaceMetadataResponse, error) {
	namespaceId, _, tx, err := metadataPrepareOperation(NamespaceMetadataType, "insert", ctx, a.txMgr, a.tenantMgr)
	if err != nil {
		return nil, err
	}

	err = a.namespaceStore.InsertNamespaceMetadata(ctx, tx, namespaceId, req.GetMetadataKey(), req.GetValue())
	if err != nil {
		if err = tx.Rollback(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to rollback transaction.")
		}
		return nil, errors.Internal("Failed to insert namespace metadata.")
	}
	if err = tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to commit transaction.")
		return nil, errors.Internal("Failed to insert namespace metadata. reason: transaction was not committed.")
	}
	return &api.InsertNamespaceMetadataResponse{
		MetadataKey: req.GetMetadataKey(),
		NamespaceId: namespaceId,
		Value:       req.GetValue(),
	}, nil
}

func (a *DefaultNamespaceMetadataProvider) UpdateNamespaceMetadata(ctx context.Context, req *api.UpdateNamespaceMetadataRequest) (*api.UpdateNamespaceMetadataResponse, error) {
	namespaceId, _, tx, err := metadataPrepareOperation(NamespaceMetadataType, "update", ctx, a.txMgr, a.tenantMgr)
	if err != nil {
		return nil, err
	}

	err = a.namespaceStore.UpdateNamespaceMetadata(ctx, tx, namespaceId, req.GetMetadataKey(), req.GetValue())
	if err != nil {
		if err = tx.Rollback(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to rollback transaction.")
		}
		return nil, errors.Internal("Failed to update namespace metadata.")
	}

	if err = tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to commit transaction.")
		return nil, errors.Internal("Failed to insert namespace metadata. reason: transaction was not committed.")
	}

	return &api.UpdateNamespaceMetadataResponse{
		MetadataKey: req.GetMetadataKey(),
		NamespaceId: namespaceId,
		Value:       req.GetValue(),
	}, nil
}
