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

package v1

import (
	"context"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
)

type UserMetadataProvider interface {
	GetUserMetadata(ctx context.Context, req *api.GetUserMetadataRequest) (*api.GetUserMetadataResponse, error)
	InsertUserMetadata(ctx context.Context, req *api.InsertUserMetadataRequest) (*api.InsertUserMetadataResponse, error)
	UpdateUserMetadata(ctx context.Context, req *api.UpdateUserMetadataRequest) (*api.UpdateUserMetadataResponse, error)
}

type DefaultUserMetadataProvider struct {
	userStore *metadata.UserSubspace
	txMgr     *transaction.Manager
	tenantMgr *metadata.TenantManager
}

func (a *DefaultUserMetadataProvider) GetUserMetadata(ctx context.Context, req *api.GetUserMetadataRequest) (*api.GetUserMetadataResponse, error) {
	namespaceId, currentSub, tx, err := metadataPrepareOperation("read", ctx, a.txMgr, a.tenantMgr)
	if err != nil {
		return nil, err
	}

	val, err := a.userStore.GetUserMetadata(ctx, tx, namespaceId, metadata.User, currentSub, req.GetMetadataKey())
	if err != nil {
		if err = tx.Rollback(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to rollback transaction.")
		}
		return nil, errors.Internal("Failed to read user metadata.")
	}

	return &api.GetUserMetadataResponse{
		MetadataKey: req.GetMetadataKey(),
		UserId:      currentSub,
		NamespaceId: namespaceId,
		Value:       val,
	}, nil
}

func (a *DefaultUserMetadataProvider) InsertUserMetadata(ctx context.Context, req *api.InsertUserMetadataRequest) (*api.InsertUserMetadataResponse, error) {
	namespaceId, currentSub, tx, err := metadataPrepareOperation("insert", ctx, a.txMgr, a.tenantMgr)
	if err != nil {
		return nil, err
	}

	err = a.userStore.InsertUserMetadata(ctx, tx, namespaceId, metadata.User, currentSub, req.GetMetadataKey(), req.GetValue())
	if err != nil {
		if err = tx.Rollback(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to rollback transaction.")
		}
		return nil, errors.Internal("Failed to insert user metadata.")
	}
	if err = tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to commit transaction.")
		return nil, errors.Internal("Failed to insert user metadata. reason: transaction was not committed.")
	}
	return &api.InsertUserMetadataResponse{
		MetadataKey: req.GetMetadataKey(),
		UserId:      currentSub,
		NamespaceId: namespaceId,
		Value:       req.GetValue(),
	}, nil
}

func metadataPrepareOperation(operationName string, ctx context.Context, txMgr *transaction.Manager, tenantMgr *metadata.TenantManager) (uint32, string, transaction.Tx, error) {
	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		return 0, "", nil, errors.Internal("Failed to %s user metadata. reason: failed to read user namespace.", operationName)
	}

	currentSub, err := getCurrentSub(ctx)
	if err != nil {
		return 0, "", nil, errors.Internal("Failed to %s user metadata. reason: failed to read user.", operationName)
	}

	tenant, err := tenantMgr.GetTenant(ctx, namespace)
	if err != nil {
		return 0, "", nil, errors.Internal("Failed to %s user metadata. reason: failed to read namespace id.", operationName)
	}

	tx, err := txMgr.StartTx(ctx)
	if err != nil {
		return 0, "", nil, errors.Internal("Failed to %s user metadata. reason: failed to create internal transaction.", operationName)
	}
	return tenant.GetNamespace().Id(), currentSub, tx, nil
}

func (a *DefaultUserMetadataProvider) UpdateUserMetadata(ctx context.Context, req *api.UpdateUserMetadataRequest) (*api.UpdateUserMetadataResponse, error) {
	namespaceId, currentSub, tx, err := metadataPrepareOperation("update", ctx, a.txMgr, a.tenantMgr)
	if err != nil {
		return nil, err
	}

	err = a.userStore.UpdateUserMetadata(ctx, tx, namespaceId, metadata.User, currentSub, req.GetMetadataKey(), req.GetValue())
	if err != nil {
		if err = tx.Rollback(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to rollback transaction.")
		}
		return nil, errors.Internal("Failed to update user metadata.")
	}

	if err = tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to commit transaction.")
		return nil, errors.Internal("Failed to insert user metadata. reason: transaction was not committed.")
	}

	return &api.UpdateUserMetadataResponse{
		MetadataKey: req.GetMetadataKey(),
		UserId:      currentSub,
		NamespaceId: namespaceId,
		Value:       req.GetValue(),
	}, nil
}
