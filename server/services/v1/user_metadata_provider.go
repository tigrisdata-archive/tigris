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
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/auth"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
)

const (
	UserMetadataType = "user"
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
	namespaceId, currentSub, tx, err := metadataPrepareOperation(ctx, UserMetadataType, "read", a.txMgr, a.tenantMgr)
	if err != nil {
		log.Err(err).Msg("Failed to get user metadata")
		return nil, err
	}

	val, err := a.userStore.GetUserMetadata(ctx, tx, namespaceId, metadata.User, currentSub, req.GetMetadataKey())
	if err != nil {
		log.Err(err).Msg("Failed to get user metadata")
		ulog.E(tx.Rollback(ctx))

		if err != errors.ErrNotFound {
			log.Err(err).Msg("Failed to get user metadata")
			return nil, errors.Internal("Failed to read user metadata.")
		}
	}

	return &api.GetUserMetadataResponse{
		MetadataKey: req.GetMetadataKey(),
		UserId:      currentSub,
		NamespaceId: namespaceId,
		Value:       val,
	}, nil
}

func (a *DefaultUserMetadataProvider) InsertUserMetadata(ctx context.Context, req *api.InsertUserMetadataRequest) (*api.InsertUserMetadataResponse, error) {
	namespaceId, currentSub, tx, err := metadataPrepareOperation(ctx, UserMetadataType, "insert", a.txMgr, a.tenantMgr)
	if err != nil {
		log.Err(err).Msg("Failed to insert user metadata")
		return nil, err
	}

	err = a.userStore.InsertUserMetadata(ctx, tx, namespaceId, metadata.User, currentSub, req.GetMetadataKey(), req.GetValue())
	if err != nil {
		log.Err(err).Msg("Failed to insert user metadata")
		if err1 := tx.Rollback(ctx); err1 != nil {
			log.Error().Err(err1).Msg("Failed to rollback transaction.")
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

func metadataPrepareOperation(ctx context.Context, metadataType string, operationName string, txMgr *transaction.Manager, tenantMgr *metadata.TenantManager) (uint32, string, transaction.Tx, error) {
	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msgf("Failed to %s %s metadata. reason: failed to read user namespace.", operationName, metadataType)
		return 0, "", nil, errors.Internal("Failed to %s %s metadata. reason: failed to read user namespace.", operationName, metadataType)
	}

	currentSub, err := auth.GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msgf("Failed to %s %s metadata. reason: failed to read current user.", operationName, metadataType)
		return 0, "", nil, errors.Internal("Failed to %s %s metadata. reason: failed to current user.", operationName, metadataType)
	}

	tenant, err := tenantMgr.GetTenant(ctx, namespace)
	if err != nil {
		log.Err(err).Msgf("Failed to %s %s metadata. reason: failed to read namespace id.", operationName, metadataType)
		return 0, "", nil, errors.Internal("Failed to %s %s metadata. reason: failed to read namespace id.", operationName, metadataType)
	}

	tx, err := txMgr.StartTx(ctx)
	if err != nil {
		log.Err(err).Msgf("Failed to %s %s metadata. reason: failed to create internal transaction.", operationName, metadataType)
		return 0, "", nil, errors.Internal("Failed to %s %s metadata. reason: failed to create internal transaction.", operationName, metadataType)
	}
	return tenant.GetNamespace().Id(), currentSub, tx, nil
}

func (a *DefaultUserMetadataProvider) UpdateUserMetadata(ctx context.Context, req *api.UpdateUserMetadataRequest) (*api.UpdateUserMetadataResponse, error) {
	namespaceId, currentSub, tx, err := metadataPrepareOperation(ctx, UserMetadataType, "update", a.txMgr, a.tenantMgr)
	if err != nil {
		log.Err(err).Msg("Failed to update user metadata")
		return nil, err
	}

	err = a.userStore.UpdateUserMetadata(ctx, tx, namespaceId, metadata.User, currentSub, req.GetMetadataKey(), req.GetValue())
	if err != nil {
		log.Err(err).Msg("Failed to update user metadata")
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
