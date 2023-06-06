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

package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
)

func tokenError(userFacingErrorMsg string, err error) error {
	log.Warn().Err(err).Msg(userFacingErrorMsg)
	return errors.Internal(userFacingErrorMsg)
}

func readDate(dateStr string) int64 {
	result, err := date.ToUnixMilli(time.RFC3339, dateStr)
	if err != nil {
		log.Warn().Err(err).Msgf("%s field was not parsed to int64", dateStr)
		result = -1
	}
	return result
}

func createAccessTokenMetadataKey(clientSecret string) string {
	hash := sha256.Sum256([]byte(clientSecret))
	encodedHash := base64.StdEncoding.EncodeToString(hash[:])
	return accessToken + encodedHash
}

func deleteApplicationMetadata(ctx context.Context, namespaceId uint32, appId string, metadataKey string, userStore *metadata.UserSubspace, txMgr *transaction.Manager) error {
	// delete metadata related to this app from user metadata
	tx, err := txMgr.StartTx(ctx)
	if err != nil {
		return tokenError("Failed to delete application metadata", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	err = userStore.DeleteUserMetadata(ctx, tx, namespaceId, metadata.Application, appId, metadataKey)
	if err != nil {
		return tokenError("Failed to delete metadata", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return tokenError("Failed to delete metadata", err)
	}
	return nil
}

func deleteApplication(ctx context.Context, appId string, userStore *metadata.UserSubspace, txMgr *transaction.Manager) error {
	tx, err := txMgr.StartTx(ctx)
	if err != nil {
		return tokenError("Failed to delete application metadata", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	err = userStore.DeleteUser(ctx, tx, defaultNamespaceId, metadata.Application, appId)
	if err != nil {
		return tokenError("Failed to delete metadata", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return tokenError("Failed to delete metadata", err)
	}
	return nil
}

func insertApplicationMetadata(ctx context.Context, metadataKey string, req *api.GetAccessTokenRequest, getAccessTokenResponse *api.GetAccessTokenResponse, userStore *metadata.UserSubspace, txMgr *transaction.Manager) error {
	// cache it
	tx, err := txMgr.StartTx(ctx)
	if err != nil {
		return tokenError("Failed to get access token: reason = could not process cache", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	cacheEntry := &tokenMetadataEntry{
		AccessToken: getAccessTokenResponse.GetAccessToken(),
		ExpireAt:    time.Now().Add(time.Second * time.Duration(getAccessTokenResponse.GetExpiresIn())).Unix(),
	}
	cacheEntryBytes, err := jsoniter.Marshal(cacheEntry)
	if err != nil {
		return tokenError("Failed to get access token: reason = could not process cache", err)
	}

	err = userStore.InsertUserMetadata(ctx, tx, defaultNamespaceId, metadata.Application, req.GetClientId(), metadataKey, cacheEntryBytes)
	if err != nil {
		return tokenError("Failed to get access token: reason = could not process cache", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return tokenError("Failed to get access token: reason = could not process cache", err)
	}
	return nil
}

func GetCurrentSub(ctx context.Context) (string, error) {
	// further filter for this particular user
	token, err := request.GetAccessToken(ctx)
	if err != nil {
		if request.IsLocalRoot(ctx) {
			return "root", nil
		}
		return "", errors.Internal("Failed to retrieve current sub: reason = %s", err.Error())
	}
	return token.Sub, nil
}
