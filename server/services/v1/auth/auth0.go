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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/auth0/go-auth0/management"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	"golang.org/x/net/context/ctxhttp"
)

type auth0 struct {
	AuthConfig config.AuthConfig
	Management *management.Management
	userStore  *metadata.UserSubspace
	txMgr      *transaction.Manager
}

func (a *auth0) managementToTigrisErrorCode(err error) api.Code {
	managementError, ok := err.(management.Error)
	if !ok {
		return api.Code_INTERNAL
	}
	return api.FromHttpCode(managementError.Status())
}

func (a *auth0) GetAccessToken(ctx context.Context, req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error) {
	switch req.GrantType {
	case api.GrantType_REFRESH_TOKEN:
		return getAccessTokenUsingRefreshToken(ctx, req, a)
	case api.GrantType_CLIENT_CREDENTIALS:
		return getAccessTokenUsingClientCredentials(ctx, req, a)
	}
	return nil, errors.InvalidArgument("Failed to GetAccessToken: reason = unsupported grant_type, it has to be one of [refresh_token, client_credentials]")
}

func (a *auth0) CreateAppKey(ctx context.Context, req *api.CreateAppKeyRequest) (*api.CreateAppKeyResponse, error) {
	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to list applications: reason = %s", err.Error())
	}
	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to list applications: reason = %s", err.Error())
	}

	nonInteractiveApp := "non_interactive"
	m := map[string]string{}
	m[createdAt] = time.Now().Format(time.RFC3339)
	m[createdBy] = currentSub
	m[tigrisNamespace] = currentNamespace
	if req.GetProject() != "" {
		m[tigrisProject] = req.GetProject()
	}
	c := &management.Client{
		Name:           &req.Name,
		Description:    &req.Description,
		AppType:        &nonInteractiveApp,
		ClientMetadata: m,
	}

	err = a.Management.Client.Create(c)
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to create application: reason = %s", err.Error())
	}

	// grant this client to access current service as audience
	grant := &management.ClientGrant{
		ClientID: c.ClientID,
		Audience: &a.AuthConfig.Audience,
		Scope:    []interface{}{},
	}

	err = a.Management.ClientGrant.Create(grant)
	if err != nil {
		log.Warn().Str("app_id", c.GetClientID()).Msg("Failed to create application grant")
		// delete this app and clean up
		err := a.Management.Client.Delete(c.GetClientID())
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to cleanup half-created app with clientId=%s", c.GetClientID())
		}
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to create application grant: reason = %s", err.Error())
	}

	createdApp := &api.AppKey{
		Name:        c.GetName(),
		Description: c.GetDescription(),
		Id:          c.GetClientID(),
		Secret:      c.GetClientSecret(),
		CreatedBy:   c.GetClientMetadata()[createdBy],
		CreatedAt:   readDate(c.GetClientMetadata()[createdAt]),
		Project:     req.GetProject(),
	}
	return &api.CreateAppKeyResponse{
		CreatedAppKey: createdApp,
	}, nil
}

func (a *auth0) DeleteAppKey(ctx context.Context, req *api.DeleteAppKeyRequest) (*api.DeleteAppKeyResponse, error) {
	_, _, err := validateOwnership(ctx, "delete_application", req.GetId(), a)
	if err != nil {
		return nil, err
	}

	// remove it from metadata cache
	err = deleteApplication(ctx, req.GetId(), a)
	if err != nil {
		return nil, err
	}

	// remove it from auth0
	err = a.Management.Client.Delete(req.GetId())
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to cleanup half-created app with clientId=%s", req.GetId())
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to delete application: reason = %s", err.Error())
	}

	return &api.DeleteAppKeyResponse{
		Deleted: true,
	}, nil
}

func (a *auth0) UpdateAppKey(ctx context.Context, req *api.UpdateAppKeyRequest) (*api.UpdateAppKeyResponse, error) {
	client, currentSub, err := validateOwnership(ctx, "rotate_app_secret", req.GetId(), a)
	if err != nil {
		return nil, err
	}

	m := client.GetClientMetadata()
	m[updatedBy] = currentSub
	m[updatedAt] = time.Now().Format(time.RFC3339)
	appToUpdate := &management.Client{
		Name:           &req.Name,
		Description:    &req.Description,
		ClientMetadata: m,
	}

	err = a.Management.Client.Update(req.GetId(), appToUpdate)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to update app clientId=%s", req.GetId())
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to update application: reason = %s", err.Error())
	}

	client, err = a.Management.Client.Read(req.GetId())
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to update application: reason = %s", err.Error())
	}
	return &api.UpdateAppKeyResponse{
		UpdatedAppKey: &api.AppKey{
			Id:          client.GetClientID(),
			Name:        client.GetName(),
			Description: client.GetDescription(),
			Secret:      client.GetClientSecret(),
			CreatedAt:   readDate(client.GetClientMetadata()[createdAt]),
			CreatedBy:   client.GetClientMetadata()[createdBy],
			UpdatedAt:   readDate(client.GetClientMetadata()[updatedAt]),
			UpdatedBy:   client.GetClientMetadata()[updatedBy],
		},
	}, nil
}

func (a *auth0) RotateAppKey(ctx context.Context, req *api.RotateAppKeyRequest) (*api.RotateAppKeyResponse, error) {
	_, _, err := validateOwnership(ctx, "rotate_app_secret", req.GetId(), a)
	if err != nil {
		return nil, err
	}

	// remove it from metadata cache
	err = deleteApplication(ctx, req.GetId(), a)
	if err != nil {
		return nil, err
	}

	updatedApp, err := a.Management.Client.RotateSecret(req.GetId())
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to rotate application secret: reason = %s", err.Error())
	}

	return &api.RotateAppKeyResponse{
		AppKey: &api.AppKey{
			Id:          updatedApp.GetClientID(),
			Name:        updatedApp.GetName(),
			Description: updatedApp.GetDescription(),
			Secret:      updatedApp.GetClientSecret(),
			CreatedAt:   readDate(updatedApp.GetClientMetadata()[createdAt]),
			CreatedBy:   updatedApp.GetClientMetadata()[createdBy],
		},
	}, nil
}

func (a *auth0) ListAppKeys(ctx context.Context, req *api.ListAppKeysRequest) (*api.ListAppKeysResponse, error) {
	appList, err := a.Management.Client.List(
		management.IncludeFields("client_id", "client_metadata", "client_secret", "description", "name"),
		management.Page(0),
		management.PerPage(perPage),
		management.IncludeTotals(true),
	)
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to list applications: reason = %s", err.Error())
	}
	total := appList.Total
	totalPages := int(math.Ceil(float64(total) / float64(perPage)))

	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to list applications: reason = %s", err.Error())
	}
	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to list applications: reason = %s", err.Error())
	}

	var apps []*api.AppKey
	for pageCount := 1; pageCount <= totalPages; pageCount++ {
		for _, client := range appList.Clients {
			// filter for this user's apps for this tenant
			if client.GetClientMetadata()[createdBy] == currentSub && client.GetClientMetadata()[tigrisNamespace] == currentNamespace {
				// if project filter is supplied - filter for this project
				// for backward compatibility there will be some apps with project metadata
				// set to csv with multiple project names
				supportedProjects := strings.Split(client.GetClientMetadata()[tigrisProject], ",")
				containsProject := false
				for _, project := range supportedProjects {
					if req.GetProject() == project {
						containsProject = true
					}
				}

				// if project filter is not supplied OR if this application is associated for this project.
				if req.GetProject() == "" || containsProject {
					app := &api.AppKey{
						Name:        client.GetName(),
						Description: client.GetDescription(),
						Id:          client.GetClientID(),
						Secret:      client.GetClientSecret(),
						CreatedAt:   readDate(client.GetClientMetadata()[createdAt]),
						CreatedBy:   client.GetClientMetadata()[createdBy],
						UpdatedAt:   readDate(client.GetClientMetadata()[updatedAt]),
						UpdatedBy:   client.GetClientMetadata()[updatedBy],
						Project:     client.GetClientMetadata()[tigrisProject],
					}
					apps = append(apps, app)
				}
			}
		}

		appList, err = a.Management.Client.List(
			management.IncludeFields("client_id", "client_metadata", "client_secret", "description", "name"),
			management.Page(pageCount),
			management.PerPage(perPage),
			management.IncludeTotals(true),
		)
		if err != nil {
			return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to list applications: reason = %s", err.Error())
		}
	}
	return &api.ListAppKeysResponse{
		AppKeys: apps,
	}, nil
}

func (a *auth0) DeleteAppKeys(ctx context.Context, project string) error {
	listKeysResp, err := a.ListAppKeys(ctx, &api.ListAppKeysRequest{
		Project: project,
	})
	if err != nil {
		return err
	}

	for _, key := range listKeysResp.GetAppKeys() {
		// double check - don't delete historical keys which are global for all projects
		if key.Project == project {
			_, err := a.DeleteAppKey(ctx, &api.DeleteAppKeyRequest{Project: project, Id: key.Id})
			if err != nil {
				log.Warn().Str("keyId", key.Id).Str("project", project).Err(err).Msg("Failed to delete appkey associated with project: %s and key id:")
				return errors.Internal("Failed to delete appKey associated with project.")
			}
		}
	}

	return nil
}

func validateOwnership(ctx context.Context, operationName string, appId string, a *auth0) (*management.Client, string, error) {
	client, err := a.Management.Client.Read(appId)
	if err != nil {
		return nil, "", api.Errorf(a.managementToTigrisErrorCode(err), "Failed to %s: reason = %s", operationName, err.Error())
	}

	// check ownership before rotating
	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		return nil, "", errors.Internal("Failed to %s: reason = %s", operationName, err.Error())
	}
	if client.GetClientMetadata()[createdBy] != currentSub {
		return nil, "", errors.PermissionDenied("Failed to rotate application secret: reason = You cannot rotate secret for application that is not created by you.")
	}
	return client, currentSub, nil
}

func GetCurrentSub(ctx context.Context) (string, error) {
	// further filter for this particular user
	token, err := request.GetAccessToken(ctx)
	if err != nil {
		return "", errors.Internal("Failed to retrieve current sub: reason = %s", err.Error())
	}
	return token.Sub, nil
}

func getAccessTokenUsingRefreshToken(ctx context.Context, req *api.GetAccessTokenRequest, a *auth0) (*api.GetAccessTokenResponse, error) {
	data := url.Values{
		"refresh_token": {req.RefreshToken},
		"client_id":     {a.AuthConfig.ClientId},
		"grant_type":    {refreshToken},
		"scope":         {scope},
	}
	resp, err := ctxhttp.PostForm(ctx, &http.Client{}, a.AuthConfig.ExternalTokenURL, data)
	if err != nil {
		return nil, errors.Internal("Failed to get access token: reason = %s", err.Error())
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Internal("Failed to get access token: reason = %s", err.Error())
	}
	bodyStr := string(body)
	if resp.StatusCode == http.StatusOK {
		getAccessTokenResponse := api.GetAccessTokenResponse{}

		err = jsoniter.Unmarshal([]byte(bodyStr), &getAccessTokenResponse)
		if err != nil {
			return nil, errors.Internal("Failed to parse external response: reason = %s", err.Error())
		}
		return &getAccessTokenResponse, nil
	}
	log.Error().Msgf("auth0 response status code=%d", resp.StatusCode)
	return nil, errors.Internal("Failed to get access token: reason = %s", bodyStr)
}

type tokenMetadataEntry struct {
	AccessToken string
	ExpireAt    int64 // unix second
}

func getAccessTokenUsingClientCredentials(ctx context.Context, req *api.GetAccessTokenRequest, a *auth0) (*api.GetAccessTokenResponse, error) {
	// lookup the internal namespace
	tx, err := a.txMgr.StartTx(ctx)
	if err != nil {
		return nil, tokenError("Failed to get access token: reason = could not start tx for internal lookup", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	metadataKey := createAccessTokenMetadataKey(req.GetClientSecret())
	cachedToken, err := a.userStore.GetUserMetadata(ctx, tx, defaultNamespaceId, metadata.Application, req.GetClientId(), metadataKey)
	if err != nil && err != errors.ErrNotFound {
		return nil, tokenError("Failed to get access token: reason = could not process cache", err)
	}

	if cachedToken != nil {
		tokenMetadataEntry := &tokenMetadataEntry{}
		err := jsoniter.Unmarshal(cachedToken, tokenMetadataEntry)
		if err != nil {
			return nil, tokenError("Failed to get access token: reason = could not internally lookup", err)
		}
		// invalidate cache before 10min of expiry
		if tokenMetadataEntry.ExpireAt > time.Now().Unix()+600 {
			return &api.GetAccessTokenResponse{
				AccessToken: tokenMetadataEntry.AccessToken,
				ExpiresIn:   int32(tokenMetadataEntry.ExpireAt - time.Now().Unix()),
			}, nil
		} else {
			// expired entry, delete it
			err := deleteApplicationMetadata(ctx, defaultNamespaceId, req.GetClientId(), metadataKey, a)
			if err != nil {
				return nil, err
			}
		}
	}

	payload := map[string]string{}
	payload["client_id"] = req.ClientId
	payload["client_secret"] = req.ClientSecret
	payload["audience"] = a.AuthConfig.Audience
	payload["grant_type"] = clientCredentials
	jsonPayload, err := jsoniter.Marshal(payload)
	if err != nil {
		return nil, tokenError("Failed to get access token: reason = failed to create external request payload", err)
	}

	resp, err := ctxhttp.Post(ctx, &http.Client{}, a.AuthConfig.ExternalTokenURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, tokenError("Failed to get access token: reason = failed to make external request", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, tokenError("Failed to get access token: reason = failed to read external response", err)
	}
	bodyStr := string(body)
	if resp.StatusCode == http.StatusOK {
		getAccessTokenResponse := api.GetAccessTokenResponse{}

		err = jsoniter.Unmarshal([]byte(bodyStr), &getAccessTokenResponse)
		if err != nil {
			return nil, tokenError("Failed to parse external response: reason = failed to unmarshal JSON", err)
		}

		// cache it
		err = insertApplicationMetadata(ctx, metadataKey, req, &getAccessTokenResponse, a)
		if err != nil {
			return nil, err
		}

		return &getAccessTokenResponse, nil
	}
	log.Error().Msgf("auth0 response status code=%d", resp.StatusCode)
	return nil, errors.Internal("Failed to get access token: reason = %s", bodyStr)
}

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

func deleteApplicationMetadata(ctx context.Context, namespaceId uint32, appId string, metadataKey string, a *auth0) error {
	// delete metadata related to this app from user metadata
	tx, err := a.txMgr.StartTx(ctx)
	if err != nil {
		return tokenError("Failed to delete application metadata", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	err = a.userStore.DeleteUserMetadata(ctx, tx, namespaceId, metadata.Application, appId, metadataKey)
	if err != nil {
		return tokenError("Failed to delete metadata", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return tokenError("Failed to delete metadata", err)
	}
	return nil
}

func deleteApplication(ctx context.Context, appId string, a *auth0) error {
	tx, err := a.txMgr.StartTx(ctx)
	if err != nil {
		return tokenError("Failed to delete application metadata", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	err = a.userStore.DeleteUser(ctx, tx, defaultNamespaceId, metadata.Application, appId)
	if err != nil {
		return tokenError("Failed to delete metadata", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return tokenError("Failed to delete metadata", err)
	}
	return nil
}

func insertApplicationMetadata(ctx context.Context, metadataKey string, req *api.GetAccessTokenRequest, getAccessTokenResponse *api.GetAccessTokenResponse, a *auth0) error {
	// cache it
	tx, err := a.txMgr.StartTx(ctx)
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
	cacheEntryBytes, err := jsoniter.Marshal(&cacheEntry)
	if err != nil {
		return tokenError("Failed to get access token: reason = could not process cache", err)
	}

	err = a.userStore.InsertUserMetadata(ctx, tx, defaultNamespaceId, metadata.Application, req.GetClientId(), metadataKey, cacheEntryBytes)
	if err != nil {
		return tokenError("Failed to get access token: reason = could not process cache", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return tokenError("Failed to get access token: reason = could not process cache", err)
	}
	return nil
}
