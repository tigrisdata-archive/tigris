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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/auth0/go-auth0/management"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
)

const (
	refreshToken       = "refresh_token"
	accessToken        = "access_token"
	scope              = "offline_access openid"
	createdBy          = "created_by"
	createdAt          = "created_at"
	updatedBy          = "updated_by"
	updatedAt          = "updated_at"
	tigrisNamespace    = "tigris_namespace"
	clientCredentials  = "client_credentials"
	defaultNamespaceId = 1
)

type AuthProvider interface {
	GetAccessToken(ctx context.Context, req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error)
	CreateApplication(ctx context.Context, req *api.CreateApplicationRequest) (*api.CreateApplicationResponse, error)
	UpdateApplication(ctx context.Context, req *api.UpdateApplicationRequest) (*api.UpdateApplicationResponse, error)
	RotateApplicationSecret(ctx context.Context, req *api.RotateApplicationSecretRequest) (*api.RotateApplicationSecretResponse, error)
	DeleteApplication(ctx context.Context, req *api.DeleteApplicationsRequest) (*api.DeleteApplicationResponse, error)
	ListApplications(ctx context.Context, req *api.ListApplicationsRequest) (*api.ListApplicationsResponse, error)
}

type Auth0 struct {
	AuthConfig config.AuthConfig
	Management *management.Management
	userStore  *metadata.UserSubspace
	txMgr      *transaction.Manager
}

func (a *Auth0) managementToTigrisErrorCode(err error) api.Code {
	managementError, ok := err.(management.Error)
	if !ok {
		return api.Code_INTERNAL
	}
	return api.FromHttpCode(managementError.Status())
}

func (a *Auth0) GetAccessToken(ctx context.Context, req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error) {
	switch req.GrantType {
	case api.GrantType_REFRESH_TOKEN:
		return getAccessTokenUsingRefreshToken(req, a)
	case api.GrantType_CLIENT_CREDENTIALS:
		return getAccessTokenUsingClientCredentials(ctx, req, a)
	}
	return nil, errors.InvalidArgument("Failed to GetAccessToken: reason = unsupported grant_type, it has to be one of [refresh_token, client_credentials]")
}

func (a *Auth0) CreateApplication(ctx context.Context, req *api.CreateApplicationRequest) (*api.CreateApplicationResponse, error) {
	currentSub, err := getCurrentSub(ctx)
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

	createdApp := &api.Application{
		Name:        c.GetName(),
		Description: c.GetDescription(),
		Id:          c.GetClientID(),
		Secret:      c.GetClientSecret(),
		CreatedBy:   c.GetClientMetadata()[createdBy],
		CreatedAt:   readDate(c.GetClientMetadata()[createdAt]),
	}
	return &api.CreateApplicationResponse{
		CreatedApplication: createdApp,
	}, nil
}

func (a *Auth0) DeleteApplication(ctx context.Context, req *api.DeleteApplicationsRequest) (*api.DeleteApplicationResponse, error) {
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

	return &api.DeleteApplicationResponse{
		Deleted: true,
	}, nil
}

func (a *Auth0) UpdateApplication(ctx context.Context, req *api.UpdateApplicationRequest) (*api.UpdateApplicationResponse, error) {
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
	return &api.UpdateApplicationResponse{
		UpdatedApplication: &api.Application{
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

func (a *Auth0) RotateApplicationSecret(ctx context.Context, req *api.RotateApplicationSecretRequest) (*api.RotateApplicationSecretResponse, error) {
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

	return &api.RotateApplicationSecretResponse{
		Application: &api.Application{
			Id:          updatedApp.GetClientID(),
			Name:        updatedApp.GetName(),
			Description: updatedApp.GetDescription(),
			Secret:      updatedApp.GetClientSecret(),
			CreatedAt:   readDate(updatedApp.GetClientMetadata()[createdAt]),
			CreatedBy:   updatedApp.GetClientMetadata()[createdBy],
		},
	}, nil
}

func (a *Auth0) ListApplications(ctx context.Context, _ *api.ListApplicationsRequest) (*api.ListApplicationsResponse, error) {

	appList, err := a.Management.Client.List(management.IncludeFields("client_id", "client_metadata", "client_secret", "description", "name"))
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to list applications: reason = %s", err.Error())
	}

	currentSub, err := getCurrentSub(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to list applications: reason = %s", err.Error())
	}

	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to list applications: reason = %s", err.Error())
	}
	var apps []*api.Application
	for _, client := range appList.Clients {
		// filter for this user's apps for this tenant
		if client.GetClientMetadata()[createdBy] == currentSub && client.GetClientMetadata()[tigrisNamespace] == currentNamespace {
			app := &api.Application{
				Name:        client.GetName(),
				Description: client.GetDescription(),
				Id:          client.GetClientID(),
				Secret:      client.GetClientSecret(),
				CreatedAt:   readDate(client.GetClientMetadata()[createdAt]),
				CreatedBy:   client.GetClientMetadata()[createdBy],
				UpdatedAt:   readDate(client.GetClientMetadata()[updatedAt]),
				UpdatedBy:   client.GetClientMetadata()[updatedBy],
			}
			apps = append(apps, app)
		}
	}
	return &api.ListApplicationsResponse{
		Applications: apps,
	}, nil
}

func validateOwnership(ctx context.Context, operationName string, appId string, a *Auth0) (*management.Client, string, error) {
	client, err := a.Management.Client.Read(appId)
	if err != nil {
		return nil, "", api.Errorf(a.managementToTigrisErrorCode(err), "Failed to %s: reason = %s", operationName, err.Error())
	}

	// check ownership before rotating
	currentSub, err := getCurrentSub(ctx)
	if err != nil {
		return nil, "", errors.Internal("Failed to %s: reason = %s", operationName, err.Error())
	}
	if client.GetClientMetadata()[createdBy] != currentSub {
		return nil, "", errors.PermissionDenied("Failed to rotate application secret: reason = You cannot rotate secret for application that is not created by you.")
	}
	return client, currentSub, nil
}

func getCurrentSub(ctx context.Context) (string, error) {
	// further filter for this particular user
	token, err := request.GetAccessToken(ctx)
	if err != nil {
		return "", errors.Internal("Failed to retrieve current sub: reason = %s", err.Error())
	}
	return token.Sub, nil
}

func getAccessTokenUsingRefreshToken(req *api.GetAccessTokenRequest, a *Auth0) (*api.GetAccessTokenResponse, error) {
	data := url.Values{
		"refresh_token": {req.RefreshToken},
		"client_id":     {a.AuthConfig.ClientId},
		"grant_type":    {refreshToken},
		"scope":         {scope},
	}
	resp, err := http.PostForm(a.AuthConfig.ExternalTokenURL, data)
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

		err = json.Unmarshal([]byte(bodyStr), &getAccessTokenResponse)
		if err != nil {
			return nil, errors.Internal("Failed to parse external response: reason = %s", err.Error())
		}
		return &getAccessTokenResponse, nil
	}
	log.Error().Msgf("Auth0 response status code=%d", resp.StatusCode)
	return nil, errors.Internal("Failed to get access token: reason = %s", bodyStr)
}

type tokenMetadataEntry struct {
	AccessToken string
	ExpireAt    int64 // unix second
}

func getAccessTokenUsingClientCredentials(ctx context.Context, req *api.GetAccessTokenRequest, a *Auth0) (*api.GetAccessTokenResponse, error) {
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
	if err != nil {
		return nil, tokenError("Failed to get access token: reason = could not process cache", err)
	}

	if cachedToken != nil {
		tokenMetadataEntry := &tokenMetadataEntry{}
		err := json.Unmarshal(cachedToken, tokenMetadataEntry)
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
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, tokenError("Failed to get access token: reason = failed to create external request payload", err)
	}

	resp, err := http.Post(a.AuthConfig.ExternalTokenURL, "application/json", bytes.NewBuffer(jsonPayload))
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

		err = json.Unmarshal([]byte(bodyStr), &getAccessTokenResponse)
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
	log.Error().Msgf("Auth0 response status code=%d", resp.StatusCode)
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

func deleteApplicationMetadata(ctx context.Context, namespaceId uint32, appId string, metadataKey string, a *Auth0) error {
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

func deleteApplication(ctx context.Context, appId string, a *Auth0) error {
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

func insertApplicationMetadata(ctx context.Context, metadataKey string, req *api.GetAccessTokenRequest, getAccessTokenResponse *api.GetAccessTokenResponse, a *Auth0) error {
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
	cacheEntryBytes, err := json.Marshal(&cacheEntry)
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
