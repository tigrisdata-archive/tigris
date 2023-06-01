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
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/types"
	"golang.org/x/net/context/ctxhttp"
)

const (
	GotrueAudHeaderKey       = "X-JWT-AUD"
	ClientIdPrefix           = "tid_"
	ClientSecretPrefix       = "tsec_"
	GlobalClientIdPrefix     = "tgid_"
	GlobalClientSecretPrefix = "tgsec_"
	ApiKeyPrefix             = "tkey_"
	Component                = "component"
	AppKey                   = "app_key"
	AppKeyUser               = "app key"

	InvitationStatusPending  = "PENDING"
	InvitationStatusAccepted = "ACCEPTED"
	InvitationStatusExpired  = "EXPIRED"

	AppKeyTypeCredentials = "credentials"
	AppKeyTypeApiKey      = "api_key"
)

type gotrue struct {
	AuthConfig config.AuthConfig
}

type CreateUserPayload struct {
	Email    string      `json:"email"`
	Password string      `json:"password"`
	AppData  UserAppData `json:"app_data"`
}

type CreateInvitationPayload struct {
	Email string `json:"email"`
	Role  string `json:"role"`

	TigrisNamespace     string `json:"tigris_namespace"`
	TigrisNamespaceName string `json:"tigris_namespace_name"`

	CreatedBy      string `json:"created_by"`
	CreatedByName  string `json:"created_by_name"`
	ExpirationTime int64  `json:"expiration_time"`
}

type DeleteInvitationsPayload struct {
	Email           string `json:"email"`
	CreatedBy       string `json:"created_by"`
	TigrisNamespace string `json:"tigris_namespace"`
	Status          string `json:"status"`
}

type VerifyInvitationPayload struct {
	Email string `json:"email"`
	Code  string `json:"code"`
	Dry   bool   `json:"dry"`
}

type UserAppData struct {
	CreatedAt       int64  `json:"created_at"`
	CreatedBy       string `json:"created_by"`
	UpdatedAt       int64  `json:"updated_at"`
	UpdatedBy       string `json:"updated_by"`
	TigrisNamespace string `json:"tigris_namespace"`
	Name            string `json:"name"`
	Description     string `json:"description"`
	Project         string `json:"tigris_project"`
	KeyType         string `json:"key_type"`
}

// returns currentSub, creationTime, error.
func _createAppKey(ctx context.Context, clientId string, clientSecret string, g *gotrue, keyName string, keyDescription string, project string, keyType string) (string, int64, error) {
	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to create application: reason - unable to extract current sub")
		return "", 0, errors.Internal("Failed to create application: reason = %s", err.Error())
	}

	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to create application: reason - unable to extract current namespace")
		return "", 0, errors.Internal("Failed to create applications: reason = %s", err.Error())
	}

	// make gotrue call
	email := _getEmail(clientId, keyType, g)
	creationTime := time.Now().UnixMilli()
	payloadBytes, err := jsoniter.Marshal(CreateUserPayload{
		Email:    email,
		Password: clientSecret,
		AppData: UserAppData{
			CreatedAt:       creationTime,
			CreatedBy:       currentSub,
			TigrisNamespace: currentNamespace,
			Name:            keyName,
			Description:     keyDescription,
			Project:         project,
			KeyType:         keyType,
		},
	})
	if err != nil {
		log.Err(err).Msg("Failed to create user")
		return "", 0, errors.Internal("Failed to create user")
	}
	err = createUser(ctx, payloadBytes, g.AuthConfig.PrimaryAudience, AppKeyUser, g)
	if err != nil {
		return "", 0, err
	}
	log.Info().
		Str("namespace", currentNamespace).
		Str("sub", currentSub).
		Str("client_id", clientId).
		Str("key_type", keyType).
		Str(Component, AppKey).
		Msg("appkey created")
	return currentSub, creationTime, nil
}

func (g *gotrue) CreateAppKey(ctx context.Context, req *api.CreateAppKeyRequest) (*api.CreateAppKeyResponse, error) {
	if req.GetProject() == "" {
		return nil, errors.InvalidArgument("Project must be specified")
	}
	if req.GetKeyType() != "" && !(req.GetKeyType() == AppKeyTypeCredentials || req.GetKeyType() == AppKeyTypeApiKey) {
		return nil, errors.InvalidArgument("app key supported types are [credentials, api_key]")
	}

	appKeyType := AppKeyTypeCredentials
	if req.GetKeyType() != "" {
		appKeyType = req.GetKeyType()
	}
	var clientId, clientSecret string
	if appKeyType == AppKeyTypeCredentials {
		clientId = generateClientId(ClientIdPrefix, config.DefaultConfig.Auth.Gotrue.ClientIdLength)
		clientSecret = generateClientSecret(g, ClientSecretPrefix)
	} else {
		clientId = generateClientId(ApiKeyPrefix, config.DefaultConfig.Auth.ApiKeys.Length)
		clientSecret = config.DefaultConfig.Auth.ApiKeys.UserPassword
	}
	currentSub, creationTime, err := _createAppKey(ctx, clientId, clientSecret, g, req.GetName(), req.GetDescription(), req.GetProject(), appKeyType)
	if err != nil {
		return nil, err
	}

	if req.GetKeyType() == AppKeyTypeApiKey {
		clientSecret = "" // hide the secret
	}
	return &api.CreateAppKeyResponse{
		CreatedAppKey: &api.AppKey{
			Id:          clientId,
			Name:        req.GetName(),
			Description: req.GetDescription(),
			Secret:      clientSecret,
			CreatedAt:   creationTime,
			CreatedBy:   currentSub,
			Project:     req.GetProject(),
		},
	}, nil
}

func (g *gotrue) CreateGlobalAppKey(ctx context.Context, req *api.CreateGlobalAppKeyRequest) (*api.CreateGlobalAppKeyResponse, error) {
	clientId := generateClientId(GlobalClientIdPrefix, config.DefaultConfig.Auth.Gotrue.ClientIdLength)
	clientSecret := generateClientSecret(g, GlobalClientSecretPrefix)

	currentSub, creationTime, err := _createAppKey(ctx, clientId, clientSecret, g, req.GetName(), req.GetDescription(), "", AppKeyTypeCredentials)
	if err != nil {
		return nil, err
	}

	return &api.CreateGlobalAppKeyResponse{
		CreatedAppKey: &api.GlobalAppKey{
			Id:          clientId,
			Name:        req.GetName(),
			Description: req.GetDescription(),
			Secret:      clientSecret,
			CreatedAt:   creationTime,
			CreatedBy:   currentSub,
		},
	}, nil
}

func _getEmail(clientId string, appKeyType string, g *gotrue) string {
	if appKeyType == "" || appKeyType == AppKeyTypeCredentials {
		return fmt.Sprintf("%s%s", clientId, g.AuthConfig.Gotrue.UsernameSuffix)
	} else if appKeyType == AppKeyTypeApiKey {
		return fmt.Sprintf("%s%s", clientId, g.AuthConfig.ApiKeys.EmailSuffix)
	}
	return ""
}

func _updateAppKey(ctx context.Context, g *gotrue, id string, name string, description string, appKeyType string) error {
	email := _getEmail(id, appKeyType, g)
	updateAppKeyUrl := fmt.Sprintf("%s/admin/users/%s", g.AuthConfig.Gotrue.URL, email)

	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Couldn't resolve current sub")
		return errors.Internal("Failed to update app key")
	}

	newAppMetadata := UserAppData{
		UpdatedAt: time.Now().UnixMilli(),
		UpdatedBy: currentSub,
	}

	if name != "" {
		newAppMetadata.Name = name
	}

	if description != "" {
		newAppMetadata.Description = description
	}
	appMetadataMap := make(map[string]UserAppData)
	appMetadataMap["app_metadata"] = newAppMetadata
	payloadBytes, err := jsoniter.Marshal(appMetadataMap)
	if err != nil {
		log.Err(err).Msg("Failed to marshal payload")
		return errors.Internal("Unable to update app key")
	}
	payloadBytesReader := bytes.NewReader(payloadBytes)

	client := &http.Client{}
	updateAppKeyReq, err := http.NewRequestWithContext(ctx, http.MethodPut, updateAppKeyUrl, payloadBytesReader)
	if err != nil {
		log.Err(err).Msg("Failed to construct updateAppKeyReq")
		return errors.Internal("Unable to update app key")
	}
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		log.Err(err).Msg("Failed to get admin access token")
		return errors.Internal("Failed to update app key: couldn't get admin access token")
	}
	updateAppKeyReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))
	updateAppKeyReq.Header.Add("Content-Type", "application/json")

	updateAppKeyRes, err := ctxhttp.Do(ctx, client, updateAppKeyReq)
	if err != nil {
		log.Err(err).Msg("Failed to update app key - failed to make call to gotrue")
		return errors.Internal("Failed to update app key")
	}
	defer updateAppKeyRes.Body.Close()

	if updateAppKeyRes.StatusCode != http.StatusOK {
		log.Error().Int("status", updateAppKeyRes.StatusCode).Msg("Received non OK status code to update user.")
		return errors.Internal("Failed to update app key")
	}
	log.Info().
		Str("sub", currentSub).
		Str("client_id", id).
		Str(Component, AppKey).
		Msg("appkey updated")
	return nil
}

// retrieves the key type from the client id naming scheme.
func getAppKeyType(clientId string) string {
	if strings.HasPrefix(clientId, ApiKeyPrefix) {
		return AppKeyTypeApiKey
	}
	return AppKeyTypeCredentials
}

func (g *gotrue) UpdateAppKey(ctx context.Context, req *api.UpdateAppKeyRequest) (*api.UpdateAppKeyResponse, error) {
	if req.GetProject() == "" {
		return nil, errors.InvalidArgument("Project must be specified")
	}
	err := _updateAppKey(ctx, g, req.GetId(), req.GetName(), req.GetDescription(), getAppKeyType(req.GetId()))
	if err != nil {
		return nil, err
	}
	result := &api.UpdateAppKeyResponse{
		UpdatedAppKey: &api.AppKey{
			Id: req.GetId(),
		},
	}
	if req.GetName() != "" {
		result.UpdatedAppKey.Name = req.GetName()
	}
	if req.GetDescription() != "" {
		result.UpdatedAppKey.Description = req.GetDescription()
	}
	return result, nil
}

func (g *gotrue) UpdateGlobalAppKey(ctx context.Context, req *api.UpdateGlobalAppKeyRequest) (*api.UpdateGlobalAppKeyResponse, error) {
	err := _updateAppKey(ctx, g, req.GetId(), req.GetName(), req.GetDescription(), AppKeyTypeCredentials)
	if err != nil {
		return nil, err
	}
	result := &api.UpdateGlobalAppKeyResponse{
		UpdatedAppKey: &api.GlobalAppKey{
			Id: req.GetId(),
		},
	}
	if req.GetName() != "" {
		result.UpdatedAppKey.Name = req.GetName()
	}
	if req.GetDescription() != "" {
		result.UpdatedAppKey.Description = req.GetDescription()
	}
	return result, nil
}

func _rotateAppKeySecret(ctx context.Context, g *gotrue, id string, clientSecretPrefix string) (string, error) {
	// no rotation for api key, only for credentials
	email := _getEmail(id, AppKeyTypeCredentials, g)
	updateAppKeyUrl := fmt.Sprintf("%s/admin/users/%s", g.AuthConfig.Gotrue.URL, email)

	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Couldn't resolve current sub")
		return "", errors.Internal("Failed to update app key")
	}

	newSecret := generateClientSecret(g, clientSecretPrefix)

	newAppMetadata := &UserAppData{UpdatedBy: currentSub, UpdatedAt: time.Now().UnixMilli()}
	payload := make(map[string]any)
	payload["password"] = newSecret
	payload["app_metadata"] = newAppMetadata
	payloadBytes, err := jsoniter.Marshal(payload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal payload")
		return "", errors.Internal("Unable to update app key")
	}

	payloadBytesReader := bytes.NewReader(payloadBytes)

	client := &http.Client{}
	updateAppKeyReq, err := http.NewRequestWithContext(ctx, http.MethodPut, updateAppKeyUrl, payloadBytesReader)
	if err != nil {
		log.Err(err).Msg("Failed to construct updateAppKeyReq")
		return "", errors.Internal("Unable to update app key")
	}
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		log.Err(err).Msg("Failed to get admin access token")
		return "", errors.Internal("Failed to update app key: couldn't get admin access token")
	}
	updateAppKeyReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))
	updateAppKeyReq.Header.Add("Content-Type", "application/json")

	updateAppKeyRes, err := ctxhttp.Do(ctx, client, updateAppKeyReq)
	if err != nil {
		log.Err(err).Msg("Failed to update app key - failed to make call to gotrue")
		return "", errors.Internal("Failed to update app key")
	}
	defer updateAppKeyRes.Body.Close()

	if updateAppKeyRes.StatusCode != http.StatusOK {
		log.Error().Int("status", updateAppKeyRes.StatusCode).Msg("Received non OK status code to update user.")
		return "", errors.Internal("Failed to update app key")
	}

	log.Info().
		Str("sub", currentSub).
		Str("client_id", id).
		Str(Component, AppKey).
		Msg("appkey rotated")

	return newSecret, nil
}

func (g *gotrue) RotateAppKey(ctx context.Context, req *api.RotateAppKeyRequest) (*api.RotateAppKeyResponse, error) {
	if req.GetProject() == "" {
		return nil, errors.InvalidArgument("Project must be specified")
	}
	newSecret, err := _rotateAppKeySecret(ctx, g, req.GetId(), ClientSecretPrefix)
	if err != nil {
		return nil, err
	}

	result := &api.RotateAppKeyResponse{
		AppKey: &api.AppKey{
			Id:     req.GetId(),
			Secret: newSecret,
		},
	}
	return result, nil
}

func (g *gotrue) RotateGlobalAppKeySecret(ctx context.Context, req *api.RotateGlobalAppKeySecretRequest) (*api.RotateGlobalAppKeySecretResponse, error) {
	newSecret, err := _rotateAppKeySecret(ctx, g, req.GetId(), GlobalClientSecretPrefix)
	if err != nil {
		return nil, err
	}

	result := &api.RotateGlobalAppKeySecretResponse{
		AppKey: &api.GlobalAppKey{
			Id:     req.GetId(),
			Secret: newSecret,
		},
	}
	return result, nil
}

func _deleteAppKey(ctx context.Context, g *gotrue, id string) error {
	// TODO: verify ownership

	// get admin access token
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		return err
	}

	// make external call
	email := _getEmail(id, getAppKeyType(id), g)
	deleteUserUrl := fmt.Sprintf("%s/admin/users/%s", g.AuthConfig.Gotrue.URL, email)
	client := &http.Client{}
	deleteUserReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteUserUrl, nil)
	if err != nil {
		log.Err(err).Msg("Failed to form request to delete user from gotrue")
		return errors.Internal("Failed to form request to delete app key")
	}
	deleteUserReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))

	deleteUserRes, err := ctxhttp.Do(ctx, client, deleteUserReq)
	if err != nil {
		log.Err(err).Msg("Failed to delete user from gotrue")
		return errors.Internal("Failed to delete user from gotrue")
	}

	defer deleteUserRes.Body.Close()

	if deleteUserRes.StatusCode != http.StatusOK {
		log.Error().Int("status", deleteUserRes.StatusCode).Msg("Received non OK status code to delete user")
		return errors.Internal("Received non OK status code to delete user")
	}

	currentSub, _ := GetCurrentSub(ctx)
	log.Info().
		Str("sub", currentSub).
		Str("client_id", id).
		Str(Component, AppKey).
		Msg("appkey deleted")
	return nil
}

func (g *gotrue) DeleteAppKey(ctx context.Context, req *api.DeleteAppKeyRequest) (*api.DeleteAppKeyResponse, error) {
	if req.GetProject() == "" {
		return nil, errors.InvalidArgument("Project must be specified")
	}
	err := _deleteAppKey(ctx, g, req.GetId())
	if err != nil {
		return nil, err
	}
	return &api.DeleteAppKeyResponse{
		Deleted: true,
	}, nil
}

func (g *gotrue) DeleteGlobalAppKey(ctx context.Context, req *api.DeleteGlobalAppKeyRequest) (*api.DeleteGlobalAppKeyResponse, error) {
	err := _deleteAppKey(ctx, g, req.GetId())
	if err != nil {
		return nil, err
	}
	return &api.DeleteGlobalAppKeyResponse{
		Deleted: true,
	}, nil
}

type appKeyInternal struct {
	Id          string
	Name        string
	Description string
	Secret      string
	CreatedBy   string
	CreatedAt   int64
	Project     string
	KeyType     string
}

func _listAppKeys(ctx context.Context, g *gotrue, project string, keyType string) ([]*appKeyInternal, error) {
	if keyType != "" {
		if !(keyType == AppKeyTypeApiKey || keyType == AppKeyTypeCredentials) {
			return nil, errors.InvalidArgument("Invalid keytype. Supported values are [credentials, api_key]")
		}
	}
	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to list applications: reason = %s", err.Error())
	}

	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to list applications: reason = %s", err.Error())
	}

	// get admin access token
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		return nil, err
	}

	// make external call
	getUsersUrl := fmt.Sprintf("%s/admin/users?created_by=%s&tigris_namespace=%s&tigris_project=%s&page=1&per_page=5000", g.AuthConfig.Gotrue.URL, currentSub, currentNamespace, project)
	if keyType != "" {
		getUsersUrl = fmt.Sprintf("%s&keyType=%s", getUsersUrl, keyType)
	}
	log.Info().Str("url", getUsersUrl).Msg("Fetching users")
	client := &http.Client{}
	getUsersReq, err := http.NewRequestWithContext(ctx, http.MethodGet, getUsersUrl, nil)
	if err != nil {
		log.Err(err).Msg("Failed to form request to delete user from gotrue")
		return nil, errors.Internal("Failed to form request to get users request")
	}
	getUsersReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))

	getUsersResp, err := client.Do(getUsersReq)
	if err != nil {
		log.Err(err).Msg("Failed to get users from gotrue")
		return nil, errors.Internal("Failed to get users from gotrue")
	}

	if getUsersResp.StatusCode != http.StatusOK {
		log.Error().Int("status", getUsersResp.StatusCode).Msg("Received non OK status code to get users")
		return nil, errors.Internal("Received non OK status code to get users")
	}

	// remove it from metadata cache
	defer getUsersResp.Body.Close()

	getUsersRespBytes, err := io.ReadAll(getUsersResp.Body)
	if err != nil {
		log.Err(err).Msg("Failed to read get users response")
		return nil, errors.Internal("Failed to read get users response")
	}
	var getUsersRespJSON map[string]jsoniter.RawMessage
	err = jsoniter.Unmarshal(getUsersRespBytes, &getUsersRespJSON)
	if err != nil {
		log.Err(err).Msg("Failed to parse getUsersResp")
		return nil, errors.Internal("Failed to parse getUsers response")
	}

	var users []map[string]jsoniter.RawMessage
	err = jsoniter.Unmarshal(getUsersRespJSON["users"], &users)
	if err != nil {
		log.Err(err).Msg("Failed to parse getUsersResp - users")
		return nil, errors.Internal("Failed to parse getUsers response")
	}

	appKeys := make([]*appKeyInternal, len(users))
	for i, user := range users {
		var email, clientSecret string
		err := jsoniter.Unmarshal(user["email"], &email)
		if err != nil {
			log.Err(err).Msg("Failed to parse getUsersResp - email")
			return nil, errors.Internal("Failed to parse getUsers response")
		}
		clientId := strings.Split(email, "@")[0]

		err = jsoniter.Unmarshal(user["encrypted_password"], &clientSecret)
		if err != nil {
			log.Err(err).Msg("Failed to parse getUsersResp - clientSecret")
			return nil, errors.Internal("Failed to parse getUsers response")
		}

		var appMetadata UserAppData
		err = jsoniter.Unmarshal(user["app_metadata"], &appMetadata)
		if err != nil {
			log.Err(err).Msg("Failed to parse getUsersResp - appMetadata")
			return nil, errors.Internal("Failed to parse getUsers response")
		}

		var createdAtStr string
		var createdAtMillis int64
		err = jsoniter.Unmarshal(user["created_at"], &createdAtStr)
		if err != nil {
			log.Err(err).Msg("Failed to parse getUsersResp - createAt")
			return nil, errors.Internal("Failed to parse getUsers response")
		}
		if createdAtStr != "" {
			// parse string time to millis using rfc3339 format
			createdAtMillis = readDate(createdAtStr)
		}
		log.Info().Interface("metadata", appMetadata).Msg("AppMetadata")
		appKey := appKeyInternal{
			Id:          clientId,
			Name:        appMetadata.Name,
			Description: appMetadata.Description,
			Secret:      clientSecret,
			CreatedBy:   appMetadata.CreatedBy,
			CreatedAt:   createdAtMillis,
			Project:     appMetadata.Project,
		}
		if appMetadata.KeyType == "" || appMetadata.KeyType == AppKeyTypeCredentials {
			appKey.KeyType = AppKeyTypeCredentials
		} else if appMetadata.KeyType == AppKeyTypeApiKey {
			appKey.KeyType = AppKeyTypeApiKey
		}
		appKeys[i] = &appKey
	}
	return appKeys, nil
}

func (g *gotrue) ListAppKeys(ctx context.Context, req *api.ListAppKeysRequest) (*api.ListAppKeysResponse, error) {
	appKeysInternal, err := _listAppKeys(ctx, g, req.GetProject(), req.GetKeyType())
	if err != nil {
		return nil, errors.Internal("Failed to list app keys")
	}
	appKeys := make([]*api.AppKey, len(appKeysInternal))
	for i, internalAppKey := range appKeysInternal {
		appKeys[i] = &api.AppKey{
			Id:          internalAppKey.Id,
			Name:        internalAppKey.Name,
			Description: internalAppKey.Description,
			CreatedAt:   internalAppKey.CreatedAt,
			CreatedBy:   internalAppKey.CreatedBy,
			Project:     internalAppKey.Project,
			KeyType:     internalAppKey.KeyType,
		}
		// expose secret in case of credentials, for backward compatibility defaults to credentials
		if req.GetKeyType() == "" || req.GetKeyType() == AppKeyTypeCredentials {
			appKeys[i].Secret = internalAppKey.Secret
		}
	}

	return &api.ListAppKeysResponse{
		AppKeys: appKeys,
	}, nil
}

func (g *gotrue) ListGlobalAppKeys(ctx context.Context, _ *api.ListGlobalAppKeysRequest) (*api.ListGlobalAppKeysResponse, error) {
	appKeysInternal, err := _listAppKeys(ctx, g, "", "")
	if err != nil {
		return nil, errors.Internal("Failed to delete app keys")
	}
	globalAppKeys := make([]*api.GlobalAppKey, len(appKeysInternal))
	for i, internalAppKey := range appKeysInternal {
		globalAppKeys[i] = &api.GlobalAppKey{
			Id:          internalAppKey.Id,
			Name:        internalAppKey.Name,
			Description: internalAppKey.Description,
			Secret:      internalAppKey.Secret,
			CreatedAt:   internalAppKey.CreatedAt,
			CreatedBy:   internalAppKey.CreatedBy,
		}
	}
	return &api.ListGlobalAppKeysResponse{
		AppKeys: globalAppKeys,
	}, nil
}

func (g *gotrue) DeleteAppKeys(ctx context.Context, project string) error {
	// TODO make it transactional on gotrue side
	listAppKeysResp, err := g.ListAppKeys(ctx, &api.ListAppKeysRequest{Project: project})
	if err != nil {
		log.Err(err).Msg("Failed to list app keys to delete them")
		return errors.Internal("Failed to delete app keys")
	}
	currentSub, _ := GetCurrentSub(ctx)

	for _, key := range listAppKeysResp.GetAppKeys() {
		_, err := g.DeleteAppKey(ctx, &api.DeleteAppKeyRequest{
			Id:      key.Id,
			Project: key.Project,
		})
		if err != nil {
			log.Err(err).Str("clientId", key.Id).Msg("Failed to delete app key")
			return errors.Internal("Failed to delete all app keys")
		}

		log.Info().
			Str("sub", currentSub).
			Str("client_id", key.GetId()).
			Str(Component, AppKey).
			Msg("appkey deleted via project")
	}
	return nil
}

func (g *gotrue) GetAccessToken(ctx context.Context, req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error) {
	switch req.GrantType {
	case api.GrantType_REFRESH_TOKEN:
		return nil, errors.Unimplemented("Use client_credentials to get the access token")
	case api.GrantType_CLIENT_CREDENTIALS:
		accessToken, expiresIn, err := getAccessTokenUsingClientCredentialsGotrue(ctx, _getEmail(req.GetClientId(), AppKeyTypeCredentials, g), req.GetClientSecret(), g)
		if err != nil {
			return nil, err
		}
		return &api.GetAccessTokenResponse{
			AccessToken:  accessToken,
			RefreshToken: "",
			ExpiresIn:    expiresIn,
		}, nil
	}
	return nil, errors.InvalidArgument("Failed to GetAccessToken: reason = unsupported grant_type, it has to be one of [refresh_token, client_credentials]")
}

func (g *gotrue) ValidateApiKey(ctx context.Context, apiKey string, auds []string) (*types.AccessToken, error) {
	// get admin token
	adminToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		log.Err(err).Msgf("Failed to create gotrue admin access token")
		return nil, errors.Internal("Could not form request to validate api key")
	}
	// admin-get user
	client := &http.Client{}
	email := _getEmail(apiKey, AppKeyTypeApiKey, g)
	getUserReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/admin/users/%s", g.AuthConfig.Gotrue.URL, email), nil)
	if err != nil {
		log.Err(err).Msgf("Failed to create request to retrieve user")
		return nil, errors.Internal("Could not form request to validate api key")
	}
	getUserReq.Header.Add("Content-Type", "application/json")
	getUserReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminToken))

	getUserRes, err := client.Do(getUserReq)
	if err != nil {
		log.Err(err).Msg("Failed to make get user call")
		return nil, errors.Internal("Could not validate api key")
	}
	defer getUserRes.Body.Close()

	// form the validatedClaims
	if getUserRes.StatusCode != http.StatusOK {
		log.Error().Int("status", getUserRes.StatusCode).Msg("Received non OK status from gotrue while validating api key")
		return nil, errors.Internal("Received non OK status from gotrue while validating api key")
	}

	getUserResBody, err := io.ReadAll(getUserRes.Body)
	if err != nil {
		log.Err(err).Msg("Failed to read get user body while validating api key")
		return nil, errors.Internal("Failed to validate the api key")
	}

	// parse JSON response
	var getUserJsonMap map[string]any
	err = json.Unmarshal(getUserResBody, &getUserJsonMap)
	if err != nil {
		log.Err(err).Msg("Failed to deserialize response into JSON")
		return nil, errors.Internal("Failed to validate the api key")
	}

	// validate password
	password := getUserJsonMap["encrypted_password"].(string)
	log.Error().Str("stored_password", password).
		Str("input_password", config.DefaultConfig.Auth.ApiKeys.UserPassword).
		Msg("Do password match")
	if config.DefaultConfig.Auth.ApiKeys.UserPassword != password {
		return nil, errors.Unauthenticated("Unsupported api-key")
	}

	// validate aud
	aud := getUserJsonMap["aud"].(string)
	allowedAud := false
	for _, supportedAud := range auds {
		if supportedAud == aud {
			allowedAud = true
			break
		}
	}
	if !allowedAud {
		log.Error().Str("api_key_aud", aud).Strs("supported_auds", auds).Msg("Audience is not supported")
		return nil, errors.Unauthenticated("Unsupported audience")
	}

	role := getUserJsonMap["role"].(string)
	metadata := getUserJsonMap["app_metadata"].(map[string]any)
	tigrisNamespaceCode := metadata["tigris_namespace"].(string)
	project := metadata["tigris_project"].(string)
	sub := fmt.Sprintf("gt_key|%s", getUserJsonMap["id"])

	return &types.AccessToken{
		Namespace: tigrisNamespaceCode,
		Sub:       sub,
		Project:   project,
		Role:      role,
	}, nil
}

func createUser(ctx context.Context, createUserPayload []byte, aud string, userType string, g *gotrue) error {
	payloadReader := bytes.NewReader(createUserPayload)

	client := &http.Client{}
	createAppKeyReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/signup", g.AuthConfig.Gotrue.URL), payloadReader)
	if err != nil {
		log.Err(err).Msgf("Failed to create %s request", userType)
		return err
	}
	createAppKeyReq.Header.Add("X-JWT-AUD", aud)
	createAppKeyReq.Header.Add("Content-Type", "application/json")

	createAppKeyRes, err := client.Do(createAppKeyReq)
	if err != nil {
		log.Err(err).Msgf("Failed to make %s call", userType)
		return err
	}
	defer createAppKeyRes.Body.Close()

	if createAppKeyRes.StatusCode != http.StatusOK {
		log.Error().Int("status", createAppKeyRes.StatusCode).Msgf("Received non OK status from gotrue while creating %s", userType)
		return errors.Internal("Received non OK status from gotrue while creating %s", userType)
	}
	return nil
}

func getAccessTokenUsingClientCredentialsGotrue(ctx context.Context, clientId string, clientSecret string, g *gotrue) (string, int32, error) {
	// make external call
	getTokenUrl := fmt.Sprintf("%s/token?grant_type=password", g.AuthConfig.Gotrue.URL)

	payloadValues := url.Values{}
	payloadValues.Set("username", clientId)
	payloadValues.Set("password", clientSecret)
	log.Error().Interface("payload", payloadValues).Msg("Payload")
	client := &http.Client{}

	getTokenReq, err := http.NewRequestWithContext(ctx, http.MethodPost, getTokenUrl, strings.NewReader(payloadValues.Encode()))
	if err != nil {
		log.Err(err).Msg("Failed to call to get token")
		return "", 0, err
	}
	getTokenReq.Header.Add(GotrueAudHeaderKey, g.AuthConfig.PrimaryAudience)
	getTokenReq.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	getTokenRes, err := ctxhttp.Do(ctx, client, getTokenReq)
	if err != nil {
		log.Err(err).Msg("Failed to call to get token")
		return "", 0, err
	}
	defer getTokenRes.Body.Close()

	getTokenResBody, err := io.ReadAll(getTokenRes.Body)
	if err != nil {
		log.Err(err).Msg("Failed to read getTokenRes body")
		return "", 0, err
	}

	if getTokenRes.StatusCode == http.StatusBadRequest {
		log.Error().Int("status", getTokenRes.StatusCode).Msg("Non OK status received to get access token")
		return "", 0, errors.Unauthenticated("Invalid credentials")
	} else if getTokenRes.StatusCode != http.StatusOK {
		log.Error().Int("status", getTokenRes.StatusCode).Msg("Non OK status received to get access token")
		return "", 0, errors.Internal("Non OK status code received from gotrue")
	}

	// parse JSON response
	var getTokenJsonMap map[string]any
	err = json.Unmarshal(getTokenResBody, &getTokenJsonMap)
	if err != nil {
		log.Err(err).Msg("Failed to deserialize response into JSON")
		return "", 0, err
	}

	return getTokenJsonMap["access_token"].(string), int32(getTokenJsonMap["expires_in"].(float64)), nil
}

func getGotrueAdminAccessToken(ctx context.Context, g *gotrue) (string, int32, error) {
	return getAccessTokenUsingClientCredentialsGotrue(ctx, g.AuthConfig.Gotrue.AdminUsername, g.AuthConfig.Gotrue.AdminPassword, g)
}

var (
	idChars     = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")
	secretChars = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-+")
)

func generateClientId(prefix string, length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = idChars[generateRandomInt(len(idChars))]
	}
	return fmt.Sprintf("%s%s", prefix, string(b))
}

func generateClientSecret(g *gotrue, prefix string) string {
	clientSecretLength := g.AuthConfig.Gotrue.ClientSecretLength
	b := make([]rune, clientSecretLength)
	for i := range b {
		b[i] = secretChars[generateRandomInt(len(secretChars))]
	}
	return fmt.Sprintf("%s%s", prefix, string(b))
}

func generateRandomInt(max int) int {
	var bytes [8]byte
	_, err := rand.Read(bytes[:])
	if err != nil {
		log.Err(err).Msgf("Failed to generate random int of length: %d", max)
	}
	return int(binary.BigEndian.Uint32(bytes[:])) % max
}

func validateInvitationStatusInput(inputStatus string) error {
	val := strings.ToUpper(inputStatus)
	if !(val == InvitationStatusPending || val == InvitationStatusExpired || val == InvitationStatusAccepted) {
		return errors.InvalidArgument("Status can be one of these [%s, %s, %s]", InvitationStatusPending, InvitationStatusAccepted, InvitationStatusExpired)
	}
	return nil
}
