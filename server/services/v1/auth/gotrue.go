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

	"github.com/davecgh/go-spew/spew"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
	"golang.org/x/net/context/ctxhttp"
)

const (
	GotrueAudHeaderKey = "X-JWT-AUD"
	ClientIdPrefix     = "tid_"
	ClientSecretPrefix = "tsec_"
	Component          = "component"
	AppKey             = "app_key"
	AppKeyUser         = "app key"

	InvitationStatusPending  = "PENDING"
	InvitationStatusAccepted = "ACCEPTED"
	InvitationStatusExpired  = "EXPIRED"
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
}

func _createAppKey(ctx context.Context, clientId string, clientSecret string, g *gotrue, keyName string, keyDescription string, project string) (error, string, int64) {
	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to create application: reason - unable to extract current sub")
		return errors.Internal("Failed to create application: reason = %s", err.Error()), "", 0
	}

	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to create application: reason - unable to extract current namespace")
		return errors.Internal("Failed to create applications: reason = %s", err.Error()), "", 0
	}

	// make gotrue call
	creationTime := time.Now().UnixMilli()
	payloadBytes, err := jsoniter.Marshal(CreateUserPayload{
		Email:    fmt.Sprintf("%s%s", clientId, g.AuthConfig.Gotrue.UsernameSuffix),
		Password: clientSecret,
		AppData: UserAppData{
			CreatedAt:       creationTime,
			CreatedBy:       currentSub,
			TigrisNamespace: currentNamespace,
			Name:            keyName,
			Description:     keyDescription,
			Project:         project,
		},
	})
	if err != nil {
		log.Err(err).Msg("Failed to create user")
		return errors.Internal("Failed to create user"), "", 0
	}
	err = createUser(ctx, payloadBytes, g.AuthConfig.PrimaryAudience, AppKeyUser, g)
	if err != nil {
		return err, "", 0
	}
	log.Info().
		Str("namespace", currentNamespace).
		Str("sub", currentSub).
		Str("client_id", clientId).
		Str(Component, AppKey).
		Msg("appkey created")
	return nil, currentSub, creationTime
}

func (g *gotrue) CreateAppKey(ctx context.Context, req *api.CreateAppKeyRequest) (*api.CreateAppKeyResponse, error) {
	if req.GetProject() == "" {
		return nil, errors.InvalidArgument("Project must be specified")
	}
	clientId := generateClientId(g)
	clientSecret := generateClientSecret(g)

	err, currentSub, creationTime := _createAppKey(ctx, clientId, clientSecret, g, req.GetName(), req.GetDescription(), req.GetProject())
	if err != nil {
		return nil, err
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
	clientId := generateClientId(g)
	clientSecret := generateClientSecret(g)

	err, currentSub, creationTime := _createAppKey(ctx, clientId, clientSecret, g, req.GetName(), req.GetDescription(), "")
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

func _updateAppKey(ctx context.Context, g *gotrue, id string, name string, description string) error {
	email := fmt.Sprintf("%s%s", id, g.AuthConfig.Gotrue.UsernameSuffix)
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

func (g *gotrue) UpdateAppKey(ctx context.Context, req *api.UpdateAppKeyRequest) (*api.UpdateAppKeyResponse, error) {
	if req.GetProject() == "" {
		return nil, errors.InvalidArgument("Project must be specified")
	}
	err := _updateAppKey(ctx, g, req.GetId(), req.GetName(), req.GetDescription())
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
	err := _updateAppKey(ctx, g, req.GetId(), req.GetName(), req.GetDescription())
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

func _rotateAppKeySecret(ctx context.Context, g *gotrue, id string) (error, string) {
	email := fmt.Sprintf("%s%s", id, g.AuthConfig.Gotrue.UsernameSuffix)
	updateAppKeyUrl := fmt.Sprintf("%s/admin/users/%s", g.AuthConfig.Gotrue.URL, email)

	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Couldn't resolve current sub")
		return errors.Internal("Failed to update app key"), ""
	}

	newSecret := generateClientSecret(g)

	newAppMetadata := &UserAppData{UpdatedBy: currentSub, UpdatedAt: time.Now().UnixMilli()}
	payload := make(map[string]any)
	payload["password"] = newSecret
	payload["app_metadata"] = newAppMetadata
	payloadBytes, err := jsoniter.Marshal(payload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal payload")
		return errors.Internal("Unable to update app key"), ""
	}

	payloadBytesReader := bytes.NewReader(payloadBytes)

	client := &http.Client{}
	updateAppKeyReq, err := http.NewRequestWithContext(ctx, http.MethodPut, updateAppKeyUrl, payloadBytesReader)
	if err != nil {
		log.Err(err).Msg("Failed to construct updateAppKeyReq")
		return errors.Internal("Unable to update app key"), ""
	}
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		log.Err(err).Msg("Failed to get admin access token")
		return errors.Internal("Failed to update app key: couldn't get admin access token"), ""
	}
	updateAppKeyReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))
	updateAppKeyReq.Header.Add("Content-Type", "application/json")

	updateAppKeyRes, err := ctxhttp.Do(ctx, client, updateAppKeyReq)
	if err != nil {
		log.Err(err).Msg("Failed to update app key - failed to make call to gotrue")
		return errors.Internal("Failed to update app key"), ""
	}
	defer updateAppKeyRes.Body.Close()

	if updateAppKeyRes.StatusCode != http.StatusOK {
		log.Error().Int("status", updateAppKeyRes.StatusCode).Msg("Received non OK status code to update user.")
		return errors.Internal("Failed to update app key"), ""
	}

	log.Info().
		Str("sub", currentSub).
		Str("client_id", id).
		Str(Component, AppKey).
		Msg("appkey rotated")

	return nil, newSecret
}

func (g *gotrue) RotateAppKey(ctx context.Context, req *api.RotateAppKeyRequest) (*api.RotateAppKeyResponse, error) {
	if req.GetProject() == "" {
		return nil, errors.InvalidArgument("Project must be specified")
	}
	err, newSecret := _rotateAppKeySecret(ctx, g, req.GetId())
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
	err, newSecret := _rotateAppKeySecret(ctx, g, req.GetId())
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
	deleteUserUrl := fmt.Sprintf("%s/admin/users/%s%s", g.AuthConfig.Gotrue.URL, id, g.AuthConfig.Gotrue.UsernameSuffix)
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
}

func _listAppKeys(ctx context.Context, g *gotrue, project string) (error, []*appKeyInternal) {
	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		return errors.Internal("Failed to list applications: reason = %s", err.Error()), nil
	}

	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		return errors.Internal("Failed to list applications: reason = %s", err.Error()), nil
	}

	// get admin access token
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		return err, nil
	}

	// make external call
	var getUsersUrl string
	if project != "" {
		getUsersUrl = fmt.Sprintf("%s/admin/users?created_by=%s&tigris_namespace=%s&tigris_project=%s&page=1&per_page=5000", g.AuthConfig.Gotrue.URL, currentSub, currentNamespace, project)
	} else {
		getUsersUrl = fmt.Sprintf("%s/admin/users?created_by=%s&tigris_namespace=%s&page=1&per_page=5000", g.AuthConfig.Gotrue.URL, currentSub, currentNamespace)
	}
	client := &http.Client{}
	getUsersReq, err := http.NewRequestWithContext(ctx, http.MethodGet, getUsersUrl, nil)
	if err != nil {
		log.Err(err).Msg("Failed to form request to delete user from gotrue")
		return errors.Internal("Failed to form request to get users request"), nil
	}
	getUsersReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))

	getUsersResp, err := client.Do(getUsersReq)
	if err != nil {
		log.Err(err).Msg("Failed to get users from gotrue")
		return errors.Internal("Failed to get users from gotrue"), nil
	}

	if getUsersResp.StatusCode != http.StatusOK {
		log.Error().Int("status", getUsersResp.StatusCode).Msg("Received non OK status code to get users")
		return errors.Internal("Received non OK status code to get users"), nil
	}

	// remove it from metadata cache
	defer getUsersResp.Body.Close()

	getUsersRespBytes, err := io.ReadAll(getUsersResp.Body)
	if err != nil {
		log.Err(err).Msg("Failed to read get users response")
		return errors.Internal("Failed to read get users response"), nil
	}
	var getUsersRespJSON map[string]jsoniter.RawMessage
	err = jsoniter.Unmarshal(getUsersRespBytes, &getUsersRespJSON)
	if err != nil {
		log.Err(err).Msg("Failed to parse getUsersResp")
		return errors.Internal("Failed to parse getUsers response"), nil
	}

	var users []map[string]jsoniter.RawMessage
	err = jsoniter.Unmarshal(getUsersRespJSON["users"], &users)
	if err != nil {
		log.Err(err).Msg("Failed to parse getUsersResp - users")
		return errors.Internal("Failed to parse getUsers response"), nil
	}

	appKeys := make([]*appKeyInternal, len(users))
	for i, user := range users {
		var email, clientSecret string
		err := jsoniter.Unmarshal(user["email"], &email)
		if err != nil {
			log.Err(err).Msg("Failed to parse getUsersResp - email")
			return errors.Internal("Failed to parse getUsers response"), nil
		}
		clientId := strings.Split(email, "@")[0]

		err = jsoniter.Unmarshal(user["encrypted_password"], &clientSecret)
		if err != nil {
			log.Err(err).Msg("Failed to parse getUsersResp - clientSecret")
			return errors.Internal("Failed to parse getUsers response"), nil
		}

		var appMetadata UserAppData
		err = jsoniter.Unmarshal(user["app_metadata"], &appMetadata)
		if err != nil {
			log.Err(err).Msg("Failed to parse getUsersResp - appMetadata")
			return errors.Internal("Failed to parse getUsers response"), nil
		}

		var createdAtStr string
		var createdAtMillis int64
		err = jsoniter.Unmarshal(user["created_at"], &createdAtStr)
		if err != nil {
			log.Err(err).Msg("Failed to parse getUsersResp - createAt")
			return errors.Internal("Failed to parse getUsers response"), nil
		}
		if createdAtStr != "" {
			// parse string time to millis using rfc3339 format
			createdAtMillis = readDate(createdAtStr)
		}

		appKey := appKeyInternal{
			Id:          clientId,
			Name:        appMetadata.Name,
			Description: appMetadata.Description,
			Secret:      clientSecret,
			CreatedBy:   appMetadata.CreatedBy,
			CreatedAt:   createdAtMillis,
			Project:     appMetadata.Project,
		}
		appKeys[i] = &appKey
	}
	return nil, appKeys
}

func (g *gotrue) ListAppKeys(ctx context.Context, req *api.ListAppKeysRequest) (*api.ListAppKeysResponse, error) {
	err, appKeysInternal := _listAppKeys(ctx, g, req.GetProject())
	if err != nil {
		return nil, errors.Internal("Failed to delete app keys")
	}
	appKeys := make([]*api.AppKey, len(appKeysInternal))
	for i, internalAppKey := range appKeysInternal {
		appKeys[i] = &api.AppKey{
			Id:          internalAppKey.Id,
			Name:        internalAppKey.Name,
			Description: internalAppKey.Description,
			Secret:      internalAppKey.Secret,
			CreatedAt:   internalAppKey.CreatedAt,
			CreatedBy:   internalAppKey.CreatedBy,
			Project:     internalAppKey.Project,
		}
	}
	return &api.ListAppKeysResponse{
		AppKeys: appKeys,
	}, nil
}

func (g *gotrue) ListGlobalAppKeys(ctx context.Context, req *api.ListGlobalAppKeysRequest) (*api.ListGlobalAppKeysResponse, error) {
	err, appKeysInternal := _listAppKeys(ctx, g, "")
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
		spew.Dump(ctx)
		accessToken, expiresIn, err := getAccessTokenUsingClientCredentialsGotrue(ctx, fmt.Sprintf("%s%s", req.GetClientId(), g.AuthConfig.Gotrue.UsernameSuffix), req.GetClientSecret(), g)
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

func generateClientId(g *gotrue) string {
	clientIdLength := g.AuthConfig.Gotrue.ClientIdLength
	b := make([]rune, clientIdLength)
	for i := range b {
		b[i] = idChars[generateRandomInt(len(idChars))]
	}
	return fmt.Sprintf("%s%s", ClientIdPrefix, string(b))
}

func generateClientSecret(g *gotrue) string {
	clientSecretLength := g.AuthConfig.Gotrue.ClientSecretLength
	b := make([]rune, clientSecretLength)
	for i := range b {
		b[i] = secretChars[generateRandomInt(len(secretChars))]
	}
	return fmt.Sprintf("%s%s", ClientSecretPrefix, string(b))
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
