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

	AppKeyUser = "app key"

	InvitationStatusPending  = "PENDING"
	InvitationStatusAccepted = "ACCEPTED"
	InvitationStatusExpired  = "EXPIRED"
)

var InvalidInvitationCodeErr = errors.Unauthenticated("Failed to verify invitation code")

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

	TigrisNamespace string `json:"tigris_namespace"`
	CreatedBy       string `json:"created_by"`
	CreatedByName   string `json:"created_by_name"`
	ExpirationTime  int64  `json:"expiration_time"`
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

func (g *gotrue) CreateAppKey(ctx context.Context, req *api.CreateAppKeyRequest) (*api.CreateAppKeyResponse, error) {
	clientId := generateClientId(g)
	clientSecret := generateClientSecret(g)

	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to create application: reason - unable to extract current sub")
		return nil, errors.Internal("Failed to create application: reason = %s", err.Error())
	}

	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to create application: reason - unable to extract current namespace")
		return nil, errors.Internal("Failed to create applications: reason = %s", err.Error())
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
			Name:            req.GetName(),
			Description:     req.GetDescription(),
			Project:         req.GetProject(),
		},
	})
	if err != nil {
		log.Err(err).Msg("Failed to create user")
		return nil, errors.Internal("Failed to create user")
	}
	err = createUser(ctx, payloadBytes, g.AuthConfig.PrimaryAudience, AppKeyUser, g)
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

func (g *gotrue) UpdateAppKey(ctx context.Context, req *api.UpdateAppKeyRequest) (*api.UpdateAppKeyResponse, error) {
	email := fmt.Sprintf("%s%s", req.GetId(), g.AuthConfig.Gotrue.UsernameSuffix)
	updateAppKeyUrl := fmt.Sprintf("%s/admin/users/%s", g.AuthConfig.Gotrue.URL, email)

	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Couldn't resolve current sub")
		return nil, errors.Internal("Failed to update app key")
	}

	newAppMetadata := UserAppData{
		UpdatedAt: time.Now().UnixMilli(),
		UpdatedBy: currentSub,
	}

	if req.GetName() != "" {
		newAppMetadata.Name = req.GetName()
	}

	if req.GetDescription() != "" {
		newAppMetadata.Description = req.GetDescription()
	}
	appMetadataMap := make(map[string]UserAppData)
	appMetadataMap["app_metadata"] = newAppMetadata
	payloadBytes, err := jsoniter.Marshal(appMetadataMap)
	if err != nil {
		log.Err(err).Msg("Failed to marshal payload")
		return nil, errors.Internal("Unable to update app key")
	}
	payloadBytesReader := bytes.NewReader(payloadBytes)

	client := &http.Client{}
	updateAppKeyReq, err := http.NewRequestWithContext(ctx, http.MethodPut, updateAppKeyUrl, payloadBytesReader)
	if err != nil {
		log.Err(err).Msg("Failed to construct updateAppKeyReq")
		return nil, errors.Internal("Unable to update app key")
	}
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		log.Err(err).Msg("Failed to get admin access token")
		return nil, errors.Internal("Failed to update app key: couldn't get admin access token")
	}
	updateAppKeyReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))
	updateAppKeyReq.Header.Add("Content-Type", "application/json")

	updateAppKeyRes, err := ctxhttp.Do(ctx, client, updateAppKeyReq)
	if err != nil {
		log.Err(err).Msg("Failed to update app key - failed to make call to gotrue")
		return nil, errors.Internal("Failed to update app key")
	}
	defer updateAppKeyRes.Body.Close()

	if updateAppKeyRes.StatusCode != http.StatusOK {
		log.Error().Int("status", updateAppKeyRes.StatusCode).Msg("Received non OK status code to update user.")
		return nil, errors.Internal("Failed to update app key")
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

func (g *gotrue) RotateAppKey(ctx context.Context, req *api.RotateAppKeyRequest) (*api.RotateAppKeyResponse, error) {
	email := fmt.Sprintf("%s%s", req.GetId(), g.AuthConfig.Gotrue.UsernameSuffix)
	updateAppKeyUrl := fmt.Sprintf("%s/admin/users/%s", g.AuthConfig.Gotrue.URL, email)

	currentSub, err := GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Couldn't resolve current sub")
		return nil, errors.Internal("Failed to update app key")
	}

	newSecret := generateClientSecret(g)

	newAppMetadata := &UserAppData{UpdatedBy: currentSub, UpdatedAt: time.Now().UnixMilli()}
	payload := make(map[string]interface{})
	payload["password"] = newSecret
	payload["app_metadata"] = newAppMetadata
	payloadBytes, err := jsoniter.Marshal(payload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal payload")
		return nil, errors.Internal("Unable to update app key")
	}

	payloadBytesReader := bytes.NewReader(payloadBytes)

	client := &http.Client{}
	updateAppKeyReq, err := http.NewRequestWithContext(ctx, http.MethodPut, updateAppKeyUrl, payloadBytesReader)
	if err != nil {
		log.Err(err).Msg("Failed to construct updateAppKeyReq")
		return nil, errors.Internal("Unable to update app key")
	}
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		log.Err(err).Msg("Failed to get admin access token")
		return nil, errors.Internal("Failed to update app key: couldn't get admin access token")
	}
	updateAppKeyReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))
	updateAppKeyReq.Header.Add("Content-Type", "application/json")

	updateAppKeyRes, err := ctxhttp.Do(ctx, client, updateAppKeyReq)
	if err != nil {
		log.Err(err).Msg("Failed to update app key - failed to make call to gotrue")
		return nil, errors.Internal("Failed to update app key")
	}
	defer updateAppKeyRes.Body.Close()

	if updateAppKeyRes.StatusCode != http.StatusOK {
		log.Error().Int("status", updateAppKeyRes.StatusCode).Msg("Received non OK status code to update user.")
		return nil, errors.Internal("Failed to update app key")
	}
	result := &api.RotateAppKeyResponse{
		AppKey: &api.AppKey{
			Id:     req.GetId(),
			Secret: newSecret,
		},
	}

	return result, nil
}

func (g *gotrue) DeleteAppKey(ctx context.Context, req *api.DeleteAppKeyRequest) (*api.DeleteAppKeyResponse, error) {
	// TODO: verify ownership

	// get admin access token
	adminAccessToken, _, err := getGotrueAdminAccessToken(ctx, g)
	if err != nil {
		return nil, err
	}

	// make external call
	deleteUserUrl := fmt.Sprintf("%s/admin/users/%s%s", g.AuthConfig.Gotrue.URL, req.GetId(), g.AuthConfig.Gotrue.UsernameSuffix)
	client := &http.Client{}
	deleteUserReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteUserUrl, nil)
	if err != nil {
		log.Err(err).Msg("Failed to form request to delete user from gotrue")
		return nil, errors.Internal("Failed to form request to delete app key")
	}
	deleteUserReq.Header.Add("Authorization", fmt.Sprintf("bearer %s", adminAccessToken))

	deleteUserRes, err := ctxhttp.Do(ctx, client, deleteUserReq)
	if err != nil {
		log.Err(err).Msg("Failed to delete user from gotrue")
		return nil, errors.Internal("Failed to delete user from gotrue")
	}

	defer deleteUserRes.Body.Close()

	if deleteUserRes.StatusCode != http.StatusOK {
		log.Error().Int("status", deleteUserRes.StatusCode).Msg("Received non OK status code to delete user")
		return nil, errors.Internal("Received non OK status code to delete user")
	}

	return &api.DeleteAppKeyResponse{
		Deleted: true,
	}, nil
}

func (g *gotrue) ListAppKeys(ctx context.Context, req *api.ListAppKeysRequest) (*api.ListAppKeysResponse, error) {
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
	getUsersUrl := fmt.Sprintf("%s/admin/users?created_by=%s&tigris_namespace=%s&tigris_project=%s&page=1&per_page=5000", g.AuthConfig.Gotrue.URL, currentSub, currentNamespace, req.GetProject())
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

	appKeys := make([]*api.AppKey, len(users))
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

		appKey := api.AppKey{
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
	return &api.ListAppKeysResponse{
		AppKeys: appKeys,
	}, nil
}

func (g *gotrue) DeleteAppKeys(ctx context.Context, project string) error {
	// TODO make it transactional on gotrue side
	listAppKeysResp, err := g.ListAppKeys(ctx, &api.ListAppKeysRequest{Project: project})
	if err != nil {
		log.Err(err).Msg("Failed to list app keys to delete them")
		return errors.Internal("Failed to delete app keys")
	}

	for _, key := range listAppKeysResp.GetAppKeys() {
		_, err := g.DeleteAppKey(ctx, &api.DeleteAppKeyRequest{
			Id:      key.Id,
			Project: key.Project,
		})
		if err != nil {
			log.Err(err).Str("clientId", key.Id).Msg("Failed to delete app key")
			return errors.Internal("Failed to delete all app keys")
		}
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

func (g *gotrue) CreateInvitations(ctx context.Context, req *api.CreateInvitationsRequest) (*api.CreateInvitationsResponse, error) {
	for _, invitation := range req.Invitations {
		err := createInvitation(ctx, invitation.GetEmail(), invitation.GetRole(), invitation.GetInvitationSentByName(), g)
		if err != nil {
			return nil, err
		}
	}
	return &api.CreateInvitationsResponse{}, nil
}

func createInvitation(ctx context.Context, email string, role string, invitationSentByName string, g *gotrue) error {
	if email == "" {
		return errors.InvalidArgument("Email must be specified")
	}
	if role == "" {
		return errors.InvalidArgument("Role must be specified")
	}

	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get namespace while creating invitation")
		return errors.Internal("Could not create user invitation")
	}

	currentSub, err := request.GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get current sub while creating invitation")
		return errors.Internal("Could not create user invitation")
	}

	expirationTime := time.Now().UnixMilli() + (g.AuthConfig.UserInvitations.ExpireAfterSec * 1000)
	createInvitationPayload := CreateInvitationPayload{
		Email:           email,
		Role:            role,
		TigrisNamespace: namespace,
		CreatedBy:       currentSub,
		CreatedByName:   invitationSentByName,
		ExpirationTime:  expirationTime,
	}
	createInvitationPayloadBytes, err := jsoniter.Marshal(createInvitationPayload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal CreateUserInvitationPayload struct to json bytes")
		return errors.Internal("Could not create user invitation")
	}

	_, err = invitationsCall(ctx, createInvitationPayloadBytes, "/invitations", http.MethodPost, g)
	if err != nil {
		log.Err(err).Msg("Failed to create user invitation")
		return errors.Internal("Could not create user invitation")
	}
	log.Debug().Str("email", email).Int64("expiration_time", expirationTime).Msg("Created user invitation")
	return nil
}

func (g *gotrue) DeleteInvitations(ctx context.Context, req *api.DeleteInvitationsRequest) (*api.DeleteInvitationsResponse, error) {
	if req.GetEmail() == "" {
		return nil, errors.InvalidArgument("Email must be specified")
	}

	if req.GetStatus() != "" {
		err := validateInvitationStatusInput(req.GetStatus())
		if err != nil {
			return nil, err
		}
	}
	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get namespace while deleting invitation")
		return nil, errors.Internal("Could not delete user invitation")
	}

	currentSub, err := request.GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get current sub while deleting invitation")
		return nil, errors.Internal("Could not delete user invitation")
	}

	deleteInvitationsPayload := DeleteInvitationsPayload{
		Email:           req.GetEmail(),
		CreatedBy:       currentSub,
		TigrisNamespace: namespace,
		Status:          req.GetStatus(),
	}
	deleteInvitationsPayloadBytes, err := json.Marshal(deleteInvitationsPayload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal delete invitation payload to json bytes")
		return nil, errors.Internal("Could not delete user invitation")
	}

	_, err = invitationsCall(ctx, deleteInvitationsPayloadBytes, "/invitations", http.MethodDelete, g)
	if err != nil {
		log.Err(err).Msg("Failed to create user invitation")
		return nil, errors.Internal("Could not create user invitation")
	}

	log.Debug().Str("email", req.GetEmail()).Str("status", req.GetStatus()).Msg("Deleted user invitation(s)")
	return &api.DeleteInvitationsResponse{}, nil
}

func (g *gotrue) ListInvitations(ctx context.Context, req *api.ListInvitationsRequest) (*api.ListInvitationsResponse, error) {
	if req.GetStatus() != "" {
		err := validateInvitationStatusInput(req.GetStatus())
		if err != nil {
			return nil, err
		}
	}
	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get namespace while listing invitations")
		return nil, errors.Internal("Could not list user invitations")
	}

	currentSub, err := request.GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get current sub while listing invitations")
		return nil, errors.Internal("Could not list user invitations")
	}
	var path string
	if req.GetStatus() == "" {
		path = fmt.Sprintf("/invitations?created_by=%s&tigris_namespace=%s", currentSub, namespace)
	} else {
		path = fmt.Sprintf("/invitations?created_by=%s&tigris_namespace=%s&status=%s", currentSub, namespace, req.GetStatus())
	}

	listInvitationsRes, err := invitationsCall(ctx, nil, path, http.MethodGet, g)
	if err != nil {
		log.Err(err).Msg("Failed to create list invitations")
		return nil, errors.Internal("Could not list user invitations")
	}

	// parse JSON response
	var invitations []*api.Invitation
	err = json.Unmarshal(listInvitationsRes, &invitations)
	if err != nil {
		log.Err(err).Msg("Failed to deserialize list user invitations response into JSON")
		return nil, errors.Internal("Could not list user invitations")
	}

	return &api.ListInvitationsResponse{Invitations: invitations}, nil
}

func (g *gotrue) VerifyInvitation(ctx context.Context, req *api.VerifyInvitationRequest) (*api.VerifyInvitationResponse, error) {
	if req.GetEmail() == "" {
		return nil, errors.InvalidArgument("Email must be specified")
	}
	if req.GetCode() == "" {
		return nil, errors.InvalidArgument("Code must be specified")
	}

	verifyInvitationPayload := VerifyInvitationPayload{
		Email: req.GetEmail(),
		Code:  req.GetCode(),
	}
	verifyInvitationPayloadBytes, err := json.Marshal(verifyInvitationPayload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal verify invitation payload to json bytes")
		return nil, errors.Internal("Could not verify user invitation")
	}
	verifyInvitationResBytes, err := invitationsCall(ctx, verifyInvitationPayloadBytes, "/invitations/verify", http.MethodPost, g)
	if err == InvalidInvitationCodeErr {
		return nil, err
	}
	if err != nil {
		log.Err(err).Msg("Failed to verify invitation")
		return nil, errors.Internal("Could not verify user invitation")
	}

	var verifyUserInvitationResponse api.VerifyInvitationResponse
	err = jsoniter.Unmarshal(verifyInvitationResBytes, &verifyUserInvitationResponse)
	if err != nil {
		log.Err(err).Msg("Failed to JSON deserialize gotrue's verify user response")
		return nil, errors.Internal("Could not verify user invitation")
	}
	return &api.VerifyInvitationResponse{
		TigrisNamespace: verifyUserInvitationResponse.GetTigrisNamespace(),
		Role:            verifyUserInvitationResponse.GetRole(),
	}, nil
}

func invitationsCall(ctx context.Context, payload []byte, path string, method string, g *gotrue) ([]byte, error) {
	payloadReader := bytes.NewReader(payload)

	client := &http.Client{}
	invitationReq, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s%s", g.AuthConfig.Gotrue.URL, path), payloadReader)
	if err != nil {
		log.Err(err).Msgf("Failed to create invitation request for path: %s", path)
		return nil, err
	}
	invitationReq.Header.Add("X-JWT-AUD", g.AuthConfig.PrimaryAudience)
	invitationReq.Header.Add("Content-Type", "application/json")

	invitationRes, err := client.Do(invitationReq)
	if err != nil {
		log.Err(err).Msgf("Failed to create invitation request for path: %s", path)
		return nil, err
	}
	defer invitationRes.Body.Close()

	if invitationRes.StatusCode != http.StatusOK {
		if invitationRes.StatusCode == http.StatusUnauthorized {
			return nil, InvalidInvitationCodeErr
		} else {
			log.Error().Int("status", invitationRes.StatusCode).Msgf("Received non OK status from gotrue while performing invitation operation at path: %s", path)
			return nil, errors.Internal("Received non OK status")
		}
	}

	invitationResBody, err := io.ReadAll(invitationRes.Body)
	if err != nil {
		log.Err(err).Msgf("Failed to read invitationResBody body for operation at path: %s", path)
		return nil, errors.Internal("Failed to read invitationResBody body for operation at path: %s", path)
	}
	defer invitationRes.Body.Close()

	return invitationResBody, nil
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
	var getTokenJsonMap map[string]interface{}
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
