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
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/auth0/go-auth0/management"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
)

const (
	CreatedStatus = "created"
	DeletedStatus = "deleted"
)

var InvalidInvitationCodeErr = errors.Unauthenticated("Failed to verify invitation code")

type UsersManager interface {
	CreateInvitations(ctx context.Context, req *api.CreateInvitationsRequest) (*api.CreateInvitationsResponse, error)
	DeleteInvitations(ctx context.Context, req *api.DeleteInvitationsRequest) (*api.DeleteInvitationsResponse, error)
	ListInvitations(ctx context.Context, req *api.ListInvitationsRequest) (*api.ListInvitationsResponse, error)
	VerifyInvitation(ctx context.Context, req *api.VerifyInvitationRequest) (*api.VerifyInvitationResponse, error)
	ListUsers(ctx context.Context, req *api.ListUsersRequest) (*api.ListUsersResponse, error)
}

type DefaultUsersManager struct {
	Management    *management.Management
	TenantManager *metadata.TenantManager
}

func NewDefaultUsersManager(tm *metadata.TenantManager) *DefaultUsersManager {
	auth0HttpClient := &http.Client{Timeout: time.Duration(30) * time.Second}
	m, err := management.New(config.DefaultConfig.Auth.ExternalDomain,
		management.WithClientCredentials(config.DefaultConfig.Auth.ManagementClientId, config.DefaultConfig.Auth.ManagementClientSecret),
		management.WithClient(auth0HttpClient))
	if err != nil {
		if config.DefaultConfig.Auth.EnableOauth {
			log.Err(err).Msg("Failed to configure auth0 management client")
			panic("Unable to configure default users manager")
		}
	}
	return &DefaultUsersManager{Management: m, TenantManager: tm}
}

func (um *DefaultUsersManager) CreateInvitations(ctx context.Context, req *api.CreateInvitationsRequest) (*api.CreateInvitationsResponse, error) {
	if !config.DefaultConfig.Auth.UserInvitations.Enabled {
		return nil, errors.Unimplemented("User invitation is not enabled.")
	}
	for _, invitation := range req.Invitations {
		err := createInvitation(ctx, invitation.GetEmail(), invitation.GetRole(), invitation.GetInvitationSentByName(), um)
		if err != nil {
			return nil, err
		}
	}
	return &api.CreateInvitationsResponse{
		Status:  CreatedStatus,
		Message: "Invitation(s) created successfully",
		Count:   int32(len(req.Invitations)),
	}, nil
}

func createInvitation(ctx context.Context, email string, role string, invitationSentByName string, um *DefaultUsersManager) error {
	if email == "" {
		return errors.InvalidArgument("Email must be specified")
	}
	if role == "" {
		return errors.InvalidArgument("Role must be specified")
	}

	if !(role == OwnerRoleName || role == EditorRoleName || role == ReadOnlyRoleName) {
		return errors.InvalidArgument("Supported roles are  [editor(e), readonly(ro), owner(o)]")
	}

	err := validateRole(ctx, role)
	if err != nil {
		return err
	}

	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get namespace while creating invitation")
		return errors.Internal("Could not create user invitation")
	}

	namespaceName, err := um.TenantManager.GetNamespaceName(ctx, namespace)
	if err != nil {
		log.Err(err).Msg("Failed to get namespace name while creating invitation")
		return errors.Internal("Could not create user invitation")
	}

	currentSub, err := request.GetCurrentSub(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get current sub while creating invitation")
		return errors.Internal("Could not create user invitation")
	}

	expirationTime := time.Now().UnixMilli() + (config.DefaultConfig.Auth.UserInvitations.ExpireAfterSec * 1000)
	createInvitationPayload := CreateInvitationPayload{
		Email:               email,
		Role:                role,
		TigrisNamespace:     namespace,
		TigrisNamespaceName: namespaceName,
		CreatedBy:           currentSub,
		CreatedByName:       invitationSentByName,
		ExpirationTime:      expirationTime,
	}
	createInvitationPayloadBytes, err := jsoniter.Marshal(createInvitationPayload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal CreateUserInvitationPayload struct to json bytes")
		return errors.Internal("Could not create user invitation")
	}

	_, err = invitationsCall(ctx, createInvitationPayloadBytes, "/invitations", http.MethodPost)
	if err != nil {
		log.Err(err).Msg("Failed to create user invitation")
		return errors.Internal("Could not create user invitation")
	}
	log.Debug().Str("email", email).Int64("expiration_time", expirationTime).Msg("Created user invitation")
	return nil
}

func (*DefaultUsersManager) DeleteInvitations(ctx context.Context, req *api.DeleteInvitationsRequest) (*api.DeleteInvitationsResponse, error) {
	if !config.DefaultConfig.Auth.UserInvitations.Enabled {
		return nil, errors.Unimplemented("User invitation is not enabled.")
	}
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
	deleteInvitationsPayloadBytes, err := jsoniter.Marshal(deleteInvitationsPayload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal delete invitation payload to json bytes")
		return nil, errors.Internal("Could not delete user invitation")
	}

	_, err = invitationsCall(ctx, deleteInvitationsPayloadBytes, "/invitations", http.MethodDelete)
	if err != nil {
		log.Err(err).Msg("Failed to create user invitation")
		return nil, errors.Internal("Could not create user invitation")
	}

	log.Debug().Str("email", req.GetEmail()).Str("status", req.GetStatus()).Msg("Deleted user invitation(s)")
	return &api.DeleteInvitationsResponse{
		Status:  DeletedStatus,
		Message: "Invitation(s) deleted successfully",
	}, nil
}

func (*DefaultUsersManager) ListInvitations(ctx context.Context, req *api.ListInvitationsRequest) (*api.ListInvitationsResponse, error) {
	if !config.DefaultConfig.Auth.UserInvitations.Enabled {
		return nil, errors.Unimplemented("User invitation is not enabled.")
	}
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

	listInvitationsRes, err := invitationsCall(ctx, nil, path, http.MethodGet)
	if err != nil {
		log.Err(err).Msg("Failed to create list invitations")
		return nil, errors.Internal("Could not list user invitations")
	}

	// parse JSON response
	var invitations []*api.Invitation
	err = jsoniter.Unmarshal(listInvitationsRes, &invitations)
	if err != nil {
		log.Err(err).Msg("Failed to deserialize list user invitations response into JSON")
		return nil, errors.Internal("Could not list user invitations")
	}

	return &api.ListInvitationsResponse{Invitations: invitations}, nil
}

func (*DefaultUsersManager) VerifyInvitation(ctx context.Context, req *api.VerifyInvitationRequest) (*api.VerifyInvitationResponse, error) {
	if !config.DefaultConfig.Auth.UserInvitations.Enabled {
		return nil, errors.Unimplemented("User invitation is not enabled.")
	}
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
	if req.Dry != nil {
		verifyInvitationPayload.Dry = req.GetDry()
	} else {
		verifyInvitationPayload.Dry = false
	}
	verifyInvitationPayloadBytes, err := jsoniter.Marshal(verifyInvitationPayload)
	if err != nil {
		log.Err(err).Msg("Failed to marshal verify invitation payload to json bytes")
		return nil, errors.Internal("Could not verify user invitation")
	}
	verifyInvitationResBytes, err := invitationsCall(ctx, verifyInvitationPayloadBytes, "/invitations/verify", http.MethodPost)
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
	return &verifyUserInvitationResponse, nil
}

func (um *DefaultUsersManager) ListUsers(ctx context.Context, _ *api.ListUsersRequest) (*api.ListUsersResponse, error) {
	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get namespace while listing invitations")
		return nil, errors.Internal("Could not list users")
	}
	queryStr := fmt.Sprintf("app_metadata.accessibleNamespaces.code:%s", namespace)
	users, err := um.Management.User.Search(management.Query(queryStr))
	if err != nil {
		log.Err(err).Msg("Failed to get list of users from auth0")
		return nil, errors.Internal("Could not list users")
	}

	usersRes := make([]*api.User, len(users.Users))
	for i, user := range users.Users {
		usersRes[i] = &api.User{
			Email:     user.GetEmail(),
			Name:      user.GetName(),
			CreatedAt: user.GetCreatedAt().UnixMilli(),
			Picture:   user.GetPicture(),
		}
	}
	return &api.ListUsersResponse{Users: usersRes}, nil
}

func invitationsCall(ctx context.Context, payload []byte, path string, method string) ([]byte, error) {
	payloadReader := bytes.NewReader(payload)

	client := &http.Client{}
	invitationReq, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s%s", config.DefaultConfig.Auth.Gotrue.URL, path), payloadReader)
	if err != nil {
		log.Err(err).Msgf("Failed to create invitation request for path: %s", path)
		return nil, err
	}
	invitationReq.Header.Add("X-JWT-AUD", config.DefaultConfig.Auth.PrimaryAudience)
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
		}
		log.Error().Int("status", invitationRes.StatusCode).Msgf("Received non OK status from gotrue while performing invitation operation at path: %s", path)
		return nil, errors.Internal("Received non OK status")
	}

	invitationResBody, err := io.ReadAll(invitationRes.Body)
	if err != nil {
		log.Err(err).Msgf("Failed to read invitationResBody body for operation at path: %s", path)
		return nil, errors.Internal("Failed to read invitationResBody body for operation at path: %s", path)
	}

	return invitationResBody, nil
}
