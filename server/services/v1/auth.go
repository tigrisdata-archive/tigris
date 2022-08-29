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
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/auth0/go-auth0/management"
	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
)

const (
	authPattern       = "/auth/*"
	refreshToken      = "refresh_token"
	scope             = "offline_access openid"
	createdBy         = "created_by"
	createdAt         = "created_at"
	tigrisNamespace   = "tigris_namespace"
	clientCredentials = "client_credentials"
	auth0             = "auth0"
)

type authService struct {
	api.UnimplementedAuthServer
	OAuthProvider
}

type OAuthProvider interface {
	GetAccessToken(req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error)
	CreateApplication(ctx context.Context, req *api.CreateApplicationRequest) (*api.CreateApplicationResponse, error)
	RotateApplicationSecret(ctx context.Context, req *api.RotateApplicationSecretRequest) (*api.RotateApplicationSecretResponse, error)
	DeleteApplication(ctx context.Context, req *api.DeleteApplicationsRequest) (*api.DeleteApplicationResponse, error)
	ListApplications(ctx context.Context, req *api.ListApplicationsRequest) (*api.ListApplicationsResponse, error)
}

type Auth0 struct {
	authConfig config.AuthConfig
	management *management.Management
}

func (a *Auth0) managementToTigrisErrorCode(err error) api.Code {
	managementError, ok := err.(management.Error)
	if !ok {
		return api.Code_INTERNAL
	}
	return api.FromHttpCode(managementError.Status())
}

func (a *Auth0) GetAccessToken(req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error) {
	switch req.GrantType {
	case api.GrantType_REFRESH_TOKEN:
		return getAccessTokenUsingRefreshToken(req, a)
	case api.GrantType_CLIENT_CREDENTIALS:
		return getAccessTokenUsingClientCredentials(req, a)
	}
	return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "Failed to GetAccessToken: reason = unsupported grant_type, it has to be one of [refresh_token, client_credentials]")
}

func (a *Auth0) CreateApplication(ctx context.Context, req *api.CreateApplicationRequest) (*api.CreateApplicationResponse, error) {
	currentSub, err := getCurrentSub(ctx)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to list applications: reason = %s", err.Error())
	}
	currentNamespace, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to list applications: reason = %s", err.Error())
	}

	nonInteractiveApp := "non_interactive"
	metadata := map[string]string{}
	metadata[createdAt] = time.Now().Format(time.RFC3339)
	metadata[createdBy] = currentSub
	metadata[tigrisNamespace] = currentNamespace

	c := &management.Client{
		Name:           &req.Name,
		Description:    &req.Description,
		AppType:        &nonInteractiveApp,
		ClientMetadata: metadata,
	}

	err = a.management.Client.Create(c)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to create application: reason = %s", err.Error())
	}

	// grant this client to access current service as audience
	grant := &management.ClientGrant{
		ClientID: c.ClientID,
		Audience: &a.authConfig.Audience,
		Scope:    []interface{}{},
	}

	err = a.management.ClientGrant.Create(grant)
	if err != nil {
		log.Warn().Str("app_id", c.GetClientID()).Msg("Failed to create application grant")
		// delete this app and clean up
		err := a.management.Client.Delete(c.GetClientID())
		if err != nil {
			log.Warn().Msgf("Failed to cleanup half-created app with clientId=%s, reason=%s", c.GetClientID(), err.Error())
		}
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to create application grant: reason = %s", err.Error())
	}

	createdApp := &api.Application{
		Name:        c.GetName(),
		Description: c.GetDescription(),
		Id:          c.GetClientID(),
		Secret:      c.GetClientSecret(),
		CreatedBy:   c.GetClientMetadata()[createdBy],
		CreatedAt:   readCreatedAt(c),
	}
	return &api.CreateApplicationResponse{
		CreatedApplication: createdApp,
	}, nil
}

func (a *Auth0) DeleteApplication(ctx context.Context, req *api.DeleteApplicationsRequest) (*api.DeleteApplicationResponse, error) {
	// validate
	client, err := a.management.Client.Read(req.GetId())
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to delete application: reason = %s", err.Error())
	}

	// check ownership before deleting
	currentSub, err := getCurrentSub(ctx)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to delete application: reason = %s", err.Error())
	}
	if client.GetClientMetadata()[createdBy] != currentSub {
		return nil, api.Errorf(api.Code_PERMISSION_DENIED, "Failed to delete application: reason = You cannot delete application that is not created by you.")
	}

	err = a.management.Client.Delete(req.GetId())
	if err != nil {
		log.Warn().Msgf("Failed to cleanup half-created app with clientId=%s, reason=%s", req.GetId(), err.Error())
		return nil, err
	}
	return &api.DeleteApplicationResponse{
		Deleted: true,
	}, nil
}

func (a *Auth0) RotateApplicationSecret(ctx context.Context, req *api.RotateApplicationSecretRequest) (*api.RotateApplicationSecretResponse, error) {
	// validate
	client, err := a.management.Client.Read(req.GetId())
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to rotate application secret: reason = %s", err.Error())
	}

	// check ownership before rotating
	currentSub, err := getCurrentSub(ctx)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to delete application: reason = %s", err.Error())
	}
	if client.GetClientMetadata()[createdBy] != currentSub {
		return nil, api.Errorf(api.Code_PERMISSION_DENIED, "Failed to rotate application secret: reason = You cannot rotate secret for application that is not created by you.")
	}

	updatedApp, err := a.management.Client.RotateSecret(req.GetId())
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to rotate application secret: reason = %s", err.Error())
	}
	return &api.RotateApplicationSecretResponse{
		Application: &api.Application{
			Id:          updatedApp.GetClientID(),
			Name:        updatedApp.GetName(),
			Description: updatedApp.GetDescription(),
			Secret:      updatedApp.GetClientSecret(),
			CreatedAt:   readCreatedAt(updatedApp),
			CreatedBy:   updatedApp.GetClientMetadata()[createdBy],
		},
	}, nil
}

func (a Auth0) ListApplications(ctx context.Context, _ *api.ListApplicationsRequest) (*api.ListApplicationsResponse, error) {

	appList, err := a.management.Client.List(management.IncludeFields("client_id", "client_metadata", "client_secret", "description", "name"))
	if err != nil {
		return nil, api.Errorf(a.managementToTigrisErrorCode(err), "Failed to list applications: reason = %s", err.Error())
	}

	currentSub, err := getCurrentSub(ctx)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to list applications: reason = %s", err.Error())
	}

	var apps []*api.Application
	for _, client := range appList.Clients {
		// filter for this user's apps
		if client.GetClientMetadata()[createdBy] == currentSub {
			app := &api.Application{
				Name:        client.GetName(),
				Description: client.GetDescription(),
				Id:          client.GetClientID(),
				Secret:      client.GetClientSecret(),
				CreatedAt:   readCreatedAt(client),
				CreatedBy:   client.GetClientMetadata()[createdBy],
			}
			apps = append(apps, app)
		}
	}
	return &api.ListApplicationsResponse{
		Applications: apps,
	}, nil
}

func newAuthService() *authService {
	if config.DefaultConfig.Auth.OAuthProvider == auth0 {
		m, err := management.New(config.DefaultConfig.Auth.ExternalDomain, management.WithClientCredentials(config.DefaultConfig.Auth.ManagementClientId, config.DefaultConfig.Auth.ManagementClientSecret))
		if err != nil {
			if config.DefaultConfig.Auth.EnableOauth {
				panic("Unable to configure external oauth provider")
			}
		}
		return &authService{
			OAuthProvider: &Auth0{
				authConfig: config.DefaultConfig.Auth,
				management: m,
			},
		}
	}
	if config.DefaultConfig.Auth.EnableOauth {
		log.Error().Str("OAuthProvider", config.DefaultConfig.Auth.OAuthProvider).Msg("Unable to configure external oauth provider")
		panic("Unable to configure external oauth provider")
	}
	return nil
}

func (a *authService) GetAccessToken(_ context.Context, req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error) {
	return a.OAuthProvider.GetAccessToken(req)
}

func (a *authService) CreateApplication(ctx context.Context, req *api.CreateApplicationRequest) (*api.CreateApplicationResponse, error) {
	return a.OAuthProvider.CreateApplication(ctx, req)
}

func (a *authService) DeleteApplication(ctx context.Context, req *api.DeleteApplicationsRequest) (*api.DeleteApplicationResponse, error) {
	return a.OAuthProvider.DeleteApplication(ctx, req)
}

func (a *authService) ListApplications(ctx context.Context, req *api.ListApplicationsRequest) (*api.ListApplicationsResponse, error) {
	return a.OAuthProvider.ListApplications(ctx, req)
}

func (a *authService) RotateApplicationSecret(ctx context.Context, req *api.RotateApplicationSecretRequest) (*api.RotateApplicationSecretResponse, error) {
	return a.OAuthProvider.RotateApplicationSecret(ctx, req)
}

func (a *authService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterAuthHandlerClient(context.TODO(), mux, api.NewAuthClient(inproc)); err != nil {
		return err
	}
	api.RegisterAuthServer(inproc, a)
	router.HandleFunc(authPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (h *authService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterAuthServer(grpc, h)
	return nil
}

func getCurrentSub(ctx context.Context) (string, error) {
	// further filter for this particular user
	token, err := request.GetAccessToken(ctx)
	if err != nil {
		return "", api.Errorf(api.Code_INTERNAL, "Failed to retrieve current sub: reason = %s", err.Error())
	}
	return token.Sub, nil
}

func getAccessTokenUsingRefreshToken(req *api.GetAccessTokenRequest, a *Auth0) (*api.GetAccessTokenResponse, error) {
	data := url.Values{
		"refresh_token": {req.RefreshToken},
		"client_id":     {a.authConfig.ClientId},
		"grant_type":    {refreshToken},
		"scope":         {scope},
	}
	resp, err := http.PostForm(a.authConfig.ExternalTokenURL, data)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = %s", err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = %s", err.Error())
	}
	bodyStr := string(body)
	if resp.StatusCode == 200 {
		getAccessTokenResponse := api.GetAccessTokenResponse{}

		err = json.Unmarshal([]byte(bodyStr), &getAccessTokenResponse)
		if err != nil {
			return nil, api.Errorf(api.Code_INTERNAL, "Failed to parse external response: reason = %s", err.Error())
		}
		return &getAccessTokenResponse, nil
	}
	log.Error().Msgf("Auth0 response status code=%d", resp.StatusCode)
	return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = %s", bodyStr)
}

func getAccessTokenUsingClientCredentials(req *api.GetAccessTokenRequest, a *Auth0) (*api.GetAccessTokenResponse, error) {
	payload := map[string]string{}
	payload["client_id"] = req.ClientId
	payload["client_secret"] = req.ClientSecret
	payload["audience"] = a.authConfig.Audience
	payload["grant_type"] = clientCredentials
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = %s", err.Error())
	}

	resp, err := http.Post(a.authConfig.ExternalTokenURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = %s", err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = %s", err.Error())
	}
	bodyStr := string(body)
	if resp.StatusCode == 200 {
		getAccessTokenResponse := api.GetAccessTokenResponse{}

		err = json.Unmarshal([]byte(bodyStr), &getAccessTokenResponse)
		if err != nil {
			return nil, api.Errorf(api.Code_INTERNAL, "Failed to parse external response: reason = %s", err.Error())
		}
		return &getAccessTokenResponse, nil
	}
	log.Error().Msgf("Auth0 response status code=%d", resp.StatusCode)
	return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = %s", bodyStr)
}

// helper to read created_at metadata from client application
func readCreatedAt(c *management.Client) int64 {
	result, err := date.ToUnixMilli(time.RFC3339, c.GetClientMetadata()[createdAt])
	if err != nil {
		log.Warn().Msgf("%s field was not parsed to int64", c.GetClientMetadata()[createdAt])
		result = -1
	}
	return result
}
