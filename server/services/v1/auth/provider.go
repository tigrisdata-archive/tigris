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
	"net/http"
	"time"

	"github.com/auth0/go-auth0/management"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
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
	tigrisProject      = "tigris_projects"
	clientCredentials  = "client_credentials"
	defaultNamespaceId = 1
	// pagination param for list clients auth0 call.
	perPage = 50

	auth0Name  = "auth0"
	gotrueName = "gotrue"
)

type Provider interface {
	GetAccessToken(ctx context.Context, req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error)
	CreateAppKey(ctx context.Context, req *api.CreateAppKeyRequest) (*api.CreateAppKeyResponse, error)
	UpdateAppKey(ctx context.Context, req *api.UpdateAppKeyRequest) (*api.UpdateAppKeyResponse, error)
	RotateAppKey(ctx context.Context, req *api.RotateAppKeyRequest) (*api.RotateAppKeyResponse, error)
	DeleteAppKey(ctx context.Context, req *api.DeleteAppKeyRequest) (*api.DeleteAppKeyResponse, error)
	ListAppKeys(ctx context.Context, req *api.ListAppKeysRequest) (*api.ListAppKeysResponse, error)
	DeleteAppKeys(ctx context.Context, project string) error

	CreateInvitations(ctx context.Context, req *api.CreateInvitationsRequest) (*api.CreateInvitationsResponse, error)
	DeleteInvitations(ctx context.Context, req *api.DeleteInvitationsRequest) (*api.DeleteInvitationsResponse, error)
	ListInvitations(ctx context.Context, req *api.ListInvitationsRequest) (*api.ListInvitationsResponse, error)
	VerifyInvitation(ctx context.Context, req *api.VerifyInvitationRequest) (*api.VerifyInvitationResponse, error)
}

func NewProvider(userstore *metadata.UserSubspace, txMgr *transaction.Manager) Provider {
	var authProvider Provider = &noop{}

	if config.DefaultConfig.Auth.OAuthProvider == auth0Name {
		auth0HttpClient := &http.Client{Timeout: time.Duration(30) * time.Second}
		m, err := management.New(config.DefaultConfig.Auth.ExternalDomain,
			management.WithClientCredentials(config.DefaultConfig.Auth.ManagementClientId, config.DefaultConfig.Auth.ManagementClientSecret),
			management.WithClient(auth0HttpClient))
		if err != nil {
			if config.DefaultConfig.Auth.EnableOauth {
				panic("Unable to configure external oauth provider")
			}
		}

		authProvider = &auth0{
			AuthConfig: config.DefaultConfig.Auth,
			Management: m,
			userStore:  userstore,
			txMgr:      txMgr,
		}
	} else if config.DefaultConfig.Auth.OAuthProvider == gotrueName {
		authProvider = &gotrue{
			AuthConfig: config.DefaultConfig.Auth,
		}
	}

	return authProvider
}
