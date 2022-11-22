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

package auth

import (
	"context"

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

	auth0name = "auth0"
)

type Provider interface {
	GetAccessToken(ctx context.Context, req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error)
	CreateApplication(ctx context.Context, req *api.CreateApplicationRequest) (*api.CreateApplicationResponse, error)
	UpdateApplication(ctx context.Context, req *api.UpdateApplicationRequest) (*api.UpdateApplicationResponse, error)
	RotateApplicationSecret(ctx context.Context, req *api.RotateApplicationSecretRequest) (*api.RotateApplicationSecretResponse, error)
	DeleteApplication(ctx context.Context, req *api.DeleteApplicationsRequest) (*api.DeleteApplicationResponse, error)
	ListApplications(ctx context.Context, req *api.ListApplicationsRequest) (*api.ListApplicationsResponse, error)
}

func NewProvider(userstore *metadata.UserSubspace, txMgr *transaction.Manager) Provider {
	var authProvider Provider = &noop{}

	if config.DefaultConfig.Auth.OAuthProvider == auth0name {
		m, err := management.New(config.DefaultConfig.Auth.ExternalDomain, management.WithClientCredentials(config.DefaultConfig.Auth.ManagementClientId, config.DefaultConfig.Auth.ManagementClientSecret))
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
	}

	return authProvider
}
