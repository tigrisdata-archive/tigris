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
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"google.golang.org/grpc"
)

const (
	oauthPattern = "/oauth/*"
	refreshToken = "refresh_token"
	scope        = "offline_access openid"
)

type authService struct {
	api.UnimplementedAuthServer
	OAuthProvider
}

type OAuthProvider interface {
	GetAccessToken(request *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error)
}

type Auth0 struct {
	config.AuthConfig
}

func (a Auth0) GetAccessToken(req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error) {
	data := url.Values{
		"refresh_token": {req.RefreshToken},
		"client_id":     {a.AuthConfig.ClientId},
		"grant_type":    {refreshToken},
		"scope":         {scope},
	}
	resp, err := http.PostForm(a.AuthConfig.ExternalTokenURL, data)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = "+err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = "+err.Error())
	}
	bodyStr := string(body)
	if resp.StatusCode == 200 {
		getAccessTokenResponse := api.GetAccessTokenResponse{}

		err = json.Unmarshal([]byte(bodyStr), &getAccessTokenResponse)
		if err != nil {
			return nil, api.Errorf(api.Code_INTERNAL, "Failed to parse external response: reason = "+err.Error())
		}
		return &getAccessTokenResponse, nil
	}
	log.Error().Msgf("Auth0 response status code=%d", resp.StatusCode)
	return nil, api.Errorf(api.Code_INTERNAL, "Failed to get access token: reason = "+bodyStr)
}

func newAuthService() *authService {
	if config.DefaultConfig.Auth.OAuthProvider == "auth0" {
		return &authService{
			OAuthProvider: Auth0{
				config.DefaultConfig.Auth,
			},
		}
	}
	log.Error().Str("OAuthProvider", config.DefaultConfig.Auth.OAuthProvider).Msg("Unable to configure external oauth provider")
	if config.DefaultConfig.Auth.EnableOauth {
		panic("Unable to configure external oauth provider")
	}
	return nil
}

func (a *authService) GetAccessToken(_ context.Context, req *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error) {
	return a.OAuthProvider.GetAccessToken(req)
}

func (a *authService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterAuthHandlerClient(context.TODO(), mux, api.NewAuthClient(inproc)); err != nil {
		return err
	}
	api.RegisterAuthServer(inproc, a)
	router.HandleFunc(oauthPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (h *authService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterAuthServer(grpc, h)
	return nil
}
