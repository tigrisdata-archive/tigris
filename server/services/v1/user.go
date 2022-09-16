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
	"net/http"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"google.golang.org/grpc"
)

const (
	userPattern = "/" + version + "/users/*"
)

type userService struct {
	api.UnimplementedUserServer
	AuthProvider
	UserMetadataProvider
}

func newUserService(authProvider AuthProvider, txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, userstore *metadata.UserSubspace) *userService {
	if authProvider == nil && config.DefaultConfig.Auth.EnableOauth {
		log.Error().Str("AuthProvider", config.DefaultConfig.Auth.OAuthProvider).Msg("Unable to configure external auth provider")
		panic("Unable to configure external auth provider")
	}

	userMetadataProvider := &DefaultUserMetadataProvider{
		userStore: userstore,
		txMgr:     txMgr,
		tenantMgr: tenantMgr,
	}
	return &userService{
		AuthProvider:         authProvider,
		UserMetadataProvider: userMetadataProvider,
	}
}

func (a *userService) CreateApplication(ctx context.Context, req *api.CreateApplicationRequest) (*api.CreateApplicationResponse, error) {
	return a.AuthProvider.CreateApplication(ctx, req)
}

func (a *userService) UpdateApplication(ctx context.Context, req *api.UpdateApplicationRequest) (*api.UpdateApplicationResponse, error) {
	return a.AuthProvider.UpdateApplication(ctx, req)
}

func (a *userService) DeleteApplication(ctx context.Context, req *api.DeleteApplicationsRequest) (*api.DeleteApplicationResponse, error) {
	return a.AuthProvider.DeleteApplication(ctx, req)
}

func (a *userService) ListApplications(ctx context.Context, req *api.ListApplicationsRequest) (*api.ListApplicationsResponse, error) {
	return a.AuthProvider.ListApplications(ctx, req)
}

func (a *userService) RotateApplicationSecret(ctx context.Context, req *api.RotateApplicationSecretRequest) (*api.RotateApplicationSecretResponse, error) {
	return a.AuthProvider.RotateApplicationSecret(ctx, req)
}

func (a *userService) GetUserMetadata(ctx context.Context, req *api.GetUserMetadataRequest) (*api.GetUserMetadataResponse, error) {
	return a.UserMetadataProvider.GetUserMetadata(ctx, req)
}

func (a *userService) InsertUserMetadata(ctx context.Context, req *api.InsertUserMetadataRequest) (*api.InsertUserMetadataResponse, error) {
	return a.UserMetadataProvider.InsertUserMetadata(ctx, req)
}

func (a *userService) UpdateUserMetadata(ctx context.Context, req *api.UpdateUserMetadataRequest) (*api.UpdateUserMetadataResponse, error) {
	return a.UserMetadataProvider.UpdateUserMetadata(ctx, req)
}

func (a *userService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterUserHandlerClient(context.TODO(), mux, api.NewUserClient(inproc)); err != nil {
		return err
	}
	api.RegisterUserServer(inproc, a)
	router.HandleFunc(userPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (h *userService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterUserServer(grpc, h)
	return nil
}
