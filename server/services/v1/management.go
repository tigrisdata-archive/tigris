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
	"fmt"
	"net/http"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"google.golang.org/grpc"
)

const (
	userPattern = "/" + version + "/management/*"
)

type managementService struct {
	api.UnimplementedManagementServer
	AuthProvider
	UserMetadataProvider
	*transaction.Manager
	*metadata.TenantManager
}

func newManagementService(authProvider AuthProvider, txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, userstore *metadata.UserSubspace) *managementService {
	if authProvider == nil && config.DefaultConfig.Auth.EnableOauth {
		log.Error().Str("AuthProvider", config.DefaultConfig.Auth.OAuthProvider).Msg("Unable to configure external auth provider")
		panic("Unable to configure external auth provider")
	}

	userMetadataProvider := &DefaultUserMetadataProvider{
		userStore: userstore,
		txMgr:     txMgr,
		tenantMgr: tenantMgr,
	}
	return &managementService{
		AuthProvider:         authProvider,
		UserMetadataProvider: userMetadataProvider,
		Manager:              txMgr,
		TenantManager:        tenantMgr,
	}
}

func (m *managementService) CreateNamespace(ctx context.Context, req *api.CreateNamespaceRequest) (*api.CreateNamespaceResponse, error) {
	if req.GetDisplayName() == "" {
		return nil, errors.InvalidArgument("Empty namespace display name is not allowed")
	}

	id := uint32(req.GetId())
	name := uuid.New().String()

	tx, err := m.Manager.StartTx(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to create namespace")
	}

	if id == 0 {
		namespaces, err := m.TenantManager.ListNamespaces(ctx, tx)
		if err != nil {
			return nil, errors.Internal("Failed to generate ID")
		}
		var maxId uint32 = 0
		for _, namespace := range namespaces {
			if maxId < namespace.Id() {
				maxId = namespace.Id()
			}
		}
		id = maxId + 1
	}
	namespace := metadata.NewTenantNamespace(name, metadata.NewNamespaceMetadata(id, name, req.GetDisplayName()))
	_, err = m.TenantManager.CreateTenant(ctx, tx, namespace)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	} else {
		if err = tx.Commit(ctx); err == nil {
			return &api.CreateNamespaceResponse{
				Status:  "CREATED",
				Message: "Namespace created, with id=" + fmt.Sprint(id) + ", and name=" + name,
			}, nil
		} else {
			return nil, err
		}
	}
}

func (m *managementService) ListNamespaces(ctx context.Context, _ *api.ListNamespacesRequest) (*api.ListNamespacesResponse, error) {
	tx, err := m.Manager.StartTx(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to begin transaction")
	}
	namespaces, err := m.TenantManager.ListNamespaces(ctx, tx)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	_ = tx.Commit(ctx)
	if namespaces == nil {
		return &api.ListNamespacesResponse{
			Namespaces: nil,
		}, nil
	}

	namespacesInfo := make([]*api.NamespaceInfo, 0, len(namespaces))
	for _, namespace := range namespaces {
		namespacesInfo = append(namespacesInfo, &api.NamespaceInfo{
			Id:          int32(namespace.Id()),
			Name:        namespace.Name(),
			DisplayName: namespace.Metadata().DisplayName,
		})
	}
	return &api.ListNamespacesResponse{
		Namespaces: namespacesInfo,
	}, nil
}

func (m *managementService) CreateApplication(ctx context.Context, req *api.CreateApplicationRequest) (*api.CreateApplicationResponse, error) {
	return m.AuthProvider.CreateApplication(ctx, req)
}

func (m *managementService) UpdateApplication(ctx context.Context, req *api.UpdateApplicationRequest) (*api.UpdateApplicationResponse, error) {
	return m.AuthProvider.UpdateApplication(ctx, req)
}

func (m *managementService) DeleteApplication(ctx context.Context, req *api.DeleteApplicationsRequest) (*api.DeleteApplicationResponse, error) {
	return m.AuthProvider.DeleteApplication(ctx, req)
}

func (m *managementService) ListApplications(ctx context.Context, req *api.ListApplicationsRequest) (*api.ListApplicationsResponse, error) {
	return m.AuthProvider.ListApplications(ctx, req)
}

func (m *managementService) RotateApplicationSecret(ctx context.Context, req *api.RotateApplicationSecretRequest) (*api.RotateApplicationSecretResponse, error) {
	return m.AuthProvider.RotateApplicationSecret(ctx, req)
}

func (m *managementService) GetUserMetadata(ctx context.Context, req *api.GetUserMetadataRequest) (*api.GetUserMetadataResponse, error) {
	return m.UserMetadataProvider.GetUserMetadata(ctx, req)
}

func (m *managementService) InsertUserMetadata(ctx context.Context, req *api.InsertUserMetadataRequest) (*api.InsertUserMetadataResponse, error) {
	return m.UserMetadataProvider.InsertUserMetadata(ctx, req)
}

func (m *managementService) UpdateUserMetadata(ctx context.Context, req *api.UpdateUserMetadataRequest) (*api.UpdateUserMetadataResponse, error) {
	return m.UserMetadataProvider.UpdateUserMetadata(ctx, req)
}

func (m *managementService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterManagementHandlerClient(context.TODO(), mux, api.NewManagementClient(inproc)); err != nil {
		return err
	}
	api.RegisterManagementServer(inproc, m)
	router.HandleFunc(userPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (m *managementService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterManagementServer(grpc, m)
	return nil
}
