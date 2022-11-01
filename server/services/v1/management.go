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
	"fmt"
	"net/http"
	"strconv"

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
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc"
)

const (
	userPattern = "/" + version + "/management/*"
)

type managementService struct {
	api.UnimplementedManagementServer
	AuthProvider
	UserMetadataProvider
	NamespaceMetadataProvider
	*transaction.Manager
	*metadata.TenantManager
}

type nsDetailsResp = map[string]map[string]map[string]map[string]string

func newManagementService(authProvider AuthProvider, txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, userStore *metadata.UserSubspace, namespaceStore *metadata.NamespaceSubspace) *managementService {
	if authProvider == nil && config.DefaultConfig.Auth.EnableOauth {
		log.Error().Str("AuthProvider", config.DefaultConfig.Auth.OAuthProvider).Msg("Unable to configure external auth provider")
		panic("Unable to configure external auth provider")
	}

	userMetadataProvider := &DefaultUserMetadataProvider{
		userStore: userStore,
		txMgr:     txMgr,
		tenantMgr: tenantMgr,
	}
	namespaceMetadataProvider := &DefaultNamespaceMetadataProvider{
		namespaceStore: namespaceStore,
		txMgr:          txMgr,
		tenantMgr:      tenantMgr,
	}
	return &managementService{
		AuthProvider:              authProvider,
		UserMetadataProvider:      userMetadataProvider,
		NamespaceMetadataProvider: namespaceMetadataProvider,
		Manager:                   txMgr,
		TenantManager:             tenantMgr,
	}
}

func (m *managementService) CreateNamespace(ctx context.Context, req *api.CreateNamespaceRequest) (*api.CreateNamespaceResponse, error) {
	if req.GetName() == "" {
		return nil, errors.InvalidArgument("Empty namespace name is not allowed")
	}
	id := req.GetId()
	if req.GetId() == "" {
		id = uuid.New().String()
	}

	tx, err := m.Manager.StartTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create namespace - unable to create transaction")
		return nil, errors.Internal("Failed to create namespace")
	}

	code := req.GetCode()
	if req.GetCode() == 0 {
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
		code = maxId + 1
	}
	// API id maps to internal strId
	// API code maps to internal Id
	// API name maps to internal name
	namespace := metadata.NewTenantNamespace(id, metadata.NewNamespaceMetadata(code, id, req.GetName()))
	_, err = m.TenantManager.CreateTenant(ctx, tx, namespace)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	} else {
		if err = tx.Commit(ctx); err == nil {
			return &api.CreateNamespaceResponse{
				Status:  "CREATED",
				Message: "Namespace created, with code=" + fmt.Sprint(code) + ", and id=" + id,
				Namespace: &api.NamespaceInfo{
					Code: int32(code),
					Id:   id,
					Name: req.GetName(),
				},
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
			Id:   namespace.StrId(),
			Name: namespace.Metadata().Name,
			Code: int32(namespace.Id()),
		})
	}
	return &api.ListNamespacesResponse{
		Namespaces: namespacesInfo,
	}, nil
}

func (m *managementService) getNameSpaceDetails(ctx context.Context) (nsDetailsResp, error) {
	res := make(nsDetailsResp)
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

	for _, namespace := range namespaces {
		nsName := namespace.Metadata().Name
		tenant, err := m.TenantManager.GetTenant(ctx, nsName)
		if err != nil {
			return nil, err
		}

		for _, dbName := range tenant.ListDatabases(ctx) {
			db, err := tenant.GetDatabase(ctx, dbName)
			if err != nil {
				return nil, err
			}

			if db == nil {
				// database was dropped in the meantime
				continue
			}

			for _, coll := range db.ListCollection() {
				size, err := tenant.CollectionSize(ctx, db, coll)
				if err != nil {
					return nil, err
				}
				res[nsName][dbName][coll.Name] = map[string]string{
					"schema": string(coll.Schema),
					"size":   strconv.FormatInt(size, 10),
				}
			}
		}
	}
	return res, nil
}

func (m *managementService) DescribeNamespaces(ctx context.Context, _ *api.DescribeNamespacesRequest) (*api.DescribeNamespacesResponse, error) {
	data, err := m.getNameSpaceDetails(ctx)
	if ulog.E(err) {
		return nil, err
	}

	jsonStr, err := json.Marshal(data)
	if ulog.E(err) {
		return nil, err
	}

	return &api.DescribeNamespacesResponse{
		Data: &api.DescribeNamespacesData{
			Details: string(jsonStr),
		},
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

func (m *managementService) GetNamespaceMetadata(ctx context.Context, req *api.GetNamespaceMetadataRequest) (*api.GetNamespaceMetadataResponse, error) {
	return m.NamespaceMetadataProvider.GetNamespaceMetadata(ctx, req)
}

func (m *managementService) InsertNamespaceMetadata(ctx context.Context, req *api.InsertNamespaceMetadataRequest) (*api.InsertNamespaceMetadataResponse, error) {
	return m.NamespaceMetadataProvider.InsertNamespaceMetadata(ctx, req)
}

func (m *managementService) UpdateNamespaceMetadata(ctx context.Context, req *api.UpdateNamespaceMetadataRequest) (*api.UpdateNamespaceMetadataResponse, error) {
	return m.NamespaceMetadataProvider.UpdateNamespaceMetadata(ctx, req)
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
