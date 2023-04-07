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

package v1

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	uuid2 "github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/services/v1/auth"
	"github.com/tigrisdata/tigris/server/services/v1/billing"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc"
)

const (
	userPattern   = "/" + version + "/management/*"
	DeletedStatus = "deleted"
)

type managementService struct {
	api.UnimplementedManagementServer
	auth.Provider
	BillingProvider billing.Provider
	UserMetadataProvider
	NamespaceMetadataProvider
	*transaction.Manager
	*metadata.TenantManager
}

type nsDetailsResp = map[string]map[string]map[string]map[string]string

func newManagementService(authProvider auth.Provider, txMgr *transaction.Manager, tenantMgr *metadata.TenantManager, userStore *metadata.UserSubspace, namespaceStore *metadata.NamespaceSubspace) *managementService {
	if authProvider == nil && config.DefaultConfig.Auth.EnableOauth {
		log.Error().Str("Provider", config.DefaultConfig.Auth.OAuthProvider).Msg("Unable to configure external auth provider")
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
		Provider:                  authProvider,
		UserMetadataProvider:      userMetadataProvider,
		NamespaceMetadataProvider: namespaceMetadataProvider,
		BillingProvider:           billing.NewProvider(),
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
		id = uuid2.New().String()
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

	meta := metadata.NewNamespaceMetadata(code, id, req.GetName())

	// Create a Billing account, if it fails metrics reporter will retry in a separate flow
	// does not block namespace creation
	billingId, err := m.BillingProvider.CreateAccount(ctx, id, req.GetName())
	if !ulog.E(err) && billingId != uuid2.NullUUID {
		// account creation succeeds, update namespace metadata
		meta.Accounts.AddMetronome(billingId.String())
		// add tenant to default plan
		added, err := m.BillingProvider.AddDefaultPlan(ctx, billingId)

		if err != nil || !added {
			log.Error().Err(err).Msg("error adding default plan to customer")
		}
	}

	// API id maps to internal strId
	// API code maps to internal Id
	// API name maps to internal name
	namespace := metadata.NewTenantNamespace(id, meta)
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

func (m *managementService) ListNamespaces(ctx context.Context, req *api.ListNamespacesRequest) (*api.ListNamespacesResponse, error) {
	if req.GetDescribe() {
		return m.getNamespacesDetails(ctx, req.GetNamespaceId())
	} else {
		return m.listNamespaces(ctx, req.GetNamespaceId())
	}
}

func (m *managementService) DeleteNamespace(ctx context.Context, req *api.DeleteNamespaceRequest) (*api.DeleteNamespaceResponse, error) {
	// putting this behind safety switch.
	// The deletion of namespace isn't fully automated yet
	if !config.DefaultConfig.Auth.EnableNamespaceDeletion {
		return nil, errors.Unimplemented("Deletion is not enabled on this cluster")
	}

	tenantToDelete, err := m.TenantManager.GetTenant(ctx, req.GetNamespaceId())
	if err != nil || tenantToDelete == nil {
		log.Error().Err(err).Msgf("Failed to get tenant for the namespaceId: %s", req.GetNamespaceId())
		return nil, errors.NotFound("Unable to find the namespace: %s", req.GetNamespaceId())
	}

	tx, err := m.StartTx(ctx)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to start tx to delete projects")
		return nil, errors.Internal("Failed to delete namespace")
	}

	err = m.TenantManager.DeleteTenant(ctx, tx, tenantToDelete)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to delete tenant")
		err = tx.Rollback(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to rollback transaction")
		}
		return nil, errors.Internal("Failed to delete namespace")
	}

	// delete namespace metadata
	err = m.NamespaceMetadataProvider.DeleteNamespace(ctx, tx, tenantToDelete.GetNamespace().Id())
	if err != nil {
		log.Error().Err(err).Msgf("Failed to delete namespace metadata")
		err = tx.Rollback(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to rollback transaction")
		}
		return nil, errors.Internal("Failed to delete namespace")
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to commit tx to delete namespace")
		return nil, errors.Internal("Failed to delete namespace")
	}

	return &api.DeleteNamespaceResponse{
		Message: fmt.Sprintf("Namespace: %s deleted successfully", req.GetNamespaceId()),
		Status:  DeletedStatus,
	}, nil
}

func (m *managementService) getNamespacesDetails(ctx context.Context, namespaceId string) (*api.ListNamespacesResponse, error) {
	data, err := m.getNameSpaceDetails(ctx, namespaceId)
	if ulog.E(err) {
		return nil, err
	}
	jsonStr, err := jsoniter.Marshal(data)
	if ulog.E(err) {
		return nil, err
	}
	return &api.ListNamespacesResponse{Data: &api.DescribeNamespacesData{Details: string(jsonStr)}}, nil
}

func (m *managementService) listNamespaces(ctx context.Context, namespaceId string) (*api.ListNamespacesResponse, error) {
	tx, err := m.Manager.StartTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Could not begin transaction to list namespaces")
		return nil, errors.Internal("Failed to begin transaction")
	}
	namespaces, err := m.TenantManager.ListNamespaces(ctx, tx)
	if err != nil {
		log.Error().Err(err).Msg("Could not list namespaces")
		_ = tx.Rollback(ctx)
		return nil, err
	}
	_ = tx.Commit(ctx)
	if namespaces == nil {
		return &api.ListNamespacesResponse{
			Namespaces: nil,
		}, nil
	}

	outputArrayLen := len(namespaces)
	if namespaceId != "" {
		outputArrayLen = 1
	}

	namespacesInfo := make([]*api.NamespaceInfo, 0, outputArrayLen)
	for _, namespace := range namespaces {
		// if specific namespace is requested and if this is not that one - continue
		if namespaceId != "" && namespaceId != namespace.StrId() {
			continue
		}
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

func (m *managementService) getNameSpaceDetails(ctx context.Context, namespaceId string) (nsDetailsResp, error) {
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
		// if specific namespace is requested and if this is not that one - continue
		if namespaceId != "" && namespaceId != namespace.StrId() {
			continue
		}
		nsName := namespace.Metadata().Name
		tenant, err := m.TenantManager.GetTenant(ctx, nsName)
		if err != nil {
			return nil, err
		}

		for _, projName := range tenant.ListProjects(ctx) {
			project, err := tenant.GetProject(projName)
			if err != nil {
				// project was dropped in the meantime
				continue
			}

			for _, db := range project.GetDatabaseWithBranches() {
				for _, coll := range db.ListCollection() {
					size, err := tenant.CollectionSize(ctx, db, coll)
					if err != nil {
						return nil, err
					}
					res[nsName][db.Name()][coll.Name] = map[string]string{
						"schema": string(coll.Schema),
						"size":   strconv.FormatInt(size.StoredBytes, 10),
					}
				}
			}
		}
	}
	return res, nil
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
