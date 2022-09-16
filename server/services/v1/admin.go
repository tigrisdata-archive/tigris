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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	"google.golang.org/grpc"
)

var (
	ReservedNamespaceNames = container.NewHashSet(request.DefaultNamespaceName, "unknown")
)

const (
	adminPath = "/" + version + "/admin"
)
const (
	namespacePath        = "/namespaces"
	namespacePathPattern = namespacePath + "/*"
)

type adminService struct {
	api.UnimplementedAdminServer
	tenantMgr *metadata.TenantManager
	txMgr     *transaction.Manager
}

func newAdminService(tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) *adminService {
	return &adminService{
		tenantMgr: tenantMgr,
		txMgr:     txMgr,
	}
}

func (a *adminService) CreateNamespace(ctx context.Context, req *api.CreateNamespaceRequest) (*api.CreateNamespaceResponse, error) {
	if ReservedNamespaceNames.Contains(req.Name) {
		return nil, errors.InvalidArgument(req.Name + " is reserved name")
	}
	if req.Name == "" {
		return nil, errors.InvalidArgument("Empty namespace name is not allowed")
	}
	if req.GetId() <= 1 {
		return nil, errors.InvalidArgument("NamespaceId must be greater than 1")
	}

	namespace := metadata.NewTenantNamespace(req.Name, uint32(req.Id))
	tx, err := a.txMgr.StartTx(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to create namespace")
	}
	_, err = a.tenantMgr.CreateTenant(ctx, tx, namespace)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	} else {
		if err = tx.Commit(ctx); err == nil {
			return &api.CreateNamespaceResponse{
				Status:  "CREATED",
				Message: "Namespace created, with id=" + fmt.Sprint(req.Id) + ", and name=" + req.Name,
			}, nil
		} else {
			return nil, err
		}
	}
}

func (a *adminService) ListNamespaces(ctx context.Context, _ *api.ListNamespacesRequest) (*api.ListNamespacesResponse, error) {
	tx, err := a.txMgr.StartTx(ctx)
	if err != nil {
		return nil, errors.Internal("Failed to begin transaction")
	}
	namespaces, err := a.tenantMgr.ListNamespaces(ctx, tx)
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

	var namespacesInfo []*api.NamespaceInfo
	for _, namespace := range namespaces {
		namespacesInfo = append(namespacesInfo, &api.NamespaceInfo{
			Id:   int32(namespace.Id()),
			Name: namespace.Name(),
		})
	}
	return &api.ListNamespacesResponse{
		Namespaces: namespacesInfo,
	}, nil
}

func (a *adminService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterAdminHandlerClient(context.TODO(), mux, api.NewAdminClient(inproc)); err != nil {
		return err
	}
	api.RegisterAdminServer(inproc, a)
	router.HandleFunc(adminPath+namespacePathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (a *adminService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterAdminServer(grpc, a)
	return nil
}
