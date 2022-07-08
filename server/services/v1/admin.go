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
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"google.golang.org/grpc"
)

const (
	adminPath = "/admin/" + version
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
	namespace := metadata.NewTenantNamespace(req.Name, uint32(req.Id))
	tx, err := a.txMgr.StartTx(ctx)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to create namespace")
	}
	err = a.tenantMgr.CreateTenant(ctx, tx, namespace)
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

func (h *adminService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterAdminServer(grpc, h)
	return nil
}
