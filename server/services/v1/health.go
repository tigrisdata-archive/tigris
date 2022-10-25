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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"google.golang.org/grpc"
)

const (
	healthPath = "/health"
)

type healthService struct {
	api.UnimplementedHealthAPIServer

	versionH *metadata.VersionHandler
	txMgr    *transaction.Manager
}

func newHealthService(txMgr *transaction.Manager) *healthService {
	return &healthService{
		versionH: &metadata.VersionHandler{},
		txMgr:    txMgr,
	}
}

func (h *healthService) Health(ctx context.Context, _ *api.HealthCheckInput) (*api.HealthCheckResponse, error) {
	_, err := h.versionH.ReadInOwnTxn(ctx, h.txMgr, false)
	if err != nil {
		return nil, errors.Unavailable("Could not read metadata version")
	}

	return &api.HealthCheckResponse{
		Response: "OK",
	}, nil
}

func (h *healthService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterHealthAPIHandlerClient(context.TODO(), mux, api.NewHealthAPIClient(inproc)); err != nil {
		return err
	}
	api.RegisterHealthAPIServer(inproc, h)
	router.HandleFunc(apiPathPrefix+healthPath, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (h *healthService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterHealthAPIServer(grpc, h)
	return nil
}
