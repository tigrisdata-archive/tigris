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

	"net/http"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/services/v1/billing"
	"google.golang.org/grpc"
)

const (
	billingPathPattern = apiPathPrefix + "/billing/*"
)

type billingService struct {
	api.UnimplementedBillingServer
	billing.Provider
}

func newBillingService(b billing.Provider) *billingService {
	return &billingService{Provider: b}
}

func (b *billingService) ListInvoices(_ context.Context, _ *api.ListInvoicesRequest) (*api.ListInvoicesResponse, error) {
	return nil, errors.Unimplemented("list invoices is not yet implemented on the server")
}

func (b *billingService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterBillingHandlerClient(context.TODO(), mux, api.NewBillingClient(inproc)); err != nil {
		return err
	}
	api.RegisterBillingServer(inproc, b)
	router.HandleFunc(billingPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (b *billingService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterBillingServer(grpc, b)
	return nil
}
