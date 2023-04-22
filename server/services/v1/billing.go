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
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/billing"
	"google.golang.org/grpc"
)

const (
	billingPathPattern = apiPathPrefix + "/billing/*"
)

type billingService struct {
	api.UnimplementedBillingServer
	billing.Provider
	nsMgr metadata.NamespaceMetadataMgr
}

func newBillingService(b billing.Provider, nsMgr metadata.NamespaceMetadataMgr) *billingService {
	return &billingService{
		Provider: b,
		nsMgr:    nsMgr,
	}
}

func (b *billingService) ListInvoices(ctx context.Context, req *api.ListInvoicesRequest) (*api.ListInvoicesResponse, error) {
	namespace, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, err
	}
	mId, err := b.getMetronomeId(ctx, namespace)
	if err != nil {
		return nil, err
	}

	if inv := req.GetInvoiceId(); len(inv) > 0 {
		return b.Provider.GetInvoiceById(ctx, mId, req.GetInvoiceId())
	}

	return b.GetInvoices(ctx, mId, req)
}

func (b *billingService) getMetronomeId(ctx context.Context, namespaceId string) (billing.MetronomeId, error) {
	nsMeta := b.nsMgr.GetNamespaceMetadata(ctx, namespaceId)
	if nsMeta == nil {
		log.Warn().Msgf("Could not find namespace, this must not happen with right authn/authz configured")
		return uuid.Nil, errors.NotFound("Namespace %s not found", namespaceId)
	}

	mIdStr, enabled := nsMeta.Accounts.GetMetronomeId()
	if !enabled {
		log.Error().Msgf("No metronome account for the namespace: %s", namespaceId)
		return uuid.Nil, errors.Internal("No account linked for the user")
	}

	mId, err := uuid.Parse(mIdStr)
	if err != nil {
		return uuid.Nil, err
	}
	return mId, nil
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
