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
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/search"
	"github.com/tigrisdata/tigris/server/transaction"
	searchStore "github.com/tigrisdata/tigris/store/search"
	"google.golang.org/grpc"
)

const (
	searchPathPattern = fullProjectPath + "/search/*"
)

type searchService struct {
	api.UnimplementedSearchServer

	txMgr         *transaction.Manager
	tenantMgr     *metadata.TenantManager
	versionH      *metadata.VersionHandler
	sessions      search.Session
	runnerFactory *search.RunnerFactory
}

func newSearchService(store searchStore.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) *searchService {
	return &searchService{
		txMgr:         txMgr,
		tenantMgr:     tenantMgr,
		versionH:      tenantMgr.GetVersionHandler(),
		sessions:      search.NewSessionManager(txMgr, tenantMgr, metadata.NewCacheTracker(tenantMgr, txMgr)),
		runnerFactory: search.NewRunnerFactory(store, tenantMgr.GetEncoder(), txMgr),
	}
}

func (s *searchService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
		runtime.WithIncomingHeaderMatcher(api.CustomMatcher),
		runtime.WithOutgoingHeaderMatcher(api.CustomMatcher),
	)

	if err := api.RegisterSearchHandlerClient(context.TODO(), mux, api.NewSearchClient(inproc)); err != nil {
		return err
	}

	api.RegisterSearchServer(inproc, s)

	router.HandleFunc(apiPathPrefix+searchPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})

	return nil
}

func (s *searchService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterSearchServer(grpc, s)
	return nil
}

func (s *searchService) CreateOrUpdateIndex(ctx context.Context, req *api.CreateOrUpdateIndexRequest) (*api.CreateOrUpdateIndexResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetIndexRunner(accessToken)
	runner.SetCreateIndexReq(req)

	resp, err := s.sessions.TxExecute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return &api.CreateOrUpdateIndexResponse{
		Status: resp.Status,
	}, nil
}

func (s *searchService) GetIndex(ctx context.Context, req *api.GetIndexRequest) (*api.GetIndexResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetIndexRunner(accessToken)
	runner.SetGetIndexReq(req)

	resp, err := s.sessions.TxExecute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.GetIndexResponse), nil
}

func (s *searchService) DeleteIndex(ctx context.Context, req *api.DeleteIndexRequest) (*api.DeleteIndexResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetIndexRunner(accessToken)
	runner.SetDeleteIndexReq(req)

	resp, err := s.sessions.TxExecute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return &api.DeleteIndexResponse{
		Status: resp.Status,
	}, nil
}

func (s *searchService) ListIndexes(ctx context.Context, req *api.ListIndexesRequest) (*api.ListIndexesResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetIndexRunner(accessToken)
	runner.SetListIndexesReq(req)

	resp, err := s.sessions.TxExecute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListIndexesResponse), nil
}

func (s *searchService) Get(ctx context.Context, req *api.GetDocumentRequest) (*api.GetDocumentResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetReadRunner(req, accessToken)
	resp, err := s.sessions.Execute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.GetDocumentResponse), nil
}

func (s *searchService) CreateById(ctx context.Context, req *api.CreateByIdRequest) (*api.CreateByIdResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetCreateRunner(accessToken)
	runner.SetCreateByIdReq(req)

	resp, err := s.sessions.Execute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.CreateByIdResponse), nil
}

func (s *searchService) Create(ctx context.Context, req *api.CreateDocumentRequest) (*api.CreateDocumentResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetCreateRunner(accessToken)
	runner.SetCreateDocumentsReq(req)

	resp, err := s.sessions.Execute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.CreateDocumentResponse), nil
}

func (s *searchService) CreateOrReplace(ctx context.Context, req *api.CreateOrReplaceDocumentRequest) (*api.CreateOrReplaceDocumentResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetCreateOrReplaceRunner(req, accessToken)
	resp, err := s.sessions.Execute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.CreateOrReplaceDocumentResponse), nil
}

func (s *searchService) Update(ctx context.Context, req *api.UpdateDocumentRequest) (*api.UpdateDocumentResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetUpdateQueryRunner(req, accessToken)
	resp, err := s.sessions.Execute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.UpdateDocumentResponse), nil
}

func (s *searchService) Delete(ctx context.Context, req *api.DeleteDocumentRequest) (*api.DeleteDocumentResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetDeleteQueryRunner(accessToken)
	runner.SetDeleteDocumentReq(req)

	resp, err := s.sessions.Execute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DeleteDocumentResponse), nil
}

func (s *searchService) DeleteByQuery(ctx context.Context, req *api.DeleteByQueryRequest) (*api.DeleteByQueryResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	runner := s.runnerFactory.GetDeleteQueryRunner(accessToken)
	runner.SetDeleteByQueryReq(req)

	resp, err := s.sessions.Execute(ctx, runner)
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DeleteByQueryResponse), nil
}

func (s *searchService) Search(req *api.SearchIndexRequest, stream api.Search_SearchServer) error {
	accessToken, _ := request.GetAccessToken(stream.Context())

	runner := s.runnerFactory.GetSearchRunner(req, stream, accessToken)
	_, err := s.sessions.Execute(stream.Context(), runner)
	if err != nil {
		return err
	}

	return nil
}
