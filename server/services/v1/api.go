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

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/cdc"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/auth"
	"github.com/tigrisdata/tigris/server/services/v1/database"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc"
)

const (
	projectsPath           = "/projects"
	fullProjectPath        = projectsPath + "/{project}"
	databasePathPattern    = fullProjectPath + "/database/*"
	applicationPathPattern = fullProjectPath + "/apps/*"

	infoPath    = "/info"
	metricsPath = "/metrics"
)

type apiService struct {
	api.UnimplementedTigrisServer

	kvStore       kv.KeyValueStore
	txMgr         *transaction.Manager
	tenantMgr     *metadata.TenantManager
	cdcMgr        *cdc.Manager
	sessions      database.Session
	runnerFactory *database.QueryRunnerFactory
	versionH      *metadata.VersionHandler
	searchStore   search.Store
	authProvider  auth.Provider
}

func newApiService(kv kv.KeyValueStore, searchStore search.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager, authProvider auth.Provider) *apiService {
	u := &apiService{
		kvStore:      kv,
		txMgr:        txMgr,
		versionH:     &metadata.VersionHandler{},
		searchStore:  searchStore,
		cdcMgr:       cdc.NewManager(),
		tenantMgr:    tenantMgr,
		authProvider: authProvider,
	}

	collectionsInSearch, err := u.searchStore.AllCollections(context.TODO())
	if err != nil {
		log.Fatal().Err(err).Msgf("error starting server: loading schemas from search failed")
	}

	ctx := context.TODO()
	tx, err := u.txMgr.StartTx(ctx)
	if ulog.E(err) {
		log.Fatal().Err(err).Msgf("error starting server: starting transaction failed")
	}

	if err := tenantMgr.Reload(ctx, tx, collectionsInSearch); ulog.E(err) {
		// ToDo: no need to panic, probably handle through async thread.
		log.Fatal().Err(err).Msgf("error starting server: reloading tenants failed")
	}
	ulog.E(tx.Commit(ctx))

	var txListeners []database.TxListener
	if config.DefaultConfig.Cdc.Enabled {
		txListeners = append(txListeners, u.cdcMgr)
	}
	if config.DefaultConfig.Search.WriteEnabled {
		// just for testing so that we can disable it if needed
		txListeners = append(txListeners, database.NewSearchIndexer(searchStore, tenantMgr))
	}

	if config.DefaultConfig.Tracing.Enabled {
		u.sessions = database.NewSessionManagerWithMetrics(u.txMgr, u.tenantMgr, u.versionH, txListeners, metadata.NewCacheTracker(tenantMgr, txMgr))
	} else {
		u.sessions = database.NewSessionManager(u.txMgr, u.tenantMgr, u.versionH, txListeners, metadata.NewCacheTracker(tenantMgr, txMgr))
	}
	u.runnerFactory = database.NewQueryRunnerFactory(u.txMgr, u.cdcMgr, u.searchStore)

	return u
}

func (s *apiService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
		runtime.WithIncomingHeaderMatcher(api.CustomMatcher),
		runtime.WithOutgoingHeaderMatcher(api.CustomMatcher),
	)
	if err := api.RegisterTigrisHandlerClient(context.TODO(), mux, api.NewTigrisClient(inproc)); err != nil {
		return err
	}

	api.RegisterTigrisServer(inproc, s)

	// add list projects path
	router.HandleFunc(apiPathPrefix+projectsPath, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	for _, projectPath := range []string{"/create", "/delete"} {
		// explicit add project related path
		router.HandleFunc(apiPathPrefix+fullProjectPath+projectPath, func(w http.ResponseWriter, r *http.Request) {
			mux.ServeHTTP(w, r)
		})
	}
	router.HandleFunc(apiPathPrefix+databasePathPattern, func(w http.ResponseWriter, r *http.Request) {
		// to handle all the database related stuff
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+applicationPathPattern, func(w http.ResponseWriter, r *http.Request) {
		// to handle app keys
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+infoPath, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})

	if config.DefaultConfig.Metrics.Enabled {
		router.Handle(metricsPath, metrics.Reporter.HTTPHandler())
	}

	return nil
}

func (s *apiService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterTigrisServer(grpc, s)
	return nil
}

func (s *apiService) BeginTransaction(ctx context.Context, _ *api.BeginTransactionRequest) (*api.BeginTransactionResponse, error) {
	// explicit transactions needed to be tracked
	session, err := s.sessions.Create(ctx, true, true, true)
	if err != nil {
		return nil, err
	}

	return &api.BeginTransactionResponse{
		TxCtx: session.GetTransactionCtx(),
	}, nil
}

func (s *apiService) CommitTransaction(ctx context.Context, _ *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
	session, _ := s.sessions.Get(ctx)
	if session == nil {
		return nil, errors.NotFound("session not found")
	}
	defer func() {
		if err := s.sessions.Remove(ctx); err != nil {
			ulog.E(err)
		}
	}()

	err := session.Commit(s.versionH, session.GetTx().Context().GetStagedDatabase() != nil, nil)
	if err != nil {
		return nil, err
	}

	return &api.CommitTransactionResponse{}, nil
}

func (s *apiService) RollbackTransaction(ctx context.Context, _ *api.RollbackTransactionRequest) (*api.RollbackTransactionResponse, error) {
	session, _ := s.sessions.Get(ctx)
	if session == nil {
		return nil, errors.NotFound("session not found")
	}
	defer func() {
		if err := s.sessions.Remove(ctx); err != nil {
			ulog.E(err)
		}
	}()

	_ = session.Rollback()

	return &api.RollbackTransactionResponse{}, nil
}

// Insert new object returns an error if object already exists
// Operations done individually not in actual batch.
func (s *apiService) Insert(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
	qm := metrics.WriteQueryMetrics{}
	accessToken, _ := request.GetAccessToken(ctx)
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetInsertQueryRunner(r, &qm, accessToken), &database.ReqOptions{
		TxCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.InsertResponse{
		Status: resp.Status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.CreatedAt.GetProtoTS(),
		},
		Keys: resp.AllKeys,
	}, nil
}

func (s *apiService) Import(ctx context.Context, r *api.ImportRequest) (*api.ImportResponse, error) {
	qm := metrics.WriteQueryMetrics{}
	accessToken, _ := request.GetAccessToken(ctx)

	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetImportQueryRunner(r, &qm, accessToken), &database.ReqOptions{
		TxCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.ImportResponse{
		Status: resp.Status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.CreatedAt.GetProtoTS(),
		},
		Keys: resp.AllKeys,
	}, nil
}

func (s *apiService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	qm := metrics.WriteQueryMetrics{}
	accessToken, _ := request.GetAccessToken(ctx)
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetReplaceQueryRunner(r, &qm, accessToken), &database.ReqOptions{
		TxCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.ReplaceResponse{
		Status: resp.Status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.CreatedAt.GetProtoTS(),
		},
		Keys: resp.AllKeys,
	}, nil
}

func (s *apiService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	queryMetrics := metrics.WriteQueryMetrics{}
	accessToken, _ := request.GetAccessToken(ctx)
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetUpdateQueryRunner(r, &queryMetrics, accessToken), &database.ReqOptions{
		TxCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.UpdateResponse{
		Status:        resp.Status,
		ModifiedCount: resp.ModifiedCount,
		Metadata: &api.ResponseMetadata{
			UpdatedAt: resp.UpdatedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Delete(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
	queryMetrics := metrics.WriteQueryMetrics{}
	accessToken, _ := request.GetAccessToken(ctx)
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetDeleteQueryRunner(r, &queryMetrics, accessToken), &database.ReqOptions{
		TxCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.DeleteResponse{
		Status: resp.Status,
		Metadata: &api.ResponseMetadata{
			DeletedAt: resp.DeletedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Read(r *api.ReadRequest, stream api.Tigris_ReadServer) error {
	var err error
	queryMetrics := metrics.StreamingQueryMetrics{}
	accessToken, _ := request.GetAccessToken(stream.Context())

	if api.GetTransaction(stream.Context()) != nil {
		_, err = s.sessions.Execute(stream.Context(), s.runnerFactory.GetStreamingQueryRunner(r, stream, &queryMetrics, accessToken), &database.ReqOptions{
			TxCtx:              api.GetTransaction(stream.Context()),
			InstantVerTracking: true,
		})
	} else {
		_, err = s.sessions.ReadOnlyExecute(stream.Context(), s.runnerFactory.GetStreamingQueryRunner(r, stream, &queryMetrics, accessToken), &database.ReqOptions{})
	}
	return err
}

func (s *apiService) Search(r *api.SearchRequest, stream api.Tigris_SearchServer) error {
	queryMetrics := metrics.SearchQueryMetrics{}
	accessToken, _ := request.GetAccessToken(stream.Context())
	_, err := s.sessions.ReadOnlyExecute(stream.Context(), s.runnerFactory.GetSearchQueryRunner(r, stream, &queryMetrics, accessToken), &database.ReqOptions{
		InstantVerTracking: true,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *apiService) CreateOrUpdateCollection(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) (*api.CreateOrUpdateCollectionResponse, error) {
	collectionType, err := schema.GetCollectionType(r.Schema)
	if err != nil {
		return nil, errors.Internal("not able to extract collection type from the schema")
	}
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetCollectionQueryRunner(accessToken)
	runner.SetCreateOrUpdateCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{
		TxCtx:              api.GetTransaction(ctx),
		MetadataChange:     true,
		InstantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateOrUpdateCollectionResponse{
		Status:  resp.Status,
		Message: fmt.Sprintf("collection of type '%s' created successfully", collectionType),
	}, nil
}

func (s *apiService) DropCollection(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetCollectionQueryRunner(accessToken)
	runner.SetDropCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{
		TxCtx:              api.GetTransaction(ctx),
		MetadataChange:     true,
		InstantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.DropCollectionResponse{
		Status:  resp.Status,
		Message: "collection dropped successfully",
	}, nil
}

func (s *apiService) ListCollections(ctx context.Context, r *api.ListCollectionsRequest) (*api.ListCollectionsResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetCollectionQueryRunner(accessToken)
	runner.SetListCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{
		TxCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListCollectionsResponse), nil
}

func (s *apiService) ListProjects(ctx context.Context, r *api.ListProjectsRequest) (*api.ListProjectsResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetDatabaseQueryRunner(accessToken)
	runner.SetListProjectsReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListProjectsResponse), nil
}

func (s *apiService) CreateProject(ctx context.Context, r *api.CreateProjectRequest) (*api.CreateProjectResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetDatabaseQueryRunner(accessToken)
	runner.SetCreateProjectReq(r)
	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{
		MetadataChange:     true,
		InstantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateProjectResponse{
		Status:  resp.Status,
		Message: "project created successfully",
	}, nil
}

func (s *apiService) DeleteProject(ctx context.Context, r *api.DeleteProjectRequest) (*api.DeleteProjectResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetDatabaseQueryRunner(accessToken)
	runner.SetDeleteProjectReq(r)
	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{
		MetadataChange:     true,
		InstantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	// delete app-keys associated with project
	if config.DefaultConfig.Auth.Enabled {
		err := s.authProvider.DeleteAppKeys(ctx, r.GetProject())
		if err != nil {
			return nil, err
		}
	}
	return &api.DeleteProjectResponse{
		Status:  resp.Status,
		Message: "project deleted successfully",
	}, nil
}

func (s *apiService) DescribeCollection(ctx context.Context, r *api.DescribeCollectionRequest) (*api.DescribeCollectionResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetCollectionQueryRunner(accessToken)
	runner.SetDescribeCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeCollectionResponse), nil
}

func (s *apiService) DescribeDatabase(ctx context.Context, r *api.DescribeDatabaseRequest) (*api.DescribeDatabaseResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetDatabaseQueryRunner(accessToken)
	runner.SetDescribeDatabaseReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeDatabaseResponse), nil
}

func (s *apiService) CreateBranch(ctx context.Context, r *api.CreateBranchRequest) (*api.CreateBranchResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetDatabaseQueryRunner(accessToken)
	runner.SetCreateBranchReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{
		MetadataChange:     true,
		InstantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.CreateBranchResponse), nil
}

func (s *apiService) DeleteBranch(ctx context.Context, r *api.DeleteBranchRequest) (*api.DeleteBranchResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	runner := s.runnerFactory.GetDatabaseQueryRunner(accessToken)
	runner.SetDeleteBranchReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &database.ReqOptions{
		MetadataChange:     true,
		InstantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DeleteBranchResponse), nil
}

func (s *apiService) CreateAppKey(ctx context.Context, req *api.CreateAppKeyRequest) (*api.CreateAppKeyResponse, error) {
	return s.authProvider.CreateAppKey(ctx, req)
}

func (s *apiService) UpdateAppKey(ctx context.Context, req *api.UpdateAppKeyRequest) (*api.UpdateAppKeyResponse, error) {
	return s.authProvider.UpdateAppKey(ctx, req)
}

func (s *apiService) DeleteAppKey(ctx context.Context, req *api.DeleteAppKeyRequest) (*api.DeleteAppKeyResponse, error) {
	return s.authProvider.DeleteAppKey(ctx, req)
}

func (s *apiService) ListAppKeys(ctx context.Context, req *api.ListAppKeysRequest) (*api.ListAppKeysResponse, error) {
	return s.authProvider.ListAppKeys(ctx, req)
}

func (s *apiService) RotateAppKeySecret(ctx context.Context, req *api.RotateAppKeyRequest) (*api.RotateAppKeyResponse, error) {
	return s.authProvider.RotateAppKey(ctx, req)
}
