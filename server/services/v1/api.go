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
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/metadata"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc"
)

const (
	databasePath        = "/databases"
	databasePathPattern = databasePath + "/*"

	collectionPath        = databasePath + "/collections"
	collectionPathPattern = collectionPath + "/*"

	documentPath        = collectionPath + "/documents"
	documentPathPattern = documentPath + "/*"
)

type apiService struct {
	api.UnimplementedTigrisDBServer

	kvStore               kv.KeyValueStore
	txMgr                 *transaction.Manager
	encoder               metadata.Encoder
	tenantMgr             *metadata.TenantManager
	queryLifecycleFactory *QueryLifecycleFactory
	queryRunnerFactory    *QueryRunnerFactory
}

func newApiService(kv kv.KeyValueStore) *apiService {
	u := &apiService{
		kvStore: kv,
		txMgr:   transaction.NewManager(kv),
		encoder: metadata.NewEncoder(),
	}

	ctx := context.TODO()
	tx, err := u.txMgr.StartTxWithoutTracking(ctx)
	if ulog.E(err) {
		log.Fatal().Err(err).Msgf("error starting server: starting transaction failed")
	}

	tenantMgr := metadata.NewTenantManager()
	if err := tenantMgr.Reload(ctx, tx); ulog.E(err) {
		// ToDo: no need to panic, probably handle through async thread.
		log.Err(err).Msgf("error starting server: reloading tenants failed")
	}
	_ = tx.Commit(ctx)

	u.tenantMgr = tenantMgr
	u.queryLifecycleFactory = NewQueryLifecycleFactory(u.txMgr, u.tenantMgr)
	u.queryRunnerFactory = NewQueryRunnerFactory(u.txMgr, u.encoder)
	return u
}

func (s *apiService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{
		JSONBuiltin: &runtime.JSONBuiltin{},
	}))

	if err := api.RegisterTigrisDBHandlerClient(context.TODO(), mux, api.NewTigrisDBClient(inproc)); err != nil {
		return err
	}

	api.RegisterTigrisDBServer(inproc, s)

	router.HandleFunc(apiPathPrefix+databasePathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+collectionPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+documentPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})

	return nil
}

func (s *apiService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterTigrisDBServer(grpc, s)
	return nil
}

func (s *apiService) BeginTransaction(ctx context.Context, r *api.BeginTransactionRequest) (*api.BeginTransactionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	_, txCtx, err := s.txMgr.StartTx(ctx, true)
	if err != nil {
		return nil, err
	}

	return &api.BeginTransactionResponse{
		TxCtx: txCtx,
	}, nil
}

func (s *apiService) CommitTransaction(ctx context.Context, r *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	tx, err := s.txMgr.GetTx(r.TxCtx)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &api.CommitTransactionResponse{}, nil
}

func (s *apiService) RollbackTransaction(ctx context.Context, r *api.RollbackTransactionRequest) (*api.RollbackTransactionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	tx, err := s.txMgr.GetTx(r.TxCtx)
	if err != nil {
		return nil, err
	}

	if err = tx.Rollback(ctx); err != nil {
		// ToDo: Do we need to return here in this case? Or silently return success?
		return nil, err
	}

	return &api.RollbackTransactionResponse{}, nil
}

// Insert new object returns an error if object already exists
// Operations done individually not in actual batch
func (s *apiService) Insert(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	_, err := s.Run(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetInsertQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.InsertResponse{}, nil
}

func (s *apiService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	_, err := s.Run(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetReplaceQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.ReplaceResponse{}, nil
}

func (s *apiService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	_, err := s.Run(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetUpdateQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.UpdateResponse{}, nil
}

func (s *apiService) Delete(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	_, err := s.Run(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetDeleteQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.DeleteResponse{}, nil
}

func (s *apiService) Read(r *api.ReadRequest, stream api.TigrisDB_ReadServer) error {
	if err := r.Validate(); err != nil {
		return err
	}

	_, err := s.Run(stream.Context(), &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetStreamingQueryRunner(r, stream),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *apiService) CreateOrUpdateCollection(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) (*api.CreateOrUpdateCollectionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	runner := s.queryRunnerFactory.GetCollectionQueryRunner()
	runner.SetCreateOrUpdateCollectionReq(r)

	_, err := s.Run(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateOrUpdateCollectionResponse{
		Message: "collection created successfully",
	}, nil
}

func (s *apiService) DropCollection(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	runner := s.queryRunnerFactory.GetCollectionQueryRunner()
	runner.SetDropCollectionReq(r)

	_, err := s.Run(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return &api.DropCollectionResponse{
		Message: "collection dropped successfully",
	}, nil
}

func (s *apiService) ListCollections(ctx context.Context, r *api.ListCollectionsRequest) (*api.ListCollectionsResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	runner := s.queryRunnerFactory.GetCollectionQueryRunner()
	runner.SetListCollectionReq(r)

	resp, err := s.Run(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListCollectionsResponse), nil
}

func (s *apiService) ListDatabases(ctx context.Context, r *api.ListDatabasesRequest) (*api.ListDatabasesResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	queryRunner := s.queryRunnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetListDatabaseReq(r)

	resp, err := s.Run(ctx, &ReqOptions{
		queryRunner: queryRunner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListDatabasesResponse), nil
}

func (s *apiService) CreateDatabase(ctx context.Context, r *api.CreateDatabaseRequest) (*api.CreateDatabaseResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	queryRunner := s.queryRunnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetCreateDatabaseReq(r)
	if _, err := s.Run(ctx, &ReqOptions{
		queryRunner: queryRunner,
	}); err != nil {
		return nil, err
	}

	return &api.CreateDatabaseResponse{
		Message: "database created successfully",
	}, nil
}

func (s *apiService) DropDatabase(ctx context.Context, r *api.DropDatabaseRequest) (*api.DropDatabaseResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	queryRunner := s.queryRunnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetDropDatabaseReq(r)
	if _, err := s.Run(ctx, &ReqOptions{
		queryRunner: queryRunner,
	}); err != nil {
		return nil, err
	}

	return &api.DropDatabaseResponse{
		Message: "database dropped successfully",
	}, nil
}

func (s *apiService) Run(ctx context.Context, req *ReqOptions) (*Response, error) {
	queryLifecycle := s.queryLifecycleFactory.Get()
	return queryLifecycle.run(ctx, req)
}
