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
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/cdc"
	"github.com/tigrisdata/tigrisdb/server/metadata"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	api.UnimplementedTigrisServer

	kvStore               kv.KeyValueStore
	txMgr                 *transaction.Manager
	encoder               metadata.Encoder
	tenantMgr             *metadata.TenantManager
	cdcMgr                *cdc.Manager
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
	u.cdcMgr = cdc.NewManager()
	u.queryLifecycleFactory = NewQueryLifecycleFactory(u.txMgr, u.tenantMgr, u.cdcMgr)
	u.queryRunnerFactory = NewQueryRunnerFactory(u.txMgr, u.encoder, u.cdcMgr)
	return u
}

func (s *apiService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{
		JSONBuiltin: &runtime.JSONBuiltin{},
	}))

	if err := api.RegisterTigrisHandlerClient(context.TODO(), mux, api.NewTigrisClient(inproc)); err != nil {
		return err
	}

	api.RegisterTigrisServer(inproc, s)

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
	api.RegisterTigrisServer(grpc, s)
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

	resp, err := s.Run(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetInsertQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.InsertResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.Run(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetReplaceQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.ReplaceResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.Run(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetUpdateQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.UpdateResponse{
		Status:        resp.status,
		ModifiedCount: resp.modifiedCount,
		Metadata: &api.ResponseMetadata{
			UpdatedAt: resp.updatedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Delete(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.Run(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.queryRunnerFactory.GetDeleteQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.DeleteResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			UpdatedAt: resp.updatedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Read(r *api.ReadRequest, stream api.Tigris_ReadServer) error {
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

	resp, err := s.Run(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateOrUpdateCollectionResponse{
		Status:  resp.status,
		Message: "collection created successfully",
	}, nil
}

func (s *apiService) DropCollection(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	runner := s.queryRunnerFactory.GetCollectionQueryRunner()
	runner.SetDropCollectionReq(r)

	resp, err := s.Run(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return &api.DropCollectionResponse{
		Status:  resp.status,
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
	resp, err := s.Run(ctx, &ReqOptions{
		queryRunner: queryRunner,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateDatabaseResponse{
		Status:  resp.status,
		Message: "database created successfully",
	}, nil
}

func (s *apiService) DropDatabase(ctx context.Context, r *api.DropDatabaseRequest) (*api.DropDatabaseResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	queryRunner := s.queryRunnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetDropDatabaseReq(r)
	resp, err := s.Run(ctx, &ReqOptions{
		queryRunner: queryRunner,
	})
	if err != nil {
		return nil, err
	}

	return &api.DropDatabaseResponse{
		Status:  resp.status,
		Message: "database dropped successfully",
	}, nil
}

func (s *apiService) DescribeCollection(ctx context.Context, r *api.DescribeCollectionRequest) (*api.DescribeCollectionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	runner := s.queryRunnerFactory.GetCollectionQueryRunner()
	runner.SetDescribeCollectionReq(r)

	resp, err := s.Run(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeCollectionResponse), nil
}

func (s *apiService) DescribeDatabase(ctx context.Context, r *api.DescribeDatabaseRequest) (*api.DescribeDatabaseResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	runner := s.queryRunnerFactory.GetDatabaseQueryRunner()
	runner.SetDescribeDatabaseReq(r)

	resp, err := s.Run(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeDatabaseResponse), nil
}

func (s *apiService) Run(ctx context.Context, req *ReqOptions) (*Response, error) {
	queryLifecycle := s.queryLifecycleFactory.Get()
	return queryLifecycle.run(ctx, req)
}

func (s *apiService) Stream(r *api.StreamRequest, stream api.Tigris_StreamServer) error {
	if err := r.Validate(); err != nil {
		return err
	}

	publisher := s.cdcMgr.GetPublisher(r.GetDb())
	streamer, err := publisher.NewStreamer(s.kvStore)
	if err != nil {
		return err
	}
	defer streamer.Close()

	for {
		select {
		case tx, ok := <-streamer.Txs:
			if !ok {
				return api.Error(codes.Canceled, "buffer overflow")
			}

			changes := make([]*api.StreamChange, 0)

			for _, op := range tx.Ops {
				data, err := jsoniter.Marshal(op)
				if err != nil {
					return err
				}

				changes = append(changes, &api.StreamChange{
					CollectionName: "todo", // TODO: CDC extract name from op
					Data:           data,
				})
			}

			if err := stream.Send(&api.StreamResponse{
				Changes: changes,
			}); ulog.E(err) {
				return err
			}
		}
	}
}
