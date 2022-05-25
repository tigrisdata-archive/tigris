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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/cdc"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/metadata"
	middleware "github.com/tigrisdata/tigris/server/midddleware"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
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

	infoPath = "/info"
)

type apiService struct {
	api.UnimplementedTigrisServer

	kvStore       kv.KeyValueStore
	txMgr         *transaction.Manager
	encoder       metadata.Encoder
	tenantMgr     *metadata.TenantManager
	cdcMgr        *cdc.Manager
	sessions      *SessionManager
	runnerFactory *QueryRunnerFactory
	versionH      *metadata.VersionHandler
}

func newApiService(kv kv.KeyValueStore) *apiService {
	u := &apiService{
		kvStore:  kv,
		txMgr:    transaction.NewManager(kv),
		versionH: &metadata.VersionHandler{},
	}

	ctx := context.TODO()
	tx, err := u.txMgr.StartTx(ctx)
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
	u.encoder = metadata.NewEncoder(tenantMgr)
	u.cdcMgr = cdc.NewManager()
	u.sessions = NewSessionManager(u.txMgr, u.tenantMgr, u.versionH)
	u.runnerFactory = NewQueryRunnerFactory(u.txMgr, u.encoder, u.cdcMgr)
	return u
}

func (s *apiService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
		runtime.WithIncomingHeaderMatcher(middleware.CustomMatcher),
	)

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
	router.HandleFunc(apiPathPrefix+infoPath, func(w http.ResponseWriter, r *http.Request) {
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

	// explicit transactions needed to be tracked
	session, err := s.sessions.Create(ctx, true, true)
	if err != nil {
		return nil, err
	}

	return &api.BeginTransactionResponse{
		TxCtx: session.txCtx,
	}, nil
}

func (s *apiService) CommitTransaction(ctx context.Context, r *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	session := s.sessions.Get(r.TxCtx.GetId())
	if session == nil {
		return nil, api.Errorf(codes.NotFound, "session not found")
	}
	defer s.sessions.Remove(session.txCtx.Id)

	err := session.Commit(ctx, s.versionH, session.tx.Context().GetStagedDatabase() != nil, nil)
	if err != nil {
		return nil, err
	}

	return &api.CommitTransactionResponse{}, nil
}

func (s *apiService) RollbackTransaction(ctx context.Context, r *api.RollbackTransactionRequest) (*api.RollbackTransactionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	session := s.sessions.Get(r.TxCtx.GetId())
	if session == nil {
		return nil, api.Errorf(codes.NotFound, "session not found")
	}
	defer s.sessions.Remove(session.txCtx.Id)

	_ = session.Rollback(ctx)

	return &api.RollbackTransactionResponse{}, nil
}

// Insert new object returns an error if object already exists
// Operations done individually not in actual batch
func (s *apiService) Insert(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.runnerFactory.GetInsertQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.InsertResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
		Keys: resp.allKeys,
	}, nil
}

func (s *apiService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.runnerFactory.GetReplaceQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.ReplaceResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
		Keys: resp.allKeys,
	}, nil
}

func (s *apiService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.runnerFactory.GetUpdateQueryRunner(r),
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

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.runnerFactory.GetDeleteQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.DeleteResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			DeletedAt: resp.deletedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Read(r *api.ReadRequest, stream api.Tigris_ReadServer) error {
	if err := r.Validate(); err != nil {
		return err
	}

	_, err := s.sessions.Execute(stream.Context(), &ReqOptions{
		txCtx:       api.GetTransaction(r),
		queryRunner: s.runnerFactory.GetStreamingQueryRunner(r, stream),
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

	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetCreateOrUpdateCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:          api.GetTransaction(r),
		queryRunner:    runner,
		metadataChange: true,
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

	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetDropCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:          api.GetTransaction(r),
		queryRunner:    runner,
		metadataChange: true,
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

	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetListCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(r),
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

	queryRunner := s.runnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetListDatabaseReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
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

	queryRunner := s.runnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetCreateDatabaseReq(r)
	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		queryRunner:    queryRunner,
		metadataChange: true,
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

	queryRunner := s.runnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetDropDatabaseReq(r)
	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		queryRunner:    queryRunner,
		metadataChange: true,
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

	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetDescribeCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
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

	runner := s.runnerFactory.GetDatabaseQueryRunner()
	runner.SetDescribeDatabaseReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeDatabaseResponse), nil
}

func (s *apiService) GetInfo(_ context.Context, _ *api.GetInfoRequest) (*api.GetInfoResponse, error) {
	return &api.GetInfoResponse{
		ServerVersion: util.Version,
	}, nil
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

			for _, op := range tx.Ops {
				_, _, collection, ok := s.encoder.DecodeTableName(op.Table)
				if !ok {
					log.Err(err).Str("table", string(op.Table)).Msg("failed to decode collection name")
					return api.Error(codes.Internal, "failed to decode collection name")
				}

				if r.Collection == "" || r.Collection == collection {
					td, err := internal.Decode(op.Data)
					if err != nil {
						log.Err(err).Str("data", string(op.Data)).Msg("failed to decode data")
						return api.Error(codes.Internal, "failed to decode data")
					}

					event := &api.StreamEvent{
						TxId:       tx.Id,
						Collection: collection,
						Op:         op.Op,
						Key:        op.Key,
						Lkey:       op.LKey,
						Rkey:       op.RKey,
						Data:       td.RawData,
						Last:       op.Last,
					}

					response := &api.StreamResponse{
						Event: event,
					}

					if err := stream.Send(response); ulog.E(err) {
						return err
					}
				}
			}
		}
	}
}
