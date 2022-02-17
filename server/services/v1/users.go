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
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/encoding"
	"github.com/tigrisdata/tigrisdb/schema"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	databasePath        = "/databases"
	databasePathPattern = databasePath + "/*"

	collectionPath        = databasePath + "/collections"
	collectionPathPattern = collectionPath + "/*"

	documentPath        = collectionPath + "/documents"
	documentPathPattern = documentPath + "/*"
)

type userService struct {
	api.UnimplementedTigrisDBServer

	kv                    kv.KV
	txMgr                 *transaction.Manager
	encoder               encoding.Encoder
	schemaCache           *schema.Cache
	queryLifecycleFactory *QueryLifecycleFactory
	queryRunnerFactory    *QueryRunnerFactory
}

func newUserService(kv kv.KV) *userService {
	u := &userService{
		kv:          kv,
		txMgr:       transaction.NewManager(kv),
		schemaCache: schema.NewCache(),
		encoder:     &encoding.PrefixEncoder{},
	}

	u.queryLifecycleFactory = NewQueryLifecycleFactory()
	u.queryRunnerFactory = NewQueryRunnerFactory(u.txMgr, u.encoder)
	return u
}

func (s *userService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(string(JSON), &runtime.JSONBuiltin{}))

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

func (s *userService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterTigrisDBServer(grpc, s)
	return nil
}

func (s *userService) CreateCollection(ctx context.Context, r *api.CreateCollectionRequest) (*api.CreateCollectionResponse, error) {
	if c := s.schemaCache.Get(r.Db, r.Collection); c != nil {
		return nil, status.Errorf(codes.AlreadyExists, "collection already exists")
	}

	keys, err := schema.ExtractKeysFromSchema(r.Schema.Fields)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schema")
	}
	collection := schema.NewCollection(r.Db, r.Collection, keys)

	if err := s.kv.CreateTable(ctx, collection.StorageName()); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}
	s.schemaCache.Put(collection)

	return &api.CreateCollectionResponse{
		Msg: "collection created successfully",
	}, nil
}

func (s *userService) DropCollection(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
	if err := s.kv.DropTable(ctx, schema.StorageName(r.GetDb(), r.GetCollection())); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	s.schemaCache.Remove(r.GetDb(), r.GetCollection())

	return &api.DropCollectionResponse{
		Msg: "collection dropped successfully",
	}, nil
}

func (s *userService) BeginTransaction(ctx context.Context, r *api.BeginTransactionRequest) (*api.BeginTransactionResponse, error) {
	_, txCtx, err := s.txMgr.StartTx(ctx, true)
	if err != nil {
		return nil, err
	}

	return &api.BeginTransactionResponse{
		TxCtx: txCtx,
	}, nil
}

func (s *userService) CommitTransaction(ctx context.Context, r *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
	tx, err := s.txMgr.GetTx(r.TxCtx)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &api.CommitTransactionResponse{}, nil
}

func (s *userService) RollbackTransaction(ctx context.Context, r *api.RollbackTransactionRequest) (*api.RollbackTransactionResponse, error) {
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
func (s *userService) Insert(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}

	_, err := s.Run(ctx, &Request{
		Request:     r,
		documents:   r.Documents,
		collection:  collection,
		queryRunner: s.queryRunnerFactory.GetTxQueryRunner(),
	})
	if err != nil {
		return nil, err
	}

	return &api.InsertResponse{}, nil
}

// Update performs full body replace of existing object
// Operations done individually not in actual batch
func (s *userService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}

	_, err := s.Run(ctx, &Request{
		Request:     r,
		collection:  collection,
		queryRunner: s.queryRunnerFactory.GetTxQueryRunner(),
	})
	if err != nil {
		return nil, err
	}
	return &api.UpdateResponse{}, nil
}

func (s *userService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}

	_, err := s.Run(ctx, &Request{
		Request:     r,
		documents:   r.Documents,
		collection:  collection,
		queryRunner: s.queryRunnerFactory.GetTxQueryRunner(),
	})
	if err != nil {
		return nil, err
	}

	return &api.ReplaceResponse{}, nil
}

func (s *userService) Delete(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}

	_, err := s.Run(ctx, &Request{
		Request:     r,
		collection:  collection,
		queryRunner: s.queryRunnerFactory.GetTxQueryRunner(),
	})
	if err != nil {
		return nil, err
	}

	return &api.DeleteResponse{}, nil
}

func (s *userService) Read(r *api.ReadRequest, stream api.TigrisDB_ReadServer) error {
	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}

	_, err := s.Run(stream.Context(), &Request{
		Request:     r,
		keys:        r.Keys,
		collection:  collection,
		queryRunner: s.queryRunnerFactory.GetStreamingQueryRunner(stream),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *userService) ListDatabases(ctx context.Context, r *api.ListDatabasesRequest) (*api.ListDatabasesResponse, error) {
	return &api.ListDatabasesResponse{}, nil
}

func (s *userService) ListCollections(ctx context.Context, r *api.ListCollectionsRequest) (*api.ListCollectionsResponse, error) {
	return &api.ListCollectionsResponse{}, nil
}

func (s *userService) CreateDatabase(ctx context.Context, r *api.CreateDatabaseRequest) (*api.CreateDatabaseResponse, error) {
	return &api.CreateDatabaseResponse{
		Msg: "database created successfully",
	}, nil
}

func (s *userService) DropDatabase(ctx context.Context, r *api.DropDatabaseRequest) (*api.DropDatabaseResponse, error) {
	return &api.DropDatabaseResponse{
		Msg: "database dropped successfully",
	}, nil
}

func (s *userService) Run(ctx context.Context, req *Request) (*Response, error) {
	queryLifecycle := s.queryLifecycleFactory.Get()
	return queryLifecycle.run(ctx, req)
}
