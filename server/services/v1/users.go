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

	"github.com/davecgh/go-spew/spew"
	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/schema"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	databasePath        = "/databases/{db}"
	databasePathPattern = databasePath + "/*"

	collectionPath        = databasePath + "/collections/{collection}"
	collectionPathPattern = collectionPath + "/*"

	documentPath        = collectionPath + "/documents"
	documentPathPattern = documentPath + "/*"
)

type userService struct {
	api.UnimplementedTigrisDBServer

	kv          kv.KV
	tx          *transaction.Manager
	schemaCache *schema.Cache
}

func newUserService(kv kv.KV) *userService {
	return &userService{
		kv:          kv,
		tx:          transaction.NewTransactionMgr(),
		schemaCache: schema.NewCache(),
	}
}

func (s *userService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(string(JSON), &runtime.JSONBuiltin{}))
	if err := api.RegisterTigrisDBHandlerServer(context.TODO(), mux, s); err != nil {
		return err
	}

	if err := api.RegisterTigrisDBHandlerClient(context.TODO(), mux, api.NewTigrisDBClient(inproc)); err != nil {
		return err
	}

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

func (s *userService) RegisterGRPC(grpc *grpc.Server, inproc *inprocgrpc.Channel) error {
	api.RegisterTigrisDBServer(grpc, s)
	api.RegisterTigrisDBServer(inproc, s)
	return nil
}

func (s *userService) CreateCollection(ctx context.Context, r *api.CreateCollectionRequest) (*api.CreateCollectionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	if tbl := s.schemaCache.Get(r.GetDb(), r.GetCollection()); tbl != nil {
		return nil, status.Errorf(codes.AlreadyExists, "collection already exists")
	}

	keys, err := schema.ExtractKeysFromSchema(r.Schema.Fields)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schema")
	}
	collection := schema.NewCollection(r.GetDb(), r.GetCollection(), keys)

	if err := s.kv.CreateTable(ctx, collection.StorageName()); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}
	s.schemaCache.Put(collection)

	return &api.CreateCollectionResponse{
		Msg: "collection created successfully",
	}, nil
}

func (s *userService) DropCollection(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	if err := s.kv.DropTable(ctx, schema.StorageName(r.GetDb(), r.GetCollection())); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}
	s.schemaCache.Remove(r.GetDb(), r.GetCollection())

	return &api.DropCollectionResponse{
		Msg: "collection dropped successfully",
	}, nil
}

func (s *userService) BeginTransaction(_ context.Context, _ *api.BeginTransactionRequest) (*api.BeginTransactionResponse, error) {
	tx, err := s.tx.StartTxn()
	if err != nil {
		return nil, err
	}

	return &api.BeginTransactionResponse{
		Tx: tx,
	}, nil
}

func (s *userService) CommitTransaction(_ context.Context, r *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
	if err := s.tx.EndTxn(r.Tx.Id, true); err != nil {
		return nil, err
	}

	return &api.CommitTransactionResponse{}, nil
}

func (s *userService) RollbackTransaction(_ context.Context, r *api.RollbackTransactionRequest) (*api.RollbackTransactionResponse, error) {
	if err := s.tx.EndTxn(r.Tx.Id, false); err != nil {
		return nil, err
	}

	return &api.RollbackTransactionResponse{}, nil
}

// Insert new object returns an error if object already exists
// Operations done individually not in actual batch
func (s *userService) Insert(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}
	if err := s.processBatch(ctx, collection, r.GetDocuments(), func(b kv.Tx, key kv.Key, value []byte) error {
		return s.kv.Insert(ctx, collection.StorageName(), key, value, true)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.InsertResponse{}, nil
}

// Update performs full body replace of existing object
// Operations done individually not in actual batch
func (s *userService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}
	if err := s.processBatch(ctx, collection, nil, func(b kv.Tx, key kv.Key, value []byte) error {
		return s.kv.UpdateRange(ctx, collection.StorageName(), key, key, func(in []byte) []byte {
			// FIXME: Merge in + value
			return value
		})
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.UpdateResponse{}, nil
}

func (s *userService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}
	if err := s.processBatch(ctx, collection, r.GetDocuments(), func(b kv.Tx, key kv.Key, value []byte) error {
		return b.Insert(ctx, collection.StorageName(), key, value, false)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	log.Debug().Str("db", r.GetDb()).Str("table", r.GetCollection()).Msgf("Replace after")

	return &api.ReplaceResponse{}, nil
}

func (s *userService) Delete(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}
	if err := s.processBatch(ctx, collection, nil, func(b kv.Tx, key kv.Key, value []byte) error {
		return b.Delete(ctx, collection.StorageName(), key)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.DeleteResponse{}, nil
}

func (s *userService) Read(r *api.ReadRequest, stream api.TigrisDB_ReadServer) error {
	if err := r.Validate(); err != nil {
		return err
	}

	collection := s.schemaCache.Get(r.GetDb(), r.GetCollection())
	if collection == nil {
		return status.Errorf(codes.InvalidArgument, "collection doesn't exists")
	}

	for _, v := range r.GetKeys() {
		key, err := s.getKey(collection, v)
		if err != nil {
			return err
		}

		it, err := s.kv.Read(context.TODO(), collection.StorageName(), key)
		if err != nil {
			return err
		}
		for it.More() {
			d, err := it.Next()
			if err != nil {
				return err
			}
			s := &structpb.Struct{}
			err = protojson.Unmarshal(d.Value, s)
			if err != nil {
				return err
			}
			if err := stream.Send(&api.ReadResponse{
				Doc: s,
			}); ulog.E(err) {
				return err
			}
		}
	}
	return nil
}

func (s *userService) getKey(collection schema.Collection, v *api.UserDocument) (kv.Key, error) {
	var key kv.Key
	pkey := collection.PrimaryKeys()
	doc := v.Doc.AsMap()

	var k []string
	for _, p := range pkey {
		k = append(k, p.FieldName)
	}
	var err error
	if key, err = buildKey(doc, k); err != nil {
		return nil, err
	}
	spew.Dump(doc)
	spew.Dump(key)
	return key, nil
}

func (s *userService) processBatch(ctx context.Context, collection schema.Collection, docs []*api.UserDocument, fn func(b kv.Tx, key kv.Key, value []byte) error) error {
	b, err := s.kv.Batch()
	if err != nil {
		return err
	}

	for _, v := range docs {
		key, err := s.getKey(collection, v)
		if err != nil {
			log.Debug().Err(err).Msg("get key error")
			return err
		}

		marshalled, err := v.GetDoc().MarshalJSON()
		if err != nil {
			log.Debug().Err(err).Msg("marshaling fail for the document")
			return err
		}
		if err := fn(b, key, marshalled); ulog.E(err) {
			log.Debug().Err(err).Msg("fn error")
			return err
		}
	}

	if err := b.Commit(ctx); err != nil {
		log.Debug().Err(err).Msg("commit error")
		return err
	}

	return nil
}

func buildKey(fields map[string]interface{}, key []string) (kv.Key, error) {
	nKey := kv.Key{}
	for _, v := range key {
		k, ok := fields[v]
		if !ok {
			return nil, ulog.CE("missing primary key column(s) %s", v)
		}

		nKey.AddPart(k)
	}
	if len(nKey) == 0 {
		return nil, ulog.CE("missing primary key column(s)")
	}
	return nKey, nil
}
