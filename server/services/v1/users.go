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
	"github.com/tigrisdata/tigrisdb/server/schemas"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"github.com/tigrisdata/tigrisdb/types"
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

	kv kv.KV
}

func newUserService(kv kv.KV) *userService {
	return &userService{
		kv: kv,
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
	if err := isValidCollectionAndDatabase(r.GetCollection(), r.GetDb()); err != nil {
		return nil, err
	}

	name := schemas.GetTableName(r.GetDb(), r.GetCollection())
	if tbl := schemas.GetTable(name); tbl != nil {
		return nil, status.Errorf(codes.AlreadyExists, "collection already exists")
	}
	if r.CreateBody == nil {
		return nil, status.Errorf(codes.InvalidArgument, "collection request body is missing")
	}
	sch := r.GetCreateBody().GetSchema()
	if sch == nil {
		return nil, status.Errorf(codes.InvalidArgument, "schema is a required during collection creation")
	}

	keys, err := schemas.ExtractKeysFromSchema(sch.Fields)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schema")
	}

	if err := s.kv.CreateTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}
	schemas.AddTable(name, keys)

	return &api.CreateCollectionResponse{
		Msg: "collection created successfully",
	}, nil
}

func (s *userService) DropCollection(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
	if err := isValidCollectionAndDatabase(r.GetCollection(), r.GetDb()); err != nil {
		return nil, err
	}

	name := schemas.GetTableName(r.GetDb(), r.GetCollection())
	if err := s.kv.DropTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}
	schemas.RemoveTable(name)

	return &api.DropCollectionResponse{
		Msg: "collection dropped successfully",
	}, nil
}

// Insert new object returns an error if object already exists
// Operations done individually not in actual batch
func (s *userService) Insert(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
	if err := isValidCollectionAndDatabase(r.GetCollection(), r.GetDb()); err != nil {
		return nil, err
	}

	if r.InsertBody == nil {
		return nil, status.Errorf(codes.InvalidArgument, "insert request body is missing")
	}
	docs := r.GetInsertBody().GetDocuments()
	if len(docs) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "empty documents received")
	}

	table := schemas.GetTableName(r.GetDb(), r.GetCollection())
	if err := s.processBatch(ctx, table, docs, func(b kv.Tx, key types.Key, value []byte) error {
		return s.kv.Insert(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.InsertResponse{}, nil
}

// Update performs full body replace of existing object
// Operations done individually not in actual batch
func (s *userService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	if err := isValidCollectionAndDatabase(r.GetCollection(), r.GetDb()); err != nil {
		return nil, err
	}

	table := schemas.GetTableName(r.GetDb(), r.GetCollection())
	if err := s.processBatch(ctx, table, nil, func(b kv.Tx, key types.Key, value []byte) error {
		return s.kv.Update(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.UpdateResponse{}, nil
}

func (s *userService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	if err := isValidCollectionAndDatabase(r.GetCollection(), r.GetDb()); err != nil {
		return nil, err
	}

	table := schemas.GetTableName(r.GetDb(), r.GetCollection())
	if err := s.processBatch(ctx, table, r.GetReplaceBody().GetDocuments(), func(b kv.Tx, key types.Key, value []byte) error {
		return b.Replace(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	log.Debug().Str("db", r.GetDb()).Str("table", r.GetCollection()).Msgf("Replace after")

	return &api.ReplaceResponse{}, nil
}

func (s *userService) Delete(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
	if err := isValidCollectionAndDatabase(r.GetCollection(), r.GetDb()); err != nil {
		return nil, err
	}

	table := schemas.GetTableName(r.GetDb(), r.GetCollection())
	if err := s.processBatch(ctx, table, nil, func(b kv.Tx, key types.Key, value []byte) error {
		return b.Delete(ctx, table, key)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.DeleteResponse{}, nil
}

func (s *userService) Read(r *api.ReadRequest, stream api.TigrisDB_ReadServer) error {
	if err := isValidCollectionAndDatabase(r.GetCollection(), r.GetDb()); err != nil {
		return err
	}
	if r.ReadBody == nil {
		return status.Errorf(codes.InvalidArgument, "read request body is missing")
	}

	name := schemas.GetTableName(r.GetDb(), r.GetCollection())
	for _, v := range r.GetReadBody().GetKeys() {
		key, err := s.getKey(name, v)
		if err != nil {
			return err
		}

		docs, err := s.kv.Read(context.TODO(), name, key)
		if err != nil {
			return err
		}
		for _, d := range docs {
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

func (s *userService) getKey(table string, v *api.UserDocument) (types.Key, error) {
	var key types.Key
	k := schemas.GetTableKey(table)
	doc := v.Doc.AsMap()

	var err error
	if key, err = buildKey(doc, k); err != nil {
		return nil, err
	}
	spew.Dump(doc)
	return key, nil
}

func (s *userService) processBatch(ctx context.Context, table string, docs []*api.UserDocument, fn func(b kv.Tx, key types.Key, value []byte) error) error {
	b := s.kv.Batch()

	for _, v := range docs {
		key, err := s.getKey(table, v)
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

func buildKey(fields map[string]interface{}, key []string) (types.Key, error) {
	b := make([]byte, 0)
	var p []byte
	for i, v := range key {
		k, ok := fields[v]
		if !ok {
			return nil, ulog.CE("missing primary key column(s) %s", v)
		}

		switch t := k.(type) {
		case int:
			b = types.EncodeIntKey(uint64(t), b)
		case uint64:
			b = types.EncodeIntKey(t, b)
		case float64:
			b = types.EncodeIntKey(uint64(t), b)
		case []byte:
			b = types.EncodeBinaryKey(t, b)
		case string:
			b = types.EncodeBinaryKey([]byte(t), b)
		}
		if i == 0 {
			p = make([]byte, len(b))
			copy(p, b)
		}
	}
	if len(b) == 0 {
		return nil, ulog.CE("missing primary key column(s)")
	}
	return types.NewUserKey(b, p), nil
}
