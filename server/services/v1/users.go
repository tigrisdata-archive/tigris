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
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"

	"github.com/davecgh/go-spew/spew"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/indexing"
	"github.com/tigrisdata/tigrisdb/server/schemas"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"github.com/tigrisdata/tigrisdb/types"
	"github.com/tigrisdata/tigrisdb/util"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (s *userService) RegisterHTTP(router chi.Router) error {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(string(JSON), &util.JSONMix{}))
	if err := api.RegisterTigrisDBHandlerServer(context.TODO(), mux, s); err != nil {
		return err
	}

	router.HandleFunc("/v1/table/*", func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc("/v1/crud/*", func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (s *userService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterTigrisDBServer(grpc, s)
	return nil
}

func (s *userService) CreateTable(ctx context.Context, r *api.TigrisDBRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("CreateTable")

	name := schemas.GetTableName(r.GetDb(), r.GetTable())
	schemas.AddTable(name, r.GetKey())

	if err := s.kv.CreateTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	name = schemas.GetIndexName(r.GetDb(), r.GetTable(), schemas.ClusteringIndexName)
	if err := s.kv.CreateTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

func (s *userService) DropTable(ctx context.Context, r *api.TigrisDBRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("DropTable")

	name := schemas.GetTableName(r.GetDb(), r.GetTable())
	if err := s.kv.DropTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	name = schemas.GetIndexName(r.GetDb(), r.GetTable(), schemas.ClusteringIndexName)
	if err := s.kv.DropTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

func (s *userService) getKey(table string, v *api.TigrisDBDoc) (types.Key, error) {
	var key types.Key
	// Use key if it set in the request
	if v.GetPrimaryKey() != nil && len(v.GetPrimaryKey()) > 0 {
		key = types.NewUserKey(v.GetPrimaryKey(), v.GetPartitionKey())
	} else {
		// otherwise build from the values of key fields of the document
		k := schemas.GetTableKey(table)
		doc := make(map[string]interface{})
		if err := json.Unmarshal(v.GetValue(), &doc); err != nil {
			return nil, err
		}
		log.Debug().Interface("keys", doc).Msg("doc")
		var err error
		key, err = buildKey(doc, k)
		if err != nil {
			return nil, err
		}
		spew.Dump(doc)
		log.Debug().Interface("keys", key).Msg("key")
	}
	return key, nil
}

func (s *userService) processBatch(ctx context.Context, table string, docs []*api.TigrisDBDoc, fn func(b kv.Tx, key types.Key, value []byte) error) error {
	b := s.kv.Batch()

	for _, v := range docs {
		key, err := s.getKey(table, v)
		if err != nil {
			return err
		}
		if err := fn(b, key, v.GetValue()); ulog.E(err) {
			return err
		}
	}

	if err := b.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (s *userService) Upsert(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	return s.Replace(ctx, r)
}

// Insert new object
// returns error if object already exists
// Operations done individually not in actual batch
func (s *userService) Insert(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Insert")

	table := schemas.GetTableName(r.GetDb(), r.GetTable())
	if err := s.processBatch(ctx, table, r.GetDocs(), func(b kv.Tx, key types.Key, value []byte) error {
		return s.kv.Insert(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

// Update performs full body replace of existing object
// Operations done individually not in actual batch
func (s *userService) Update(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Insert")

	table := schemas.GetTableName(r.GetDb(), r.GetTable())
	if err := s.processBatch(ctx, table, r.GetDocs(), func(b kv.Tx, key types.Key, value []byte) error {
		return s.kv.Update(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

func (s *userService) Replace(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Replace")

	table := schemas.GetTableName(r.GetDb(), r.GetTable())
	if err := s.processBatch(ctx, table, r.GetDocs(), func(b kv.Tx, key types.Key, value []byte) error {
		return b.Replace(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Replace after")

	return &api.TigrisDBResponse{}, nil
}

func (s *userService) Delete(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Delete")

	table := schemas.GetTableName(r.GetDb(), r.GetTable())
	if err := s.processBatch(ctx, table, r.GetDocs(), func(b kv.Tx, key types.Key, value []byte) error {
		return b.Delete(ctx, table, key)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

func offloaded(o *indexing.TigrisDBObject) bool {
	return o.TigrisDB.Metadata.MicroShard != nil
}

func (s *userService) Read(r *api.TigrisDBCRUDRequest, stream api.TigrisDB_ReadServer) error {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Read")

	name := schemas.GetTableName(r.GetDb(), r.GetTable())
	for _, v := range r.Docs {
		key, err := s.getKey(name, v)
		if err != nil {
			return err
		}
		docs, err := s.kv.Read(context.TODO(), name, key)
		if err != nil {
			return err
		}
		for _, d := range docs {
			o := indexing.TigrisDBObject{}
			if err := json.Unmarshal(d.Value, &o); ulog.E(err) {
				return err
			}
			if offloaded(&o) {
				log.Debug().Stringer("key", d.Key).Msg("Offloaded object")
			}
			if err := stream.Send(&api.TigrisDBDoc{Value: d.Value}); ulog.E(err) {
				return err
			}
		}
	}
	return nil
}

func buildKey(fields map[string]interface{}, key []string) (types.Key, error) {
	b := make([]byte, 0)
	var p []byte
	for i, v := range key {
		k, ok := fields[v]
		if !ok {
			return nil, ulog.CE("primary key part is no present in the doc: %s", v)
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
	log.Debug().Str("primary", string(b)).Str("part", string(p)).Msg("aaaaa")
	return types.NewUserKey(b, p), nil
}
