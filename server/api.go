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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/tigrisdata/tigrisdb/util"

	"github.com/davecgh/go-spew/spew"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"github.com/tigrisdata/tigrisdb/types"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// allows to serve GRPC and HTTP on the same port
func combinedRouter(grpcServer *grpc.Server, httpHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 || !strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			httpHandler.ServeHTTP(w, r)
		} else {
			grpcServer.ServeHTTP(w, r)
		}
	})
}

// ServeAPI initializes and start serving HTTP and GRPC APIs
func ServeAPI(host string, GRPCPort int16, HTTPPort int16, kv kv.KV) error {
	log.Info().Str("host", host).Int16("GRPCPort", GRPCPort).Msg("Initializing GRPC server")

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, GRPCPort))
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	//service.RegisterChannelzServiceToServer(grpcServer)

	idx, err := NewIndexStore(kv)
	if err != nil {
		return err
	}

	api.RegisterIndexAPIServer(grpcServer, &indexAPI{idx: idx})
	api.RegisterTigrisDBServer(grpcServer, &userAPI{kv: kv})

	go func() {
		err := grpcServer.Serve(l)
		log.Err(err).Msg("GRPC server exited")
	}()

	log.Info().Str("host", host).Int16("HTTPPort", HTTPPort).Msg("Initializing HTTP server")

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	//r.Use(middleware.Compress(6, "gzip"))
	indexMux := runtime.NewServeMux(runtime.WithMarshalerOption("application/json", &util.JSONMix{}))
	userMux := runtime.NewServeMux(runtime.WithMarshalerOption("application/json", &util.JSONMix{}))

	if err = api.RegisterIndexAPIHandlerServer(context.TODO(), indexMux, &indexAPI{idx: idx}); err != nil {
		return err
	}

	if err = api.RegisterTigrisDBHandlerServer(context.TODO(), userMux, &userAPI{kv: kv}); err != nil {
		return err
	}

	r.HandleFunc("/v1/index/*", func(w http.ResponseWriter, r *http.Request) {
		indexMux.ServeHTTP(w, r)
	})

	r.HandleFunc("/v1/table/*", func(w http.ResponseWriter, r *http.Request) {
		userMux.ServeHTTP(w, r)
	})

	r.HandleFunc("/v1/crud/*", func(w http.ResponseWriter, r *http.Request) {
		userMux.ServeHTTP(w, r)
	})

	return http.ListenAndServe(fmt.Sprintf(":%d", HTTPPort), combinedRouter(grpcServer, r))
}

type indexAPI struct {
	idx *index
	api.UnimplementedIndexAPIServer
}

func (i *indexAPI) UpdateIndex(ctx context.Context, r *api.UpdateIndexRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Str("index", r.GetIndex()).Msg("UpdateIndex")

	spew.Dump(r)

	name := GetIndexName(r.GetDb(), r.GetTable(), r.GetIndex())

	if err := i.idx.ReplaceMicroShardFile(ctx, name, r.GetOld(), r.GetNew()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &api.TigrisDBResponse{}, nil
}

func (i *indexAPI) ReadIndex(ctx context.Context, r *api.ReadIndexRequest) (*api.ReadIndexResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Str("index", r.GetIndex()).Str("min_key", string(r.GetMinKey())).Str("max_key", string(r.GetMinKey())).Msg("ReadIndex")

	name := GetIndexName(r.GetDb(), r.GetTable(), r.GetIndex())
	shards, err := i.idx.ReadIndex(ctx, name, r.GetMinKey(), r.GetMaxKey())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &api.ReadIndexResponse{Shards: shards}, nil
}

func (i *indexAPI) PatchPrimaryIndex(ctx context.Context, r *api.PatchPrimaryIndexRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msg("PatchPrimaryIndex")

	name := GetTableName(r.GetDb(), r.GetTable())
	if err := i.idx.PatchPrimaryIndex(ctx, name, r.Entries); err != nil {
		return nil, err
	}
	return &api.TigrisDBResponse{}, nil
}

type userAPI struct {
	kv kv.KV
	api.UnimplementedTigrisDBServer
}

func (s *userAPI) CreateTable(ctx context.Context, r *api.TigrisDBRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("CreateTable")

	name := GetTableName(r.GetDb(), r.GetTable())
	AddTable(name, r.GetKey())

	if err := s.kv.CreateTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	name = GetIndexName(r.GetDb(), r.GetTable(), ClusteringIndexName)
	if err := s.kv.CreateTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

func (s *userAPI) DropTable(ctx context.Context, r *api.TigrisDBRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("DropTable")

	name := GetTableName(r.GetDb(), r.GetTable())
	if err := s.kv.DropTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	name = GetIndexName(r.GetDb(), r.GetTable(), ClusteringIndexName)
	if err := s.kv.DropTable(ctx, name); ulog.E(err) {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

func (s *userAPI) getKey(table string, v *api.TigrisDBDoc) (types.Key, error) {
	var key types.Key
	// Use key if it set in the request
	if v.GetPrimaryKey() != nil && len(v.GetPrimaryKey()) > 0 {
		key = types.NewUserKey(v.GetPrimaryKey(), v.GetPartitionKey())
	} else {
		// otherwise build from the values of key fields of the document
		k := GetTableKey(table)
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

func (s *userAPI) processBatch(ctx context.Context, table string, docs []*api.TigrisDBDoc, fn func(b kv.Tx, key types.Key, value []byte) error) error {
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

func (s *userAPI) Upsert(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	return s.Replace(ctx, r)
}

// Insert new object
// returns error if object already exists
// Operations done individually not in actual batch
func (s *userAPI) Insert(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Insert")

	table := GetTableName(r.GetDb(), r.GetTable())
	if err := s.processBatch(ctx, table, r.GetDocs(), func(b kv.Tx, key types.Key, value []byte) error {
		return s.kv.Insert(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

// Update performs full body replace of existing object
// Operations done individually not in actual batch
func (s *userAPI) Update(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Insert")

	table := GetTableName(r.GetDb(), r.GetTable())
	if err := s.processBatch(ctx, table, r.GetDocs(), func(b kv.Tx, key types.Key, value []byte) error {
		return s.kv.Update(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

func (s *userAPI) Replace(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Replace")

	table := GetTableName(r.GetDb(), r.GetTable())
	if err := s.processBatch(ctx, table, r.GetDocs(), func(b kv.Tx, key types.Key, value []byte) error {
		return b.Replace(ctx, table, key, value)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Replace after")

	return &api.TigrisDBResponse{}, nil
}

func (s *userAPI) Delete(ctx context.Context, r *api.TigrisDBCRUDRequest) (*api.TigrisDBResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Delete")

	table := GetTableName(r.GetDb(), r.GetTable())
	if err := s.processBatch(ctx, table, r.GetDocs(), func(b kv.Tx, key types.Key, value []byte) error {
		return b.Delete(ctx, table, key)
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "error: %v", err)
	}

	return &api.TigrisDBResponse{}, nil
}

func offloaded(o *TigrisDBObject) bool {
	return o.TigrisDB.Metadata.MicroShard != nil
}

func (s *userAPI) Read(r *api.TigrisDBCRUDRequest, stream api.TigrisDB_ReadServer) error {
	log.Debug().Str("db", r.GetDb()).Str("table", r.GetTable()).Msgf("Read")

	name := GetTableName(r.GetDb(), r.GetTable())
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
			o := TigrisDBObject{}
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
