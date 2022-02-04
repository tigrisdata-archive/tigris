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
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/indexing"
	"github.com/tigrisdata/tigrisdb/server/schemas"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type indexService struct {
	api.UnimplementedIndexAPIServer

	idx *indexing.Index
}

func newIndexService(kv kv.KV) *indexService {
	idx, _ := indexing.NewIndexStore(kv)
	return &indexService{
		idx: idx,
	}
}

func (a *indexService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(string(JSON), &runtime.JSONBuiltin{}))
	if err := api.RegisterIndexAPIHandlerServer(context.TODO(), mux, a); err != nil {
		return err
	}
	router.HandleFunc("/api/v1/index/*", func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})

	return nil
}

func (a *indexService) RegisterGRPC(grpc *grpc.Server, inproc *inprocgrpc.Channel) error {
	api.RegisterIndexAPIServer(grpc, a)
	return nil
}

func (a *indexService) UpdateIndex(ctx context.Context, r *api.UpdateIndexRequest) (*api.WriteIndexResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("collection", r.GetCollection()).Str("index", r.GetIndex()).Msg("UpdateIndex")

	spew.Dump(r)

	name := schemas.GetIndexName(r.GetDb(), r.GetCollection(), r.GetIndex())

	if err := a.idx.ReplaceMicroShardFile(ctx, name, r.GetOld(), r.GetNew()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &api.WriteIndexResponse{}, nil
}

func (a *indexService) ReadIndex(ctx context.Context, r *api.ReadIndexRequest) (*api.ReadIndexResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("collection", r.GetCollection()).Str("index", r.GetIndex()).Str("min_key", string(r.GetMinKey())).Str("max_key", string(r.GetMinKey())).Msg("ReadIndex")

	name := schemas.GetIndexName(r.GetDb(), r.GetCollection(), r.GetIndex())
	shards, err := a.idx.ReadIndex(ctx, name, r.GetMinKey(), r.GetMaxKey())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &api.ReadIndexResponse{Shards: shards}, nil
}

func (a *indexService) PatchPrimaryIndex(ctx context.Context, r *api.PatchPrimaryIndexRequest) (*api.WriteIndexResponse, error) {
	log.Debug().Str("db", r.GetDb()).Str("collection", r.GetCollection()).Msg("PatchPrimaryIndex")

	name := schemas.GetTableName(r.GetDb(), r.GetCollection())
	if err := a.idx.PatchPrimaryIndex(ctx, name, r.Entries); err != nil {
		return nil, err
	}
	return &api.WriteIndexResponse{}, nil
}
