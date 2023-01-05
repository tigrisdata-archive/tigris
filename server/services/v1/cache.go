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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/cache"
	"github.com/tigrisdata/tigris/server/transaction"
	cache2 "github.com/tigrisdata/tigris/store/cache"
	"google.golang.org/grpc"
)

const (
	cachePattern = "/" + version + "/projects/*"
)

type cacheService struct {
	api.UnimplementedCacheServer

	sessions      cache.Session
	runnerFactory *cache.RunnerFactory
}

func newCacheService(tenantMgr *metadata.TenantManager, txMgr *transaction.Manager, versionH *metadata.VersionHandler) *cacheService {
	cacheSessions := cache.NewSessionManager(txMgr, tenantMgr, versionH, metadata.NewCacheTracker(tenantMgr, txMgr))
	return &cacheService{
		UnimplementedCacheServer: api.UnimplementedCacheServer{},
		sessions:                 cacheSessions,
		runnerFactory:            cache.NewRunnerFactory(metadata.NewCacheEncoder(), cache2.NewCache(&config.DefaultConfig.Cache)),
	}
}

func (c *cacheService) CreateCache(ctx context.Context, req *api.CreateCacheRequest) (*api.CreateCacheResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	resp, err := c.sessions.TxExecute(ctx, c.runnerFactory.GetCreateCacheRunner(req, accessToken))
	if err != nil {
		return nil, err
	}
	return &api.CreateCacheResponse{
		Status:  resp.Status,
		Message: "Cache is created successfully",
	}, nil
}

func (c *cacheService) DeleteCache(ctx context.Context, req *api.DeleteCacheRequest) (*api.DeleteCacheResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	resp, err := c.sessions.TxExecute(ctx, c.runnerFactory.GetDeleteCacheRunner(req, accessToken))
	if err != nil {
		return nil, err
	}
	return &api.DeleteCacheResponse{
		Status:  resp.Status,
		Message: "Cache is deleted successfully",
	}, nil
}

func (c *cacheService) ListCaches(ctx context.Context, req *api.ListCachesRequest) (*api.ListCachesResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)

	resp, err := c.sessions.TxExecute(ctx, c.runnerFactory.GetListCachesRunner(req, accessToken))
	if err != nil {
		return nil, err
	}
	return &api.ListCachesResponse{
		Caches: resp.Caches,
	}, nil
}

func (c *cacheService) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	resp, err := c.sessions.Execute(ctx, c.runnerFactory.GetSetRunner(req, accessToken))
	if err != nil {
		return nil, err
	}
	return &api.SetResponse{
		Status:  resp.Status,
		Message: "Key is set successfully",
	}, nil
}

func (c *cacheService) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	resp, err := c.sessions.Execute(ctx, c.runnerFactory.GetGetRunner(req, accessToken))
	if err != nil {
		return nil, err
	}
	return &api.GetResponse{
		Value: resp.Data,
	}, nil
}

func (c *cacheService) Del(ctx context.Context, req *api.DelRequest) (*api.DelResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	resp, err := c.sessions.Execute(ctx, c.runnerFactory.GetDelRunner(req, accessToken))
	if err != nil {
		return nil, err
	}
	return &api.DelResponse{
		Status:  resp.Status,
		Message: fmt.Sprintf("Entries deleted count# %d", resp.DeletedCount),
	}, nil
}

func (c *cacheService) Keys(ctx context.Context, req *api.KeysRequest) (*api.KeysResponse, error) {
	accessToken, _ := request.GetAccessToken(ctx)
	resp, err := c.sessions.Execute(ctx, c.runnerFactory.GetKeysRunner(req, accessToken))
	if err != nil {
		return nil, err
	}
	return &api.KeysResponse{
		Keys: resp.Keys,
	}, nil
}

func (c *cacheService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterCacheHandlerClient(context.TODO(), mux, api.NewCacheClient(inproc)); err != nil {
		return err
	}

	api.RegisterCacheServer(inproc, c)
	router.HandleFunc(cachePattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (c *cacheService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterCacheServer(grpc, c)
	return nil
}
