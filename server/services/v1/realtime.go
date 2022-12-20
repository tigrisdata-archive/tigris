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
	"fmt"
	"net/http"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/services/v1/realtime"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/cache"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"google.golang.org/grpc"
)

const (
	realtimePathPattern = fullProjectPath + "/realtime/*"
)

type realtimeService struct {
	api.UnimplementedRealtimeServer

	cache cache.Cache
}

func newRealtimeService(_ kv.KeyValueStore, _ search.Store, _ *metadata.TenantManager, _ *transaction.Manager) *realtimeService {
	cacheS := cache.NewCache(&config.DefaultConfig.Cache)
	return &realtimeService{
		cache: cacheS,
	}
}

func (s *realtimeService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
		runtime.WithIncomingHeaderMatcher(api.CustomMatcher),
		runtime.WithOutgoingHeaderMatcher(api.CustomMatcher),
	)

	if err := api.RegisterRealtimeHandlerClient(context.TODO(), mux, api.NewRealtimeClient(inproc)); err != nil {
		return err
	}

	api.RegisterRealtimeServer(inproc, s)

	router.HandleFunc(apiPathPrefix+"/projects/{project}/realtime", s.DeviceConnectionHandler)
	router.HandleFunc(apiPathPrefix+realtimePathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})

	return nil
}

func (s *realtimeService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterRealtimeServer(grpc, s)
	return nil
}

var upgradeToSocket = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (s *realtimeService) extractConnParams(r *http.Request) realtime.ConnectionParams {
	var params realtime.ConnectionParams

	// project name is part of path
	params.ProjectName = chi.URLParam(r, "project")

	// query params
	params.SessionId = r.URL.Query().Get("session_id")

	return params
}

func (s *realtimeService) DeviceConnectionHandler(w http.ResponseWriter, r *http.Request) {
	params := s.extractConnParams(r)
	conn, err := upgradeToSocket.Upgrade(w, r, nil)
	if err != nil {
		// ToDo: Change to WS errors
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error": "%s"}`, err.Error())))
		return
	}

	log.Debug().Msgf("params '%v'", params)
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"error": "not implemented"}`))
	_ = conn.Close()
}

func (s *realtimeService) Ping(_ context.Context, _ *api.HeartbeatEvent) (*api.HeartbeatEvent, error) {
	return &api.HeartbeatEvent{}, nil
}

func (s *realtimeService) GetRTChannel(ctx context.Context, req *api.GetRTChannelRequest) (*api.GetRTChannelResponse, error) {
	return nil, errors.Unimplemented("not implemented")
}

func (s *realtimeService) GetRTChannels(ctx context.Context, req *api.GetRTChannelsRequest) (*api.GetRTChannelsResponse, error) {
	return nil, errors.Unimplemented("not implemented")
}

func (s *realtimeService) ReadMessages(req *api.ReadMessagesRequest, stream api.Realtime_ReadMessagesServer) error {
	return errors.Unimplemented("not implemented")
}

func (s *realtimeService) Messages(ctx context.Context, req *api.MessagesRequest) (*api.MessagesResponse, error) {
	return nil, errors.Unimplemented("not implemented")
}

func (s *realtimeService) ListSubscriptions(ctx context.Context, req *api.ListSubscriptionRequest) (*api.ListSubscriptionResponse, error) {
	return nil, errors.Unimplemented("not implemented")
}
