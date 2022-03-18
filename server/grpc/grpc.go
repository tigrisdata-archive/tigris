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

package grpc

import (
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigrisdb/server/config"
	middleware "github.com/tigrisdata/tigrisdb/server/midddleware"
	"github.com/tigrisdata/tigrisdb/server/types"
	"google.golang.org/grpc"
)

type Server struct {
	*grpc.Server
}

func NewServer(cfg *config.Config) *Server {
	s := &Server{}

	unary, stream := middleware.Get(cfg)
	s.Server = grpc.NewServer(grpc.StreamInterceptor(stream), grpc.UnaryInterceptor(unary))

	return s
}

func (s *Server) Start(mux cmux.CMux) error {
	// MatchWithWriters is needed as it needs SETTINGS frame from the server otherwise the client will block
	match := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	go func() {
		err := s.Serve(match)
		log.Fatal().Err(err).Msg("start http server")
	}()
	return nil
}

func (s *Server) GetType() string {
	return types.GRPCServer
}
