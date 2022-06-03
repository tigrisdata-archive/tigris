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

package muxer

import (
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigris/server/config"
	middleware "github.com/tigrisdata/tigris/server/midddleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GRPCServer struct {
	*grpc.Server
}

func NewGRPCServer(cfg *config.Config) *GRPCServer {
	s := &GRPCServer{}

	unary, stream := middleware.Get(cfg)
	s.Server = grpc.NewServer(grpc.StreamInterceptor(stream), grpc.UnaryInterceptor(unary))
	reflection.Register(s)
	return s
}

func (s *GRPCServer) Start(mux cmux.CMux) error {
	// MatchWithWriters is needed as it needs SETTINGS frame from the server otherwise the client will block
	match := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	go func() {
		err := s.Serve(match)
		log.Fatal().Err(err).Msg("start http server")
	}()
	return nil
}
