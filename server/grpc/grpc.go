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
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/server/types"
	"google.golang.org/grpc"
)

type Server struct {
	GrpcS *grpc.Server
}

func NewServer(cfg *config.Config) *Server {
	var opts []grpc.ServerOption
	s := grpc.NewServer(opts...)

	return &Server{
		GrpcS: s,
	}
}

func (s *Server) Start(mux cmux.CMux) error {
	match := mux.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	go s.GrpcS.Serve(match)
	return nil
}

func (s *Server) SetupMiddlewares() {}

func (s *Server) GetType() string {
	return types.GRPCServer
}
