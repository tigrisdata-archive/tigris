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
	"fmt"
	"net"

	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigris/server/config"
	tgrpc "github.com/tigrisdata/tigris/server/grpc"
	tHTTP "github.com/tigrisdata/tigris/server/http"
	v1 "github.com/tigrisdata/tigris/server/services/v1"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
)

type Server interface {
	Start(mux cmux.CMux) error
	GetType() string
}

type Muxer struct {
	servers []Server
}

func NewMuxer(cfg *config.Config) *Muxer {
	var s []Server
	s = append(s, tHTTP.NewServer(cfg))
	s = append(s, tgrpc.NewServer(cfg))
	m := &Muxer{
		servers: s,
	}

	return m
}

func (m *Muxer) RegisterServices(kvStore kv.KeyValueStore, searchStore search.Store) {
	services := v1.GetRegisteredServices(kvStore, searchStore)
	for _, r := range services {
		for _, s := range m.servers {
			if s.GetType() == types.GRPCServer {
				_ = r.RegisterGRPC(s.(*tgrpc.Server).Server)
			} else if s.GetType() == types.HTTPServer {
				_ = r.RegisterHTTP(s.(*tHTTP.Server).Router, s.(*tHTTP.Server).Inproc)
			}
		}
	}
}

func (m *Muxer) Start(host string, port int16) error {
	log.Info().Int16("port", port).Msg("initializing server")

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatal().Err(err).Msg("listening failed ")
	}

	cm := cmux.New(l)
	for _, s := range m.servers {
		_ = s.Start(cm)
	}

	return cm.Serve()
}
