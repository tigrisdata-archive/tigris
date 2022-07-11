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
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	v1 "github.com/tigrisdata/tigris/server/services/v1"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
)

type Server interface {
	Start(mux cmux.CMux) error
}

type Muxer struct {
	servers []Server
}

func NewMuxer(cfg *config.Config, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) *Muxer {
	return &Muxer{servers: []Server{NewHTTPServer(cfg, tenantMgr, txMgr), NewGRPCServer(cfg, tenantMgr, txMgr)}}
}

func (m *Muxer) RegisterServices(kvStore kv.KeyValueStore, searchStore search.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) {
	services := v1.GetRegisteredServices(kvStore, searchStore, tenantMgr, txMgr)
	for _, r := range services {
		for _, v := range m.servers {
			if s, ok := v.(*GRPCServer); ok {
				_ = r.RegisterGRPC(s.Server)
				// Initialize the metrics for each GRPC service
				metrics.InitRequestMetricsForServer(s.Server)
			} else if s, ok := v.(*HTTPServer); ok {
				_ = r.RegisterHTTP(s.Router, s.Inproc)
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
	log.Info().Msg("server started, servicing requests")
	return cm.Serve()
}
