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

package muxer

import (
	"fmt"
	"net"
	"os"
	"runtime"

	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	v1 "github.com/tigrisdata/tigris/server/services/v1"
	"github.com/tigrisdata/tigris/server/services/v1/billing"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type Server interface {
	Start(mux cmux.CMux) error
}

type Muxer struct {
	servers []Server
}

func NewMuxer(cfg *config.Config) *Muxer {
	return &Muxer{servers: []Server{NewHTTPServer(cfg), NewGRPCServer(cfg)}}
}

func (m *Muxer) RegisterServices(cfg *config.ServerConfig, kvStore kv.TxStore, searchStore search.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager, forSearchTxMgr *transaction.Manager, biller billing.Provider) {
	var services []v1.Service
	if cfg.Type == config.RealtimeServerType {
		services = v1.GetRegisteredServicesRealtime(kvStore, searchStore, tenantMgr, txMgr)
	} else {
		services = v1.GetRegisteredServices(kvStore, searchStore, tenantMgr, txMgr, forSearchTxMgr, biller)
	}
	for _, r := range services {
		for _, v := range m.servers {
			if s, ok := v.(*GRPCServer); ok {
				if err := r.RegisterGRPC(s.Server); err != nil {
					ulog.E(err)
				}
			} else if s, ok := v.(*HTTPServer); ok {
				if err := r.RegisterHTTP(s.Router, s.Inproc); err != nil {
					ulog.E(err)
				}
			}
		}
	}
}

func (m *Muxer) serve(l net.Listener) {
	cm := cmux.New(l)

	for _, s := range m.servers {
		_ = s.Start(cm)
	}

	err := cm.Serve()
	util.Fatal(err, "serve")
}

func (m *Muxer) Start(host string, port int16, unix string) {
	if unix == "" && port == 0 {
		util.Fatal(fmt.Errorf("please configure TCP host:port or unix socket for server to bind to"), "binding ports")
	}

	if unix != "" && (runtime.GOOS != "windows") {
		log.Info().Str("uds", unix).Msg("initializing server")

		ulog.E(os.Remove(unix))

		l, err := net.Listen("unix", unix)
		util.Fatal(err, "listen on unix domain socket")

		if port != 0 {
			go m.serve(l)
		} else {
			m.serve(l)
		}
	}

	if port == 0 && (runtime.GOOS == "windows") {
		port = 8081
	}

	if port != 0 {
		log.Info().Str("host", host).Int16("port", port).Msg("initializing server")

		l, err := net.Listen("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
		util.Fatal(err, "listen on tcp port")

		m.serve(l)
	}
}
