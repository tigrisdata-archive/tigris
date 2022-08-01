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
	"net/http"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	chi_middleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/middleware"
	"github.com/tigrisdata/tigris/server/transaction"
)

type HTTPServer struct {
	Router chi.Router
	Inproc *inprocgrpc.Channel
}

func NewHTTPServer(cfg *config.Config, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) *HTTPServer {
	r := chi.NewRouter()

	r.Use(cors.AllowAll().Handler)
	r.Mount("/debug", chi_middleware.Profiler())

	unary, stream := middleware.Get(cfg, tenantMgr, txMgr)

	inproc := &inprocgrpc.Channel{}
	inproc.WithServerStreamInterceptor(stream)
	inproc.WithServerUnaryInterceptor(unary)

	return &HTTPServer{Inproc: inproc, Router: r}
}

func (s *HTTPServer) Start(mux cmux.CMux) error {
	match := mux.Match(cmux.HTTP1Fast())
	go func() {
		srv := &http.Server{Handler: s.Router}
		err := srv.Serve(match)
		log.Fatal().Err(err).Msg("start http server")
	}()
	return nil
}
