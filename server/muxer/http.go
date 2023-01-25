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
	"net/http"
	"time"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	chi_middleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/middleware"
)

const readHeaderTimeout = 5 * time.Second

type HTTPServer struct {
	Router chi.Router
	Inproc *inprocgrpc.Channel
}

func NewHTTPServer(cfg *config.Config) *HTTPServer {
	server := &HTTPServer{
		Inproc: &inprocgrpc.Channel{},
		Router: chi.NewRouter(),
	}

	server.SetupMiddlewares(cfg)

	// mount debug handler after adding all middlewares
	server.Router.Mount("/admin/debug", chi_middleware.Profiler())

	return server
}

func (s *HTTPServer) SetupMiddlewares(cfg *config.Config) {
	unary, stream := middleware.Get(cfg)

	s.Inproc.WithServerStreamInterceptor(stream)
	s.Inproc.WithServerUnaryInterceptor(unary)

	s.Router.Use(cors.AllowAll().Handler)
	if cfg.Server.Type == config.RealtimeServerType {
		s.Router.Use(middleware.HTTPMetadataExtractorMiddleware(cfg))
		s.Router.Use(middleware.HTTPAuthMiddleware(cfg))
	}
}

func (s *HTTPServer) Start(mux cmux.CMux) error {
	match := mux.Match(cmux.HTTP1Fast("PATCH"), cmux.HTTP1HeaderField("Upgrade", "websocket"))
	go func() {
		srv := &http.Server{Handler: s.Router, ReadHeaderTimeout: readHeaderTimeout}
		err := srv.Serve(match)
		log.Fatal().Err(err).Msg("start http server")
	}()
	return nil
}
