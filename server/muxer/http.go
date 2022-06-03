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

	middleware "github.com/tigrisdata/tigris/server/midddleware"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigris/server/config"
)

type HTTPServer struct {
	Router chi.Router
	httpS  *http.Server
	Inproc *inprocgrpc.Channel
}

func NewHTTPServer(cfg *config.Config) *HTTPServer {
	r := chi.NewRouter()
	s := &HTTPServer{
		Inproc: &inprocgrpc.Channel{},
		Router: r,
		httpS: &http.Server{
			Handler: r,
		},
	}

	unary, stream := middleware.Get(cfg)

	s.Inproc.WithServerStreamInterceptor(stream)
	s.Inproc.WithServerUnaryInterceptor(unary)

	return s
}

func (s *HTTPServer) Start(mux cmux.CMux) error {
	match := mux.Match(cmux.HTTP1Fast())
	go func() {
		err := s.httpS.Serve(match)
		log.Fatal().Err(err).Msg("start http server")
	}()
	return nil
}
