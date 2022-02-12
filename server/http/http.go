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

package http

import (
	"net/http"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigrisdb/server/config"
	tmiddlewares "github.com/tigrisdata/tigrisdb/server/midddlewares/http"
	"github.com/tigrisdata/tigrisdb/server/types"
)

const (
	AllowedConnections      int           = 256
	BacklogConnectionsLimit int           = 256
	BacklogWindow           time.Duration = 1 * time.Second
)

type Server struct {
	Router chi.Router
	httpS  *http.Server
}

func NewServer(cfg *config.Config) *Server {
	r := chi.NewRouter()
	s := &Server{
		Router: r,
		httpS: &http.Server{
			Handler: r,
		},
	}

	s.SetupMiddlewares()
	return s

}

func (s *Server) Start(mux cmux.CMux) error {
	match := mux.Match(cmux.HTTP1Fast())
	go func() {
		err := s.httpS.Serve(match)
		log.Fatal().Err(err).Msg("start http server")
	}()
	return nil
}

func (s *Server) SetupMiddlewares() {
	s.Router.Use(middleware.Logger)
	s.Router.Use(middleware.ThrottleBacklog(AllowedConnections, BacklogConnectionsLimit, BacklogWindow))
	s.Router.Use(tmiddlewares.ContextSetter(nil))
	s.Router.Use(tmiddlewares.Timeout)
	s.Router.Use(tmiddlewares.RuntimeLabels)
	s.Router.Use(middleware.Recoverer)
}

func (s *Server) GetType() string {
	return types.HTTPServer
}
