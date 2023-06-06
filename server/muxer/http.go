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
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	chi_middleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/middleware"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/util"
)

const readHeaderTimeout = 5 * time.Second

type HTTPServer struct {
	Router     chi.Router
	Inproc     *inprocgrpc.Channel
	tlsConfig  *tls.Config
	UnixSocket bool
}

func NewHTTPServer(cfg *config.Config) *HTTPServer {
	server := &HTTPServer{
		Inproc:     &inprocgrpc.Channel{},
		Router:     chi.NewRouter(),
		UnixSocket: len(cfg.Server.UnixSocket) > 0,
	}

	if (cfg.Server.TLSKey != "" || cfg.Server.TLSCert != "") && cfg.Server.TLSHttp {
		server.tlsConfig = initTLS(cfg)
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
	matchers := []cmux.Matcher{cmux.HTTP1Fast("PATCH"), cmux.HTTP1HeaderField("Upgrade", "websocket")}
	if s.tlsConfig != nil {
		matchers = append(matchers, cmux.TLS())
	}

	match := mux.Match(matchers...)

	go func() {
		srv := &http.Server{
			Handler:           s.Router,
			ReadHeaderTimeout: readHeaderTimeout,
			TLSConfig:         s.tlsConfig,
		}

		if s.UnixSocket {
			srv.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
				var nc net.Conn

				c, ok := conn.(*cmux.MuxConn)
				if !ok {
					tl, ok := conn.(*tls.Conn)
					if !ok {
						log.Debug().Msgf("not cmux connection: %v", reflect.TypeOf(conn))
						return ctx
					}

					nc = tl.NetConn()
				} else {
					nc = c.Conn
				}

				creds, err := util.ReadPeerCreds(nc)
				if err == nil && creds.Uid == 0 {
					log.Debug().Msgf("local root on http")
					return request.SetLocalRoot(ctx)
				}

				return ctx
			}
		}

		var err error

		if srv.TLSConfig != nil {
			err = srv.ServeTLS(match, "", "")
		} else {
			err = srv.Serve(match)
		}

		log.Fatal().Err(err).Msg("start http server")
	}()

	return nil
}
