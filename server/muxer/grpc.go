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
	"crypto/tls"
	"io"
	"net"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/middleware"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/local"
	"google.golang.org/grpc/reflection"
)

const (
	defaultTigrisServerMaxReceiveMessageSize = 1024 * 1024 * 16
)

type GRPCServer struct {
	*grpc.Server
	ServeTLS bool
}

func initTLS(cfg *config.Config) *tls.Config {
	if len(cfg.Server.TLSCert) > 0 && (cfg.Server.TLSCert[0] == '/' || cfg.Server.TLSCert[0] == '.') {
		cert, err := os.ReadFile(cfg.Server.TLSCert)
		util.Fatal(err, "reading TLS cert from file")

		log.Info().Str("file", cfg.Server.TLSCert).Msg("read certificate from file")

		cfg.Server.TLSCert = string(cert)
	}

	if len(cfg.Server.TLSKey) > 0 && (cfg.Server.TLSKey[0] == '/' || cfg.Server.TLSKey[0] == '.') {
		key, err := os.ReadFile(cfg.Server.TLSKey)
		util.Fatal(err, "reading TLS private key from file")

		log.Info().Str("file", cfg.Server.TLSKey).Msg("read private key from file")

		cfg.Server.TLSCert = string(key)
	}

	cert, err := tls.X509KeyPair([]byte(cfg.Server.TLSCert), []byte(cfg.Server.TLSKey))
	util.Fatal(err, "loading X509 certificate")

	tlsConfig := &tls.Config{
		ServerName:   "localhost",
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	log.Info().Msg("initializing TLS")

	return tlsConfig
}

type UnixPeerCredentials struct {
	credentials.TransportCredentials
}

func (*UnixPeerCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	ai := request.AuthInfo{
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity},
	}

	c, ok := conn.(*cmux.MuxConn)
	if !ok {
		return conn, &ai, nil
	}

	creds, err := util.ReadPeerCreds(c.Conn)
	if err != nil {
		return conn, &ai, nil //nolint:nilerr
	}

	log.Debug().Msgf("grpc server handshake. user id=%v", creds.Uid)

	ai.LocalRoot = creds.Uid == 0

	if ai.LocalRoot {
		log.Debug().Msg("Local root user detected")
	}

	ai.CommonAuthInfo.SecurityLevel = credentials.PrivacyAndIntegrity

	return conn, &ai, nil
}

func NewGRPCServer(cfg *config.Config) *GRPCServer {
	s := &GRPCServer{}

	unary, stream := middleware.Get(cfg)

	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(stream),
		grpc.UnaryInterceptor(unary),
		grpc.MaxRecvMsgSize(defaultTigrisServerMaxReceiveMessageSize),
	}

	if cfg.Server.UnixSocket != "" {
		opts = append(opts, grpc.Creds(&UnixPeerCredentials{
			local.NewCredentials(),
		}))
	}

	if (cfg.Server.TLSCert != "" || cfg.Server.TLSKey != "") && !cfg.Server.TLSHttp {
		opts = append(opts, grpc.Creds(credentials.NewTLS(initTLS(cfg))))
		s.ServeTLS = true
	}

	s.Server = grpc.NewServer(opts...)

	reflection.Register(s)

	return s
}

func (s *GRPCServer) Start(mux cmux.CMux) error {
	// MatchWithWriters is needed as it needs SETTINGS frame from the server otherwise the client will block
	matchers := []cmux.MatchWriter{cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc")}

	if s.ServeTLS {
		matchers = append(matchers, func(_ io.Writer, r io.Reader) bool { return cmux.TLS()(r) })
	}

	match := mux.MatchWithWriters(matchers...)

	go func() {
		err := s.Serve(match)
		log.Fatal().Err(err).Msg("start http server")
	}()

	return nil
}
