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

package main

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/muxer"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

//Version of this build
var Version string

//BuildHash keeps Git hash of the commit of this build
var BuildHash string

func main() {
	pflag.String("api.port", "", "set port server listens on")

	config.LoadConfig("server", &config.DefaultConfig)

	ulog.Configure(config.DefaultConfig.Log)

	log.Info().Str("version", Version).Str("BuildHash", BuildHash).Msgf("Starting server")

	kvStore, err := kv.NewKeyValueStore(&config.DefaultConfig.FoundationDB)
	if err != nil {
		log.Fatal().Err(err).Msg("error initializing kv store")
	}

	mx := muxer.NewMuxer(&config.DefaultConfig)
	mx.RegisterServices(kvStore)
	if err := mx.Start(config.DefaultConfig.Server.Host, config.DefaultConfig.Server.Port); err != nil {
		log.Fatal().Err(err).Msgf("error starting server")
	}

	log.Info().Msg("Shutdown")
}
