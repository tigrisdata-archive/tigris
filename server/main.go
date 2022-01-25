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
	"github.com/tigrisdata/tigrisdb/store/kv"
	"github.com/tigrisdata/tigrisdb/util"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
)

//Version of this build
var Version string

//BuildHash keeps Git hash of the commit of this build
var BuildHash string

func main() {
	pflag.String("api.grpc_port", "", "set server grpc port")
	//	pflag.String("api.http_port", "", "set server http port")

	util.LoadConfig("server", &config)

	ulog.Configure(config.Log)

	log.Info().Str("version", Version).Str("BuildHash", BuildHash).Msgf("Starting server")

	kv, err := kv.NewDynamoDB(&config.DynamoDB)
	if err != nil {
		log.Fatal().Err(err).Msg("error initializing kv store")
	}

	if err := ServeAPI(config.API.Host, config.API.GRPCPort, config.API.HTTPPort, kv); err != nil {
		log.Fatal().Err(err).Msgf("error starting grpc/http server")
	}

	log.Info().Msg("Shutdown")
}
