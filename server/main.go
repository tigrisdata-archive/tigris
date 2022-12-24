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
	"os"
	"runtime"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/muxer"
	"github.com/tigrisdata/tigris/server/quota"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/tracing"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
)

func main() {
	os.Exit(mainWithCode())
}

func mainWithCode() int {
	config.LoadConfig(&config.DefaultConfig)
	ulog.Configure(config.DefaultConfig.Log)

	log.Info().Msgf("Environment: '%v'", config.GetEnvironment())
	log.Info().Msgf("Number of CPUs: %v", runtime.NumCPU())
	log.Info().Msgf("Server Type: '%v'", config.DefaultConfig.Server.Type)

	closerFunc, err := tracing.InitTracer(&config.DefaultConfig)
	if err != nil {
		ulog.E(err)
	}
	defer closerFunc()

	// Initialize metrics once
	cleanup := metrics.InitializeMetrics()
	defer cleanup()

	log.Info().Str("version", util.Version).Msgf("Starting server")

	var kvStore kv.KeyValueStore
	if config.DefaultConfig.Tracing.Enabled {
		kvStore, err = kv.NewKeyValueStoreWithMetrics(&config.DefaultConfig.FoundationDB)
	} else {
		kvStore, err = kv.NewKeyValueStore(&config.DefaultConfig.FoundationDB)
	}

	if err != nil {
		log.Error().Err(err).Msg("error initializing kv store")
		return 1
	}

	var searchStore search.Store
	if config.DefaultConfig.Tracing.Enabled {
		searchStore, err = search.NewStoreWithMetrics(&config.DefaultConfig.Search)
	} else {
		searchStore, err = search.NewStore(&config.DefaultConfig.Search)
	}
	if err != nil {
		log.Error().Err(err).Msg("error initializing search store")
		return 1
	}

	txMgr := transaction.NewManager(kvStore)
	log.Info().Msg("initialized transaction manager")

	tenantMgr := metadata.NewTenantManager(kvStore, searchStore, txMgr)
	log.Info().Msg("initialized tenant manager")

	if err = tenantMgr.EnsureDefaultNamespace(); err != nil {
		// ToDo: do not load collections for realtime deployment
		log.Error().Err(err).Msg("error initializing default namespace")
		return 1
	}

	cfg := &config.DefaultConfig
	request.Init(tenantMgr)
	_ = quota.Init(tenantMgr, cfg)
	defer quota.Cleanup()

	mx := muxer.NewMuxer(cfg)
	mx.RegisterServices(&cfg.Server, kvStore, searchStore, tenantMgr, txMgr)
	port := cfg.Server.Port
	if cfg.Server.Type == config.RealtimeServerType {
		port = cfg.Server.RealtimePort
	}
	if err := mx.Start(cfg.Server.Host, port); err != nil {
		log.Error().Err(err).Msgf("error starting realtime server")
		return 1
	}

	log.Info().Msg("Shutdown")
	return 0
}
