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

package main

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"

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
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs

		os.Exit(0)
	}()

	os.Exit(mainWithCode())
}

func mainWithCode() int {
	l := ulog.LogConfig{
		Level:      "info",
		SampleRate: 0.01,
		Format:     "console",
	}
	config.LoadConfig(&config.DefaultConfig)
	// ulog.Configure(config.DefaultConfig.Log)
	ulog.Configure(l)

	log.Info().Msgf("Environment: '%v'", config.GetEnvironment())
	log.Info().Msgf("Number of CPUs: %v", runtime.NumCPU())
	log.Info().Msgf("Server Type: '%v'", config.DefaultConfig.Server.Type)

	defaultConfig := &config.DefaultConfig
	closerFunc, err := tracing.InitTracer(defaultConfig)
	if err != nil {
		ulog.E(err)
	}
	defer closerFunc()

	// Initialize metrics once
	cleanup := metrics.InitializeMetrics()
	defer cleanup()

	log.Info().Str("version", util.Version).Msgf("Starting server")

	var searchStore search.Store
	if defaultConfig.Metrics.Search.Enabled {
		searchStore, err = search.NewStoreWithMetrics(&defaultConfig.Search)
	} else {
		searchStore, err = search.NewStore(&defaultConfig.Search)
	}
	if err != nil {
		log.Error().Err(err).Msg("error initializing search store")
		return 1
	}

	// creating kv store for search and database independently allows us to enable functionality slowly. This is
	// temporary as once we have functionality tested then
	kvStoreForSearch, err := kvStoreForSearch(defaultConfig)
	if err != nil {
		log.Error().Err(err).Msg("error initializing kv store for search")
		return 1
	}
	kvStoreForDatabase, err := kvStoreForDatabase(defaultConfig)
	if err != nil {
		log.Error().Err(err).Msg("error initializing kv store for database")
		return 1
	}

	txMgr := transaction.NewManager(kvStoreForDatabase)
	log.Info().Msg("initialized transaction manager")

	forSearchTxMgr := transaction.NewManager(kvStoreForSearch)
	log.Info().Msg("initialized transaction manager for search")

	tenantMgr := metadata.NewTenantManager(kvStoreForDatabase, searchStore, txMgr)
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
	mx.RegisterServices(&cfg.Server, kvStoreForDatabase, searchStore, tenantMgr, txMgr, forSearchTxMgr)
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

func kvStoreForDatabase(cfg *config.Config) (kv.TxStore, error) {
	builder := kv.NewBuilder()
	builder.WithListener() // database has a listener attached to it
	if config.DefaultConfig.Metrics.Fdb.Enabled {
		builder.WithMeasure()
	}
	return builder.Build(&cfg.FoundationDB)
}

func kvStoreForSearch(cfg *config.Config) (kv.TxStore, error) {
	builder := kv.NewBuilder()
	if config.DefaultConfig.Search.Chunking {
		builder.WithChunking()
	}
	if config.DefaultConfig.Metrics.Fdb.Enabled {
		builder.WithMeasure()
	}
	return builder.Build(&cfg.FoundationDB)
}
