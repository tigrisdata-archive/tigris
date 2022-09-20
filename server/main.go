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
	"runtime"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/muxer"
	"github.com/tigrisdata/tigris/server/quota"
	"github.com/tigrisdata/tigris/server/tracing"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
)

func main() {
	config.LoadConfig(&config.DefaultConfig)
	ulog.Configure(config.DefaultConfig.Log)

	log.Info().Msgf("Environment: %v\n", config.GetEnvironment())
	log.Info().Msgf("Number of CPUs: %v", runtime.NumCPU())

	closerFunc, err := tracing.InitTracer(&config.DefaultConfig)
	if err != nil {
		ulog.E(err)
	}
	defer closerFunc()

	// Initialize metrics once
	closer := metrics.InitializeMetrics()
	defer func() {
		ulog.E(closer.Close())
	}()

	log.Info().Str("version", util.Version).Msgf("Starting server")

	var kvStore kv.KeyValueStore
	if config.DefaultConfig.Tracing.Enabled {
		kvStore, err = kv.NewKeyValueStoreWithMetrics(&config.DefaultConfig.FoundationDB)
	} else {
		kvStore, err = kv.NewKeyValueStore(&config.DefaultConfig.FoundationDB)
	}

	if err != nil {
		log.Fatal().Err(err).Msg("error initializing kv store")
	}

	var searchStore search.Store
	if config.DefaultConfig.Tracing.Enabled {
		searchStore, err = search.NewStoreWithMetrics(&config.DefaultConfig.Search)
	} else {
		searchStore, err = search.NewStore(&config.DefaultConfig.Search)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("error initializing search store")
	}

	tenantMgr := metadata.NewTenantManager(kvStore, searchStore)
	log.Info().Msg("initialized tenant manager")

	txMgr := transaction.NewManager(kvStore)
	log.Info().Msg("initialized transaction manager")

	if err = tenantMgr.EnsureDefaultNamespace(txMgr); err != nil {
		log.Fatal().Err(err).Msg("error initializing default namespace")
	}

	quota.Init(tenantMgr, txMgr, &config.DefaultConfig.Quota)

	mx := muxer.NewMuxer(&config.DefaultConfig)
	mx.RegisterServices(kvStore, searchStore, tenantMgr, txMgr)

	if err := mx.Start(config.DefaultConfig.Server.Host, config.DefaultConfig.Server.Port); err != nil {
		log.Fatal().Err(err).Msgf("error starting server")
	}

	log.Info().Msg("Shutdown")
}
