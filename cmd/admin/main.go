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

	"github.com/spf13/pflag"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/cmd/admin/cmd"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

func main() {
	ulog.Configure(ulog.LogConfig{Level: "error"})

	pflag.CommandLine = pflag.NewFlagSet("", pflag.ContinueOnError)

	config.LoadConfig(&config.DefaultConfig)
	config.DefaultConfig.Log.Level = "error"

	ulog.Configure(config.DefaultConfig.Log)

	defaultConfig := &config.DefaultConfig

	var searchStore search.Store
	var err error
	if defaultConfig.Metrics.Search.Enabled {
		searchStore, err = search.NewStoreWithMetrics(&defaultConfig.Search)
	} else {
		searchStore, err = search.NewStore(&defaultConfig.Search)
	}
	if err != nil {
		log.Error().Err(err).Msg("error initializing search store")
		os.Exit(1)
	}

	// creating kv store for search and database independently allows us to enable functionality slowly. This is
	// temporary as once we have functionality tested then
	kvStoreForSearch, err := kv.StoreForSearch(defaultConfig)
	if err != nil {
		log.Error().Err(err).Msg("error initializing kv store for search")
		os.Exit(1)
	}
	kvStoreForDatabase, err := kv.StoreForDatabase(defaultConfig)
	if err != nil {
		log.Error().Err(err).Msg("error initializing kv store for database")
		os.Exit(1)
	}

	txMgr := transaction.NewManager(kvStoreForDatabase)
	log.Info().Msg("initialized transaction manager")

	forSearchTxMgr := transaction.NewManager(kvStoreForSearch)
	log.Info().Msg("initialized transaction manager for search")

	tenantMgr := metadata.NewTenantManager(kvStoreForDatabase, searchStore, txMgr)
	log.Info().Msg("initialized tenant manager")

	cmd.Mgr = cmd.Managers{
		Tx:        txMgr,
		SearchTx:  forSearchTxMgr,
		Tenant:    tenantMgr,
		KVDB:      kvStoreForDatabase,
		KVSearch:  kvStoreForSearch,
		MetaStore: metadata.NewMetadataDictionary(metadata.DefaultNameRegistry),
	}

	cmd.Execute()
}
