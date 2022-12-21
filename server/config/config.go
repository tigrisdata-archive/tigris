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

package config

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	ulog "github.com/tigrisdata/tigris/util/log"
	yaml "gopkg.in/yaml.v2"
)

var configPath = []string{
	"/etc/tigrisdata/tigris/",
	"$HOME/.tigrisdata/tigris/",
	"./config/",
	"./",
}

var configFile string

// envPrefix is used by viper to detect environment variables that should be used.
// viper will automatically uppercase this and append _ to it.
var envPrefix = "tigris_server"

var (
	envEnv      = "tigris_environment"
	environment string
)

const (
	EnvDevelopment = "dev"
	EnvProduction  = "prod"
	EnvSandbox     = "sbx"
	EnvTest        = "test"
	EnvLocal       = "local"
)

func GetEnvironment() string {
	return environment
}

func LoadEnvironment() {
	resolutionOrder := []string{
		os.Getenv(envEnv),
		os.Getenv(strings.ToUpper(envEnv)),
		EnvLocal,
	}

	for _, resolvedEnv := range resolutionOrder {
		if resolvedEnv != "" {
			environment = resolvedEnv
			break
		}
	}
}

func LoadConfig(config interface{}) {
	LoadEnvironment()

	pflag.StringVarP(&configFile, "config", "c", "", "server configuration file")
	ulog.E(viper.BindPFlags(pflag.CommandLine))
	pflag.Parse()

	// Deactivates lookup process for default server.yaml if
	// configuration file is specified via the command line
	if configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("server")
		viper.SetConfigType("yaml")
		for _, v := range configPath {
			viper.AddConfigPath(v)
		}
	}

	// This is needed to automatically bind environment variables to config struct
	b, err := yaml.Marshal(config)
	log.Err(err).Msg("marshal config")
	log.Debug().RawJSON("config", b).Msg("default config")
	br := bytes.NewBuffer(b)
	err = viper.MergeConfig(br)
	log.Err(err).Msg("merge config")

	spew.Dump(viper.AllKeys())

	// This is needed to replace periods with underscores when mapping environment variables to multi-level
	// config keys. For example, this will allow foundationdb.cluster_file to be mapped to FOUNDATIONDB_CLUSTER_FILE
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// The environment variables have a higher priority as compared to config values defined in the config file.
	// This allows us to override the config values using environment variables.
	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv()

	err = viper.MergeInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Warn().Err(err).Msgf("config file not found")
		} else {
			log.Fatal().Err(err).Msgf("error reading config")
		}
	}

	if err := viper.Unmarshal(&config); err != nil {
		log.Fatal().Err(err).Msg("error unmarshalling config")
	}

	log.Debug().Interface("config", &config).Msg("final")
	spew.Dump(viper.AllKeys())
	spew.Dump(&config)

	viper.OnConfigChange(func(e fsnotify.Event) {
		log.Debug().Str("notify", e.Name).Msg("Config file changed")
		// TODO: handle config change
	})

	viper.WatchConfig()
}

func GetTestFDBConfig(path string) (*FoundationDBConfig, error) {
	LoadEnvironment()

	// Environment can be set on OS X
	fn, exists := os.LookupEnv("TIGRIS_SERVER_FOUNDATIONDB_CLUSTER_FILE")

	// Use default location when run test in the docker
	// where cluster file is shared between containers
	if !exists && GetEnvironment() != EnvTest {
		fn = path + "/test/config/fdb.cluster"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "fdbcli", "-C", fn, "--exec", "configure new single memory")
	_, err := cmd.Output()
	if err != nil {
		cmd := exec.CommandContext(ctx, "fdbcli", "-C", fn, "--exec", "configure single memory")
		_, err = cmd.Output()
	}
	if err != nil {
		fmt.Printf("\nRun `make local_run` in the terminal to start FDB instance for tests\n") //nolint:golint,forbidigo
		return nil, err
	}

	return &FoundationDBConfig{ClusterFile: fn}, nil
}

func GetTestCacheConfig() *CacheConfig {
	LoadEnvironment()

	if GetEnvironment() == EnvTest {
		DefaultConfig.Cache.Host = "tigris_cache"
		return &DefaultConfig.Cache
	}

	return &DefaultConfig.Cache
}
