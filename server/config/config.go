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
	"os"
	"strings"

	"github.com/spf13/pflag"

	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

var configPath = []string{
	"/etc/tigrisdata/tigrisdb/",
	"$HOME/.tigrisdata/tigrisdb/",
	"./config/",
	"./",
}

// envPrefix is used by viper to detect environment variables that should be used.
// viper will automatically uppercase this and append _ to it
var envPrefix = "tigrisdb_server"

var envEnv = "tigrisdb_environment"
var environment string

const (
	EnvTest        = "test"
	EnvDevelopment = "development"
	EnvProduction  = "production"
)

func GetEnvironment() string {
	return environment
}

func LoadEnvironment() {
	env := os.Getenv(envEnv)
	if env == "" {
		env = os.Getenv(strings.ToUpper(envEnv))
	}

	environment = env
}

func LoadConfig(name string, config interface{}) {
	LoadEnvironment()

	if GetEnvironment() != "" {
		name += "." + GetEnvironment()
	}

	viper.SetConfigName(name)
	viper.SetConfigType("yaml")

	for _, v := range configPath {
		viper.AddConfigPath(v)
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

	pflag.Parse()
	err = viper.BindPFlags(pflag.CommandLine)
	log.Err(err).Msg("bind flags")

	err = viper.ReadInConfig()
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

	viper.OnConfigChange(func(e fsnotify.Event) {
		log.Debug().Str("notify", e.Name).Msg("Config file changed")
		//TODO: handle config change
	})

	viper.WatchConfig()
}
