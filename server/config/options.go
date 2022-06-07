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
	"time"

	"github.com/tigrisdata/tigris/util/log"
)

type ServerConfig struct {
	Host string
	Port int16
}

type Config struct {
	Log          log.LogConfig
	Server       ServerConfig `yaml:"server" json:"server"`
	Auth         AuthConfig   `yaml:"auth" json:"auth"`
	Cdc          CdcConfig    `yaml:"cdc" json:"cdc"`
	Search       SearchConfig `yaml:"search" json:"search"`
	FoundationDB FoundationDBConfig
}

type AuthConfig struct {
	IssuerURL        string
	Audience         string
	JWKSCacheTimeout time.Duration
	LogOnly          bool
}

type CdcConfig struct {
	Enabled        bool
	StreamInterval time.Duration
	StreamBatch    int
	StreamBuffer   int
}

var DefaultConfig = Config{
	Log: log.LogConfig{
		Level: "trace",
	},
	Server: ServerConfig{
		Host: "0.0.0.0",
		Port: 8081,
	},
	Auth: AuthConfig{
		IssuerURL:        "https://tigrisdata-dev.us.auth0.com/",
		Audience:         "https://tigris-db-api",
		JWKSCacheTimeout: 5 * time.Minute,
		LogOnly:          true,
	},
	Cdc: CdcConfig{
		Enabled:        true,
		StreamInterval: 500 * time.Millisecond,
		StreamBatch:    100,
		StreamBuffer:   200,
	},
	Search: SearchConfig{
		Host:         "0.0.0.0",
		Port:         8108,
		ReadEnabled:  true,
		WriteEnabled: true,
		AuthKey:      "ts_test_key",
	},
}

// FoundationDBConfig keeps FoundationDB configuration parameters
type FoundationDBConfig struct {
	ClusterFile string `mapstructure:"cluster_file" json:"cluster_file" yaml:"cluster_file"`
}

type SearchConfig struct {
	Host         string
	Port         int16
	AuthKey      string
	ReadEnabled  bool
	WriteEnabled bool
}

func (s *SearchConfig) GetHost() string {
	if GetEnvironment() == EnvTest {
		return "tigris_search"
	}
	return "0.0.0.0"
}
