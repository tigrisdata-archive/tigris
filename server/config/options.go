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

	"github.com/tigrisdata/tigris/util"

	"github.com/tigrisdata/tigris/util/log"
)

type ServerConfig struct {
	Host string
	Port int16
}

type Config struct {
	Log          log.LogConfig
	Server       ServerConfig    `yaml:"server" json:"server"`
	Auth         AuthConfig      `yaml:"auth" json:"auth"`
	Cdc          CdcConfig       `yaml:"cdc" json:"cdc"`
	Search       SearchConfig    `yaml:"search" json:"search"`
	Tracing      TracingConfig   `yaml:"tracing" json:"tracing"`
	Profiling    ProfilingConfig `yaml:"profiling" json:"profiling"`
	Tags         TagsConfig      `yaml:"tags" json:"tags"`
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

type TracingConfig struct {
	Enabled           bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	WithUDS           string
	WithAgentAddr     string `mapstructure:"agent_addr" yaml:"agent_addr" json:"agent_addr"`
	WithDogStatsdAddr string
}

type ProfilingConfig struct {
	Enabled         bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	EnableCPU       bool `mapstructure:"enable_cpu" yaml:"enable_cpu" json:"enable_cpu"`
	EnableHeap      bool
	EnableBlock     bool
	EnableMutex     bool
	EnableGoroutine bool
}

type TagsConfig struct {
	Service     string
	Environment string
	Version     string
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
		Host:         "localhost",
		Port:         8108,
		ReadEnabled:  true,
		WriteEnabled: true,
	},
	Tracing: TracingConfig{
		Enabled: false,
	},
	Profiling: ProfilingConfig{
		Enabled:    false,
		EnableCPU:  true,
		EnableHeap: true,
	},
	Tags: TagsConfig{
		Service:     util.Service,
		Environment: "local",
		Version:     util.Version,
	},
}

// FoundationDBConfig keeps FoundationDB configuration parameters
type FoundationDBConfig struct {
	ClusterFile string `mapstructure:"cluster_file" json:"cluster_file" yaml:"cluster_file"`
}

type SearchConfig struct {
	Host         string
	Port         int16
	AuthKey      string `mapstructure:"auth_key" json:"auth_key" yaml:"auth_key"`
	ReadEnabled  bool
	WriteEnabled bool
}
