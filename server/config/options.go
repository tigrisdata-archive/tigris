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
	"github.com/tigrisdata/tigrisdb/util/log"
)

type ServerConfig struct {
	Host string
	Port int16
}

type Config struct {
	Server       ServerConfig `yaml:"server" json:"server"`
	Log          log.LogConfig
	FoundationDB FoundationDBConfig
}

var DefaultConfig = Config{
	Log: log.LogConfig{
		Level: "trace",
	},
	Server: ServerConfig{
		Host: "0.0.0.0",
		Port: 8081,
	},
}

// FoundationDBConfig keeps FoundationDB configuration parameters
type FoundationDBConfig struct {
	ClusterFile string `mapstructure:"cluster_file" json:"cluster_file" yaml:"cluster_file"`
}
