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
	Host        string
	Port        int16
	FDBHardDrop bool `mapstructure:"fdb_hard_drop" yaml:"fdb_hard_drop" json:"fdb_hard_drop"`
}

type Config struct {
	Log           log.LogConfig
	Server        ServerConfig    `yaml:"server" json:"server"`
	Auth          AuthConfig      `yaml:"auth" json:"auth"`
	Cdc           CdcConfig       `yaml:"cdc" json:"cdc"`
	Search        SearchConfig    `yaml:"search" json:"search"`
	Tracing       TracingConfig   `yaml:"tracing" json:"tracing"`
	Profiling     ProfilingConfig `yaml:"profiling" json:"profiling"`
	FoundationDB  FoundationDBConfig
	Quota         QuotaConfig
	Observability ObservabilityConfig `yaml:"observability" json:"observability"`
	Users         UsersConfig         `yaml:"users" json:"users"`
}

type AuthConfig struct {
	Enabled                   bool          `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	IssuerURL                 string        `mapstructure:"issuer_url" yaml:"issuer_url" json:"issuer_url"`
	Audience                  string        `mapstructure:"audience" yaml:"audience" json:"audience"`
	JWKSCacheTimeout          time.Duration `mapstructure:"jwks_cache_timeout" yaml:"jwks_cache_timeout" json:"jwks_cache_timeout"`
	LogOnly                   bool          `mapstructure:"log_only" yaml:"log_only" json:"log_only"`
	EnableNamespaceIsolation  bool          `mapstructure:"enable_namespace_isolation" yaml:"enable_namespace_isolation" json:"enable_namespace_isolation"`
	AdminNamespaces           []string      `mapstructure:"admin_namespaces" yaml:"admin_namespaces" json:"admin_namespaces"`
	OAuthProvider             string        `mapstructure:"oauth_provider" yaml:"oauth_provider" json:"oauth_provider"`
	ClientId                  string        `mapstructure:"client_id" yaml:"client_id" json:"client_id"`
	ExternalTokenURL          string        `mapstructure:"external_token_url" yaml:"external_token_url" json:"external_token_url"`
	EnableOauth               bool          `mapstructure:"enable_oauth" yaml:"enable_oauth" json:"enable_oauth"`
	TokenCacheSize            int           `mapstructure:"token_cache_size" yaml:"token_cache_size" json:"token_cache_size"`
	ExternalDomain            string        `mapstructure:"external_domain" yaml:"external_domain" json:"external_domain"`
	ManagementClientId        string        `mapstructure:"management_client_id" yaml:"management_client_id" json:"management_client_id"`
	ManagementClientSecret    string        `mapstructure:"management_client_secret" yaml:"management_client_secret" json:"management_client_secret"`
	TokenClockSkewDurationSec int           `mapstructure:"token_clock_skew_duration_sec" yaml:"token_clock_skew_duration_sec" json:"token_clock_skew_duration_sec"`
}

type CdcConfig struct {
	Enabled        bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	StreamInterval time.Duration
	StreamBatch    int
	StreamBuffer   int
}

type TracingConfig struct {
	Enabled             bool    `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	SampleRate          float64 `mapstructure:"sample_rate" yaml:"sample_rate" json:"sample_rate"`
	CodeHotspotsEnabled bool    `mapstructure:"codehotspots_enabled" yaml:"codehotspots_enabled" json:"codehotspots_enabled"`
	EndpointsEnabled    bool    `mapstructure:"endpoints_enabled" yaml:"endpoints_enabled" json:"endpoints_enabled"`
	WithUDS             string  `mapstructure:"agent_socket" yaml:"agent_socket" json:"agent_socket"`
	WithAgentAddr       string  `mapstructure:"agent_addr" yaml:"agent_addr" json:"agent_addr"`
	WithDogStatsdAddr   string  `mapstructure:"dogstatsd_addr" yaml:"dogstatsd_addr" json:"dogstatsd_addr"`
}

type ProfilingConfig struct {
	Enabled         bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	EnableCPU       bool `mapstructure:"enable_cpu" yaml:"enable_cpu" json:"enable_cpu"`
	EnableHeap      bool `mapstructure:"enable_heap" yaml:"enable_heap" json:"enable_heap"`
	EnableBlock     bool `mapstructure:"enable_block" yaml:"enable_block" json:"enable_block"`
	EnableMutex     bool `mapstructure:"enable_mutex" yaml:"enable_mutex" json:"enable_mutex"`
	EnableGoroutine bool `mapstructure:"enable_goroutine" yaml:"enable_goroutine" json:"enable_goroutine"`
}

type GrpcMetricsConfig struct {
	Enabled      bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Counters     bool `mapstructure:"counters" yaml:"counters" json:"counters"`
	ResponseTime bool `mapstructure:"response_time" yaml:"response_time" json:"response_time"`
}

type FdbMetricsConfig struct {
	Enabled      bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Counters     bool `mapstructure:"counters" yaml:"counters" json:"counters"`
	ResponseTime bool `mapstructure:"response_time" yaml:"response_time" json:"response_time"`
}

type SearchMetricsConfig struct {
	Enabled      bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Counters     bool `mapstructure:"counters" yaml:"counters" json:"counters"`
	ResponseTime bool `mapstructure:"response_time" yaml:"response_time" json:"response_time"`
}

type UsersConfig struct {
	Enabled bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
}

type ObservabilityConfig struct {
	Provider    string `mapstructure:"provider" yaml:"provider" json:"provider"`
	Enabled     bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	ApiKey      string `mapstructure:"api_key" yaml:"api_key" json:"api_key"`
	AppKey      string `mapstructure:"app_key" yaml:"app_key" json:"app_key"`
	ProviderUrl string `mapstructure:"provider_url" yaml:"provider_url" json:"provider_url"`
}

var DefaultConfig = Config{
	Log: log.LogConfig{
		Level:      "info",
		SampleRate: 0.01,
	},
	Server: ServerConfig{
		Host:        "0.0.0.0",
		Port:        8081,
		FDBHardDrop: false,
	},
	Auth: AuthConfig{
		Enabled:          false,
		IssuerURL:        "https://tigrisdata-dev.us.auth0.com/",
		Audience:         "https://tigris-api",
		JWKSCacheTimeout: 5 * time.Minute,
		LogOnly:          true,
		AdminNamespaces:  []string{"tigris-admin"},
	},
	Cdc: CdcConfig{
		Enabled:        false,
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
		Enabled:             false,
		SampleRate:          0.01,
		CodeHotspotsEnabled: true,
		EndpointsEnabled:    true,
	},
	Profiling: ProfilingConfig{
		Enabled:    false,
		EnableCPU:  true,
		EnableHeap: true,
	},
	Quota: QuotaConfig{
		Enabled:                   false,
		RateLimit:                 1000,        // requests per second both reads and writes
		WriteThroughputLimit:      10000000,    // bytes per second
		ReadThroughputLimit:       10000000,    // bytes per second
		DataSizeLimit:             10000000000, // bytes
		LimitUpdateInterval:       5,           // seconds
		TenantSizeRefreshInterval: 60,          // seconds
		AllTenantsRefreshInternal: 300,         // seconds
	},
	Observability: ObservabilityConfig{
		Enabled: false,
	},
	Users: UsersConfig{
		Enabled: true,
	},
}

// FoundationDBConfig keeps FoundationDB configuration parameters
type FoundationDBConfig struct {
	ClusterFile string `mapstructure:"cluster_file" json:"cluster_file" yaml:"cluster_file"`
}

type SearchConfig struct {
	Host         string `mapstructure:"host" json:"host" yaml:"host"`
	Port         int16  `mapstructure:"port" json:"port" yaml:"port"`
	AuthKey      string `mapstructure:"auth_key" json:"auth_key" yaml:"auth_key"`
	ReadEnabled  bool   `mapstructure:"read_enabled" yaml:"read_enabled" json:"read_enabled"`
	WriteEnabled bool   `mapstructure:"write_enabled" yaml:"write_enabled" json:"write_enabled"`
}

type QuotaConfig struct {
	Enabled                   bool
	RateLimit                 int   `mapstructure:"rate_limit" yaml:"rate_limit" json:"rate_limit"`
	WriteThroughputLimit      int   `mapstructure:"write_throughput_limit" yaml:"write_throughput_limit" json:"write_throughput_limit"`
	ReadThroughputLimit       int   `mapstructure:"read_throughput_limit" yaml:"read_throughput_limit" json:"read_throughput_limit"`
	DataSizeLimit             int64 `mapstructure:"data_size_limit" yaml:"data_size_limit" json:"data_size_limit"`
	LimitUpdateInterval       int64 `mapstructure:"limit_update_interval" yaml:"limit_update_interval" json:"limit_update_interval"`
	TenantSizeRefreshInterval int64 `mapstructure:"tenant_size_refresh_interval" yaml:"tenant_size_refresh_interval" json:"tenant_size_refresh_interval"`
	AllTenantsRefreshInternal int64 `mapstructure:"all_tenants_refresh_interval" yaml:"all_tenants_refresh_interval" json:"all_tenants_refresh_interval"`
}

func IsIndexingStoreReadEnabled() bool {
	return DefaultConfig.Search.WriteEnabled && DefaultConfig.Search.ReadEnabled
}
