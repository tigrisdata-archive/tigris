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

package config

import (
	"time"

	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/tigrisdata/tigris/util/log"
)

const (
	DatabaseServerType = "database"
	RealtimeServerType = "realtime"
)

type ServerConfig struct {
	Host         string
	Port         int16
	Type         string `json:"type"          mapstructure:"type"          yaml:"type"`
	FDBHardDrop  bool   `json:"fdb_hard_drop" mapstructure:"fdb_hard_drop" yaml:"fdb_hard_drop"`
	RealtimePort int16  `json:"realtime_port" mapstructure:"realtime_port" yaml:"realtime_port"`
	UnixSocket   string `json:"unix_socket"   mapstructure:"unix_socket"   yaml:"unix_socket"`
	TLSKey       string `json:"tls_key"       mapstructure:"tls_key"       yaml:"tls_key"`
	TLSCert      string `json:"tls_cert"      mapstructure:"tls_cert"      yaml:"tls_cert"`
	// route TLS traffic to HTTP, by default GRPC handles TLS traffic
	TLSHttp bool `json:"tls_http" mapstructure:"tls_http" yaml:"tls_http"`
}

type Config struct {
	Log             log.LogConfig
	Server          ServerConfig         `json:"server"           yaml:"server"`
	Auth            AuthConfig           `json:"auth"             yaml:"auth"`
	MetadataCluster ClusterConfig        `json:"metadata_cluster" mapstructure:"metadata_cluster" yaml:"metadata_cluster"`
	Billing         Billing              `json:"billing"          yaml:"billing"`
	Cdc             CdcConfig            `json:"cdc"              yaml:"cdc"`
	Search          SearchConfig         `json:"search"           yaml:"search"`
	KV              KVConfig             `json:"kv"               yaml:"kv"`
	SecondaryIndex  SecondaryIndexConfig `json:"secondary_index"  mapstructure:"secondary_index"  yaml:"secondary_index"`
	Workers         WorkersConfig        `json:"workers"          yaml:"workers"`
	Cache           CacheConfig          `json:"cache"            yaml:"cache"`
	Tracing         TracingConfig        `json:"tracing"          yaml:"tracing"`
	Metrics         MetricsConfig        `json:"metrics"          yaml:"metrics"`
	Profiling       ProfilingConfig      `json:"profiling"        yaml:"profiling"`
	FoundationDB    FoundationDBConfig
	Quota           QuotaConfig
	Observability   ObservabilityConfig `json:"observability" yaml:"observability"`
	Management      ManagementConfig    `json:"management"    yaml:"management"`
	GlobalStatus    GlobalStatusConfig  `json:"global_status" yaml:"global_status"`
	Schema          SchemaConfig
}

type Gotrue struct {
	URL                string `json:"url"                  mapstructure:"url"                  yaml:"url"`
	UsernameSuffix     string `json:"username_suffix"      mapstructure:"username_suffix"      yaml:"username_suffix"`
	AdminUsername      string `json:"admin_username"       mapstructure:"admin_username"       yaml:"admin_username"`
	AdminPassword      string `json:"admin_password"       mapstructure:"admin_password"       yaml:"admin_password"`
	ClientIdLength     int    `json:"client_id_length"     mapstructure:"client_id_length"     yaml:"client_id_length"`
	ClientSecretLength int    `json:"client_secret_length" mapstructure:"client_secret_length" yaml:"client_secret_length"`
	// this is used to sign tokens with symmetric key signature algorithm
	SharedSecret string `json:"shared_secret" mapstructure:"shared_secret" yaml:"shared_secret"`
}

type AuthzConfig struct {
	Enabled bool `json:"enabled"  mapstructure:"enabled"  yaml:"enabled"`
	LogOnly bool `json:"log_only" mapstructure:"log_only" yaml:"log_only"`
}

type AuthConfig struct {
	Enabled                    bool                  `json:"enabled"                       mapstructure:"enabled"                       yaml:"enabled"`
	Validators                 []ValidatorConfig     `json:"validators"                    mapstructure:"validators"                    yaml:"validators"`
	ApiKeys                    ApiKeysConfig         `json:"api_keys"                      mapstructure:"api_keys"                      yaml:"api_keys"`
	PrimaryAudience            string                `json:"primary_audience"              mapstructure:"primary_audience"              yaml:"primary_audience"`
	JWKSCacheTimeout           time.Duration         `json:"jwks_cache_timeout"            mapstructure:"jwks_cache_timeout"            yaml:"jwks_cache_timeout"`
	LogOnly                    bool                  `json:"log_only"                      mapstructure:"log_only"                      yaml:"log_only"`
	EnableNamespaceIsolation   bool                  `json:"enable_namespace_isolation"    mapstructure:"enable_namespace_isolation"    yaml:"enable_namespace_isolation"`
	AdminNamespaces            []string              `json:"admin_namespaces"              mapstructure:"admin_namespaces"              yaml:"admin_namespaces"`
	OAuthProvider              string                `json:"oauth_provider"                mapstructure:"oauth_provider"                yaml:"oauth_provider"`
	ClientId                   string                `json:"client_id"                     mapstructure:"client_id"                     yaml:"client_id"`
	ExternalTokenURL           string                `json:"external_token_url"            mapstructure:"external_token_url"            yaml:"external_token_url"`
	EnableOauth                bool                  `json:"enable_oauth"                  mapstructure:"enable_oauth"                  yaml:"enable_oauth"`
	TokenValidationCacheSize   int                   `json:"token_cache_size"              mapstructure:"token_cache_size"              yaml:"token_cache_size"`
	TokenValidationCacheTTLSec int                   `json:"token_cache_ttl_sec"           mapstructure:"token_cache_ttl_sec"           yaml:"token_cache_ttl_sec"`
	ExternalDomain             string                `json:"external_domain"               mapstructure:"external_domain"               yaml:"external_domain"`
	ManagementClientId         string                `json:"management_client_id"          mapstructure:"management_client_id"          yaml:"management_client_id"`
	ManagementClientSecret     string                `json:"management_client_secret"      mapstructure:"management_client_secret"      yaml:"management_client_secret"`
	TokenClockSkewDurationSec  int                   `json:"token_clock_skew_duration_sec" mapstructure:"token_clock_skew_duration_sec" yaml:"token_clock_skew_duration_sec"`
	Gotrue                     Gotrue                `json:"gotrue"                        mapstructure:"gotrue"                        yaml:"gotrue"`
	NamespaceLocalization      NamespaceLocalization `json:"namespace_localization"        mapstructure:"namespace_localization"        yaml:"namespace_localization"`
	EnableNamespaceDeletion    bool                  `json:"enable_namespace_deletion"     mapstructure:"enable_namespace_deletion"     yaml:"enable_namespace_deletion"`
	EnableNamespaceCreation    bool                  `json:"enable_namespace_creation"     mapstructure:"enable_namespace_creation"     yaml:"enable_namespace_creation"`
	UserInvitations            Invitation            `json:"user_invitations"              mapstructure:"user_invitations"              yaml:"user_invitations"`
	Authz                      AuthzConfig           `json:"authz"                         mapstructure:"authz"                         yaml:"authz"`
	EnableErrorLog             bool                  `json:"enable_error_log"              mapstructure:"enable_error_log"              yaml:"enable_error_log"`
}

type Invitation struct {
	Enabled        bool  `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
	ExpireAfterSec int64 `json:"expire_after_sec" mapstructure:"expire_after_sec" yaml:"expire_after_sec"`
}

type ClusterConfig struct {
	Url   string `json:"url"   mapstructure:"url"   yaml:"url"`
	Token string `json:"token" mapstructure:"token" yaml:"token"`
}

type NamespaceLocalization struct {
	Enabled bool `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
}

type ValidatorConfig struct {
	Issuer    string                       `json:"issuer"    mapstructure:"issuer"    yaml:"issuer"`
	Algorithm validator.SignatureAlgorithm `json:"algorithm" mapstructure:"algorithm" yaml:"algorithm"`
	Audience  string                       `json:"audience"  mapstructure:"audience"  yaml:"audience"`
}

type ApiKeysConfig struct {
	Auds         []string `json:"auds"          mapstructure:"auds"          yaml:"auds"`
	Length       int      `json:"length"        mapstructure:"length"        yaml:"length"`
	EmailSuffix  string   `json:"email_suffix"  mapstructure:"email_suffix"  yaml:"email_suffix"`
	UserPassword string   `json:"user_password" mapstructure:"user_password" yaml:"user_password"`
}

type CdcConfig struct {
	Enabled        bool `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
	StreamInterval time.Duration
	StreamBatch    int
	StreamBuffer   int
}

type TracingConfig struct {
	Enabled bool                 `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
	Datadog DatadogTracingConfig `json:"datadog" mapstructure:"datadog" yaml:"datadog"`
	Jaeger  JaegerTracingConfig  `json:"jaeger"  mapstructure:"jaeger"  yaml:"jaeger"`
}

type DatadogTracingConfig struct {
	Enabled             bool    `json:"enabled"              mapstructure:"enabled"              yaml:"enabled"`
	SampleRate          float64 `json:"sample_rate"          mapstructure:"sample_rate"          yaml:"sample_rate"`
	CodeHotspotsEnabled bool    `json:"codehotspots_enabled" mapstructure:"codehotspots_enabled" yaml:"codehotspots_enabled"`
	EndpointsEnabled    bool    `json:"endpoints_enabled"    mapstructure:"endpoints_enabled"    yaml:"endpoints_enabled"`
	WithUDS             string  `json:"agent_socket"         mapstructure:"agent_socket"         yaml:"agent_socket"`
	WithAgentAddr       string  `json:"agent_addr"           mapstructure:"agent_addr"           yaml:"agent_addr"`
	WithDogStatsdAddr   string  `json:"dogstatsd_addr"       mapstructure:"dogstatsd_addr"       yaml:"dogstatsd_addr"`
}

type JaegerTracingConfig struct {
	Enabled    bool    `json:"enabled"     mapstructure:"enabled"     yaml:"enabled"`
	Url        string  `json:"url"         mapstructure:"url"         yaml:"url"`
	SampleRate float64 `json:"sample_rate" mapstructure:"sample_rate" yaml:"sample_rate"`
}

type MetricsConfig struct {
	Enabled           bool                        `json:"enabled"              mapstructure:"enabled"              yaml:"enabled"`
	DebugMessages     bool                        `json:"debug_messages"       mapstructure:"debug_messages"       yaml:"debug_messages"`
	TimerQuantiles    []float64                   `json:"quantiles"            mapstructure:"quantiles"            yaml:"quantiles"`
	LogLongMethodTime time.Duration               `json:"log_long_method_time" mapstructure:"log_long_method_time" yaml:"log_long_method_time"`
	LongRequestConfig LongRequestConfig           `json:"long_request_config"  mapstructure:"long_request_config"  yaml:"long_request_config"`
	Requests          RequestsMetricGroupConfig   `json:"requests"             mapstructure:"requests"             yaml:"requests"`
	Fdb               FdbMetricGroupConfig        `json:"fdb"                  mapstructure:"fdb"                  yaml:"fdb"`
	Search            SearchMetricGroupConfig     `json:"search"               mapstructure:"search"               yaml:"search"`
	Session           SessionMetricGroupConfig    `json:"session"              mapstructure:"session"              yaml:"session"`
	Size              SizeMetricGroupConfig       `json:"size"                 mapstructure:"size"                 yaml:"size"`
	Network           NetworkMetricGroupConfig    `json:"network"              mapstructure:"network"              yaml:"network"`
	Auth              AuthMetricsConfig           `json:"auth"                 mapstructure:"auth"                 yaml:"auth"`
	SecondaryIndex    SecondaryIndexMetricsConfig `json:"secondary_index"      mapstructure:"secondary_index"      yaml:"secondary_index"`
	Queue             QueueMetricsConfig          `json:"queue"                mapstructure:"queue"                yaml:"queue"`
	Metronome         MetronomeMetricsConfig      `json:"metronome"            mapstructure:"metronome"            yaml:"metronome"`
}

type LongRequestConfig struct {
	FilteredMethods []string `json:"filtered_methods" mapstructure:"filtered_methods" yaml:"filtered_methods"`
}

type TimerConfig struct {
	TimerEnabled     bool `json:"timer_enabled"     mapstructure:"timer_enabled"     yaml:"timer_enabled"`
	HistogramEnabled bool `json:"histogram_enabled" mapstructure:"histogram_enabled" yaml:"histogram_enabled"`
}

type CounterConfig struct {
	OkEnabled    bool `json:"ok_enabled"    mapstructure:"ok_enabled"    yaml:"ok_enabled"`
	ErrorEnabled bool `json:"error_enabled" mapstructure:"error_enabled" yaml:"error_enabled"`
}

type RequestsMetricGroupConfig struct {
	Enabled      bool          `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	Counter      CounterConfig `json:"counter"       mapstructure:"counter"       yaml:"counter"`
	Timer        TimerConfig   `json:"timer"         mapstructure:"timer"         yaml:"timer"`
	FilteredTags []string      `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type FdbMetricGroupConfig struct {
	Enabled      bool          `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	Counter      CounterConfig `json:"counter"       mapstructure:"counter"       yaml:"counter"`
	Timer        TimerConfig   `json:"timer"         mapstructure:"timer"         yaml:"timer"`
	FilteredTags []string      `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type SearchMetricGroupConfig struct {
	Enabled      bool          `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	Counter      CounterConfig `json:"counter"       mapstructure:"counter"       yaml:"counter"`
	Timer        TimerConfig   `json:"timer"         mapstructure:"timer"         yaml:"timer"`
	FilteredTags []string      `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type SessionMetricGroupConfig struct {
	Enabled      bool          `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	Counter      CounterConfig `json:"counter"       mapstructure:"counter"       yaml:"counter"`
	Timer        TimerConfig   `json:"timer"         mapstructure:"timer"         yaml:"timer"`
	FilteredTags []string      `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type SizeMetricGroupConfig struct {
	Enabled      bool     `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	Namespace    bool     `json:"namespace"     mapstructure:"namespace"     yaml:"namespace"`
	Project      bool     `json:"project"       mapstructure:"project"       yaml:"project"`
	Collection   bool     `json:"collection"    mapstructure:"collection"    yaml:"collection"`
	FilteredTags []string `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type NetworkMetricGroupConfig struct {
	Enabled      bool     `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	FilteredTags []string `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type AuthMetricsConfig struct {
	Enabled      bool     `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	FilteredTags []string `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type SecondaryIndexMetricsConfig struct {
	Enabled      bool          `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	Counter      CounterConfig `json:"counter"       mapstructure:"counter"       yaml:"counter"`
	Timer        TimerConfig   `json:"timer"         mapstructure:"timer"         yaml:"timer"`
	FilteredTags []string      `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type MetronomeMetricsConfig struct {
	Enabled      bool          `json:"enabled"       mapstructure:"enabled"       yaml:"enabled"`
	Counter      CounterConfig `json:"counter"       mapstructure:"counter"       yaml:"counter"`
	Timer        TimerConfig   `json:"timer"         mapstructure:"timer"         yaml:"timer"`
	FilteredTags []string      `json:"filtered_tags" mapstructure:"filtered_tags" yaml:"filtered_tags"`
}

type QueueMetricsConfig struct {
	Enabled bool `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
}

type WorkersConfig struct {
	Enabled       bool `json:"enabled"        mapstructure:"enabled"        yaml:"enabled"`
	Count         uint `json:"count"          mapstructure:"count"          yaml:"count"`
	SearchEnabled bool `json:"search_enabled" mapstructure:"search_enabled" yaml:"search_enabled"`
}

type ProfilingConfig struct {
	Enabled         bool `json:"enabled"          mapstructure:"enabled"          yaml:"enabled"`
	EnableCPU       bool `json:"enable_cpu"       mapstructure:"enable_cpu"       yaml:"enable_cpu"`
	EnableHeap      bool `json:"enable_heap"      mapstructure:"enable_heap"      yaml:"enable_heap"`
	EnableBlock     bool `json:"enable_block"     mapstructure:"enable_block"     yaml:"enable_block"`
	EnableMutex     bool `json:"enable_mutex"     mapstructure:"enable_mutex"     yaml:"enable_mutex"`
	EnableGoroutine bool `json:"enable_goroutine" mapstructure:"enable_goroutine" yaml:"enable_goroutine"`
}

type ManagementConfig struct {
	Enabled bool `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
}

type ObservabilityConfig struct {
	Provider    string `json:"provider"     mapstructure:"provider"     yaml:"provider"`
	Enabled     bool   `json:"enabled"      mapstructure:"enabled"      yaml:"enabled"`
	ApiKey      string `json:"api_key"      mapstructure:"api_key"      yaml:"api_key"`
	AppKey      string `json:"app_key"      mapstructure:"app_key"      yaml:"app_key"`
	ProviderUrl string `json:"provider_url" mapstructure:"provider_url" yaml:"provider_url"`
}

type GlobalStatusConfig struct {
	Enabled       bool `json:"enabled"        mapstructure:"enabled"        yaml:"enabled"`
	EmitMetrics   bool `json:"emit_metrics"   mapstructure:"emit_metrics"   yaml:"emit_metrics"`
	DebugMessages bool `json:"debug_messages" mapstructure:"debug_messages" yaml:"debug_messages"`
}

type Billing struct {
	Metronome Metronome       `json:"metronome" mapstructure:"metronome" yaml:"metronome"`
	Reporter  BillingReporter `json:"reporter"  mapstructure:"reporter"  yaml:"reporter"`
}

type Metronome struct {
	Disabled      bool          `json:"disabled"       mapstructure:"disabled"       yaml:"disabled"`
	URL           string        `json:"url"            mapstructure:"url"            yaml:"url"`
	ApiKey        string        `json:"api_key"        mapstructure:"api_key"        yaml:"api_key"`
	DefaultPlan   string        `json:"default_plan"   mapstructure:"default_plan"   yaml:"default_plan"`
	BilledMetrics BilledMetrics `json:"billed_metrics" mapstructure:"billed_metrics" yaml:"billed_metrics"`
}

type BilledMetrics = map[string]string

type BillingReporter struct {
	Disabled        bool          `json:"disabled"         mapstructure:"disabled"         yaml:"disabled"`
	RefreshInterval time.Duration `json:"refresh_interval" mapstructure:"refresh_interval" yaml:"refresh_interval"`
}

var (
	WriteUnitSize  = 4096
	ReadUnitSize   = 4096
	SearchUnitSize = 4096
)

var DefaultConfig = Config{
	Log: log.LogConfig{
		Level:      "info",
		SampleRate: 0.01,
	},
	Server: ServerConfig{
		Type:         DatabaseServerType,
		Host:         "0.0.0.0",
		Port:         8081,
		UnixSocket:   "",
		RealtimePort: 8083,
		FDBHardDrop:  true,
	},
	Auth: AuthConfig{
		Enabled: false,
		Validators: []ValidatorConfig{
			{
				Audience: "https://tigris-api",
			},
		},
		ApiKeys: ApiKeysConfig{
			Auds:   nil,
			Length: 120,
		},
		PrimaryAudience:            "https://tigris-api",
		JWKSCacheTimeout:           5 * time.Minute,
		TokenValidationCacheSize:   1000,
		TokenValidationCacheTTLSec: 7200, // 2hr
		LogOnly:                    true,
		AdminNamespaces:            []string{"tigris-admin"},
		Gotrue: Gotrue{
			ClientIdLength:     30,
			ClientSecretLength: 50,
		},
		NamespaceLocalization:   NamespaceLocalization{Enabled: false},
		EnableNamespaceDeletion: false,
		EnableNamespaceCreation: true,
		UserInvitations: Invitation{
			Enabled:        true,
			ExpireAfterSec: 259200, // 3days
		},
		Authz:          AuthzConfig{Enabled: false, LogOnly: true},
		EnableErrorLog: true,
	},
	Billing: Billing{
		Metronome: Metronome{
			URL:    "https://api.metronome.com/v1",
			ApiKey: "replace_me",
			// random placeholder UUID and not an actual plan
			DefaultPlan: "47eda90f-d2e8-4184-8955-cb3a6467782b",
		},
		Reporter: BillingReporter{
			RefreshInterval: time.Second * 60, // 60 seconds
		},
	},
	Cdc: CdcConfig{
		Enabled:        false,
		StreamInterval: 500 * time.Millisecond,
		StreamBatch:    100,
		StreamBuffer:   200,
	},
	Search: SearchConfig{
		Host:              "localhost",
		Port:              8108,
		ReadEnabled:       true,
		WriteEnabled:      true,
		StorageEnabled:    true,
		Chunking:          true,
		Compression:       false,
		IgnoreExtraFields: false,
		LogFilter:         false,
	},
	KV: KVConfig{
		Chunking:             false,
		Compression:          false,
		MinCompressThreshold: 0,
	},
	SecondaryIndex: SecondaryIndexConfig{
		ReadEnabled:   true,
		WriteEnabled:  true,
		MutateEnabled: false,
	},
	Cache: CacheConfig{
		Host:    "0.0.0.0",
		Port:    6379,
		MaxScan: 500,
	},
	Tracing: TracingConfig{
		Enabled: false,
		Datadog: DatadogTracingConfig{
			Enabled:             false,
			SampleRate:          0.01,
			CodeHotspotsEnabled: true,
			EndpointsEnabled:    true,
		},
		Jaeger: JaegerTracingConfig{
			Enabled:    true,
			Url:        "http://tigris_jaeger:14268/api/traces",
			SampleRate: 0.01,
		},
	},
	Metrics: MetricsConfig{
		Enabled:           true,
		DebugMessages:     false,
		TimerQuantiles:    []float64{0.5, 0.95, 0.99},
		LogLongMethodTime: 500 * time.Millisecond,
		LongRequestConfig: LongRequestConfig{
			FilteredMethods: []string{
				"QueryTimeSeriesMetrics",
			},
		},
		Requests: RequestsMetricGroupConfig{
			Enabled: true,
			Counter: CounterConfig{
				OkEnabled:    true,
				ErrorEnabled: true,
			},
			Timer: TimerConfig{
				TimerEnabled:     true,
				HistogramEnabled: false,
			},
			FilteredTags: nil,
		},
		Fdb: FdbMetricGroupConfig{
			Enabled: true,
			Counter: CounterConfig{
				OkEnabled:    true,
				ErrorEnabled: true,
			},
			Timer: TimerConfig{
				TimerEnabled:     true,
				HistogramEnabled: false,
			},
			FilteredTags: nil,
		},
		Search: SearchMetricGroupConfig{
			Enabled: true,
			Counter: CounterConfig{
				OkEnabled:    true,
				ErrorEnabled: true,
			},
			Timer: TimerConfig{
				TimerEnabled:     true,
				HistogramEnabled: false,
			},
			FilteredTags: nil,
		},
		SecondaryIndex: SecondaryIndexMetricsConfig{
			Enabled: true,
			Counter: CounterConfig{
				OkEnabled:    true,
				ErrorEnabled: true,
			},
			Timer: TimerConfig{
				TimerEnabled:     true,
				HistogramEnabled: false,
			},
			FilteredTags: nil,
		},
		Metronome: MetronomeMetricsConfig{
			Enabled: true,
			Counter: CounterConfig{
				OkEnabled:    true,
				ErrorEnabled: true,
			},
			Timer: TimerConfig{
				TimerEnabled:     true,
				HistogramEnabled: false,
			},
		},
		Session: SessionMetricGroupConfig{
			Enabled: true,
			Counter: CounterConfig{
				OkEnabled:    true,
				ErrorEnabled: true,
			},
			Timer: TimerConfig{
				TimerEnabled:     true,
				HistogramEnabled: false,
			},
			FilteredTags: nil,
		},
		Size: SizeMetricGroupConfig{
			Enabled:      true,
			Namespace:    true,
			Project:      true,
			Collection:   true,
			FilteredTags: nil,
		},
		Network: NetworkMetricGroupConfig{
			Enabled:      true,
			FilteredTags: nil,
		},
		Auth: AuthMetricsConfig{
			Enabled:      true,
			FilteredTags: nil,
		},
		Queue: QueueMetricsConfig{
			Enabled: true,
		},
	},
	Profiling: ProfilingConfig{
		Enabled:    false,
		EnableCPU:  true,
		EnableHeap: true,
	},
	Quota: QuotaConfig{
		// Maximum limits single node can handle. Across namespaces.
		Node: LimitsConfig{
			Enabled: false,

			ReadUnits:  4000, // read requests per second per namespace
			WriteUnits: 1000, // write requests per second
		},
		Namespace: NamespaceLimitsConfig{
			// Per cluster limits for single namespace
			Default: LimitsConfig{
				Enabled: false,

				ReadUnits:  100, // read requests per second per namespace
				WriteUnits: 25,  // write requests per second
			},
			// Maximum per node quota for single namespace
			Node: LimitsConfig{
				ReadUnits:  100, // read requests per second per namespace
				WriteUnits: 25,  // write requests per second
			},
			RefreshInterval: 60 * time.Second,
			Regulator: QuotaRegulator{
				Increment:  5,
				Hysteresis: 10,
			},
		},
		Storage: StorageLimitsConfig{
			Enabled:         false,
			DataSizeLimit:   100 * 1024 * 1024,
			RefreshInterval: 600 * time.Second,
		},
	},
	Observability: ObservabilityConfig{
		Enabled:     false,
		Provider:    "datadog",
		ProviderUrl: "us3.datadoghq.com",
	},
	Management: ManagementConfig{
		Enabled: true,
	},
	Schema: SchemaConfig{
		AllowIncompatible: false,
	},
	GlobalStatus: GlobalStatusConfig{
		Enabled:       true,
		EmitMetrics:   true,
		DebugMessages: false,
	},
	MetadataCluster: ClusterConfig{
		Url: "https://api.global.tigrisdata.cloud",
	},
	Workers: WorkersConfig{
		Enabled:       false,
		Count:         2,
		SearchEnabled: false,
	},
}

// SchemaConfig contains schema related settings.
type SchemaConfig struct {
	// AllowIncompatible when set to true enables incompatible schema changes support.
	//
	// Compatible schema change include:
	//	* adding new fields
	//  * adding defaults
	//  * extending max_length of the string fields
	//
	// Incompatible schema changes include:
	//	* deleting a field
	//  * changing field type
	//  * reducing max_length of the string fields
	//  * setting "required" property
	AllowIncompatible bool `json:"allow_incompatible" mapstructure:"allow_incompatible" yaml:"allow_incompatible"`
}

// KVConfig keeps KV store configuration parameters.
type KVConfig struct {
	// Chunking allows us to persist bigger payload in storage.
	Chunking bool `json:"chunking" mapstructure:"chunking" yaml:"chunking"`
	// Compression allows us to compress payload before storing in storage.
	Compression          bool  `json:"compression"               mapstructure:"compression"               yaml:"compression"`
	MinCompressThreshold int32 `json:"min_compression_threshold" mapstructure:"min_compression_threshold" yaml:"min_compression_threshold"`
}

// FoundationDBConfig keeps FoundationDB configuration parameters.
type FoundationDBConfig struct {
	ClusterFile string `json:"cluster_file" mapstructure:"cluster_file" yaml:"cluster_file"`
}

type SearchConfig struct {
	Host         string `json:"host"          mapstructure:"host"          yaml:"host"`
	Port         int16  `json:"port"          mapstructure:"port"          yaml:"port"`
	AuthKey      string `json:"auth_key"      mapstructure:"auth_key"      yaml:"auth_key"`
	ReadEnabled  bool   `json:"read_enabled"  mapstructure:"read_enabled"  yaml:"read_enabled"`
	WriteEnabled bool   `json:"write_enabled" mapstructure:"write_enabled" yaml:"write_enabled"`
	// StorageEnabled only applies to standalone search indexes. This is to enable persisting search indexes to storage.
	StorageEnabled bool `json:"storage_enabled" mapstructure:"storage_enabled" yaml:"storage_enabled"`
	// Chunking allows us to persist bigger search indexes payload in storage.
	Chunking bool `json:"chunking" mapstructure:"chunking" yaml:"chunking"`
	// Compression allows us to compress payload before storing in storage.
	Compression       bool `json:"compression"         mapstructure:"compression"         yaml:"compression"`
	IgnoreExtraFields bool `json:"ignore_extra_fields" mapstructure:"ignore_extra_fields" yaml:"ignore_extra_fields"`
	LogFilter         bool `json:"log_filter"          mapstructure:"log_filter"          yaml:"log_filter"`

	DoNotReloadMetadata bool `json:"do_not_reload_metadata" mapstructure:"do_not_reload_metadata" yaml:"do_not_reload_metadata"`
}

type SecondaryIndexConfig struct {
	ReadEnabled   bool `json:"read_enabled"   mapstructure:"read_enabled"   yaml:"read_enabled"`
	WriteEnabled  bool `json:"write_enabled"  mapstructure:"write_enabled"  yaml:"write_enabled"`
	MutateEnabled bool `json:"mutate_enabled" mapstructure:"mutate_enabled" yaml:"mutate_iterator"`
}

type CacheConfig struct {
	Host    string `json:"host"     mapstructure:"host"     yaml:"host"`
	Port    int16  `json:"port"     mapstructure:"port"     yaml:"port"`
	MaxScan int64  `json:"max_scan" mapstructure:"max_scan" yaml:"max_scan"`
}

type LimitsConfig struct {
	Enabled bool

	ReadUnits  int `json:"read_units"  mapstructure:"read_units"  yaml:"read_units"`
	WriteUnits int `json:"write_units" mapstructure:"write_units" yaml:"write_units"`
}

func (l *LimitsConfig) Limit(isWrite bool) int {
	if isWrite {
		return l.WriteUnits
	}

	return l.ReadUnits
}

type NamespaceLimitsConfig struct {
	Enabled    bool
	Default    LimitsConfig            // default per namespace limit
	Node       LimitsConfig            // max per node per namespace limit
	Namespaces map[string]LimitsConfig // individual namespaces configuration

	RefreshInterval time.Duration `json:"refresh_interval" mapstructure:"refresh_interval" yaml:"refresh_interval"`
	Regulator       QuotaRegulator
}

func (n *NamespaceLimitsConfig) NamespaceLimits(ns string) *LimitsConfig {
	cfg, ok := n.Namespaces[ns]
	if ok {
		return &cfg
	}
	return &n.Default
}

type NamespaceStorageLimitsConfig struct {
	Size int64
}

type StorageLimitsConfig struct {
	Enabled         bool
	DataSizeLimit   int64         `json:"data_size_limit"  mapstructure:"data_size_limit"  yaml:"data_size_limit"`
	RefreshInterval time.Duration `json:"refresh_interval" mapstructure:"refresh_interval" yaml:"refresh_interval"`

	// Per namespace limits
	Namespaces map[string]NamespaceStorageLimitsConfig
}

func (n *StorageLimitsConfig) NamespaceLimits(ns string) int64 {
	cfg, ok := n.Namespaces[ns]
	if ok {
		return cfg.Size
	}
	return n.DataSizeLimit
}

type QuotaRegulator struct {
	// This is a hysteresis band, deviation from ideal value in which regulation is no happening
	Hysteresis int `json:"hysteresis" mapstructure:"hysteresis" yaml:"hysteresis"`

	// when rate adjustment is needed increment current rate by ± this value
	// (this is percentage of maximum per node per namespace limit)
	// Set by config.DefaultConfig.Quota.Namespace.Node.(Read|Write)RateLimit.
	Increment int `json:"increment" mapstructure:"increment" yaml:"increment"`
}

type QuotaConfig struct {
	Node      LimitsConfig          // maximum rates per node. protects the node from overloading
	Namespace NamespaceLimitsConfig // user quota across all the nodes
	Storage   StorageLimitsConfig

	WriteUnitSize int
	ReadUnitSize  int
}

func (s *SearchConfig) IsReadEnabled() bool {
	return s.WriteEnabled && s.ReadEnabled
}
