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

package tracing

import (
	"context"

	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

var (
	OpenTracerProvider *tracesdk.TracerProvider
	OpenTracer         trace.Tracer
)

func getTracingOptions(c *config.Config) []tracer.StartOption {
	var opts []tracer.StartOption
	rules := []tracer.SamplingRule{tracer.ServiceRule(util.Service, c.Tracing.Datadog.SampleRate)}
	opts = append(opts, tracer.WithTraceEnabled(c.Tracing.Enabled))
	opts = append(opts, tracer.WithProfilerEndpoints(c.Tracing.Datadog.EndpointsEnabled))
	opts = append(opts, tracer.WithProfilerCodeHotspots(c.Tracing.Datadog.CodeHotspotsEnabled))
	opts = append(opts, tracer.WithSamplingRules(rules))
	opts = append(opts, tracer.WithService(util.Service))
	opts = append(opts, tracer.WithEnv(config.GetEnvironment()))
	opts = append(opts, tracer.WithServiceVersion(util.Version))
	if c.Tracing.Datadog.WithUDS != "" {
		opts = append(opts, tracer.WithUDS(c.Tracing.Datadog.WithUDS))
	}
	if c.Tracing.Datadog.WithAgentAddr != "" {
		opts = append(opts, tracer.WithAgentAddr(c.Tracing.Datadog.WithAgentAddr))
	}
	if c.Tracing.Datadog.WithDogStatsdAddr != "" {
		opts = append(opts, tracer.WithAgentAddr(c.Tracing.Datadog.WithAgentAddr))
	}

	return opts
}

func getProfilingOptions() []profiler.Option {
	var opts []profiler.Option
	opts = append(opts, profiler.WithService(util.Service))
	opts = append(opts, profiler.WithEnv(config.GetEnvironment()))
	opts = append(opts, profiler.WithVersion(util.Version))
	return opts
}

func initDatadog(config *config.Config) (func(), error) {
	tracer.Start(getTracingOptions(config)...)

	if config.Profiling.Enabled {
		if err := profiler.Start(getProfilingOptions()...); err != nil {
			return func() {}, err
		}
	}

	return func() { tracer.Stop(); profiler.Stop() }, nil
}

func tracerProvider(url string) error {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return err
	}

	OpenTracerProvider = tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exp),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(util.Service),
			attribute.String("environment", config.GetEnvironment()),
			attribute.Int64("ID", 1),
		)),
	)
	return nil
}

func initJaeger(config *config.Config) func() {
	err := tracerProvider(config.Tracing.Jaeger.Url)
	if err != nil {
		ulog.E(err)
	}
	otel.SetTracerProvider(OpenTracerProvider)

	OpenTracer = OpenTracerProvider.Tracer(util.Service)

	return func() {
		if err := OpenTracerProvider.Shutdown(context.Background()); err != nil {
			ulog.E(err)
		}
	}
}

func IsJaegerTracingEnabled(config *config.Config) bool {
	return config.Tracing.Enabled && config.Tracing.Jaeger.Enabled
}

func IsDatadogTracingEnabled(config *config.Config) bool {
	return config.Tracing.Enabled && config.Tracing.Datadog.Enabled
}

func IsTracingEnabled(config *config.Config) bool {
	return config.Tracing.Enabled && (config.Tracing.Datadog.Enabled || config.Tracing.Jaeger.Enabled)
}

func InitTracer(config *config.Config) (func(), error) {
	var tracerClosers []func()

	if IsJaegerTracingEnabled(config) {
		jaegerCloser := initJaeger(config)
		tracerClosers = append(tracerClosers, jaegerCloser)
	}

	if IsDatadogTracingEnabled(config) {
		datadogCloser, err := initDatadog(config)
		if err != nil {
			ulog.E(err)
		}
		tracerClosers = append(tracerClosers, datadogCloser)
	}

	return func() {
		for _, c := range tracerClosers {
			c()
		}
	}, nil
}
