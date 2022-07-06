package tracing

import (
	"github.com/tigrisdata/tigris/server/config"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func getTracingOptions(config *config.Config) []tracer.StartOption {
	var opts []tracer.StartOption
	opts = append(opts, tracer.WithTraceEnabled(config.Tracing.Enabled))
	opts = append(opts, tracer.WithProfilerEndpoints(config.Tracing.EndpointsEnabled))
	opts = append(opts, tracer.WithProfilerCodeHotspots(config.Tracing.CodeHotspotsEnabled))
	opts = append(opts, tracer.WithService(config.Tags.Service))
	opts = append(opts, tracer.WithEnv(config.Tags.Environment))
	opts = append(opts, tracer.WithServiceVersion(config.Tags.Version))
	if config.Tracing.WithUDS != "" {
		opts = append(opts, tracer.WithUDS(config.Tracing.WithUDS))
	}
	if config.Tracing.WithAgentAddr != "" {
		opts = append(opts, tracer.WithAgentAddr(config.Tracing.WithAgentAddr))
	}
	if config.Tracing.WithDogStatsdAddr != "" {
		opts = append(opts, tracer.WithAgentAddr(config.Tracing.WithAgentAddr))
	}

	return opts
}

func getProfilingOptions(config *config.Config) []profiler.Option {
	var opts []profiler.Option
	opts = append(opts, profiler.WithService(config.Tags.Service))
	opts = append(opts, profiler.WithEnv(config.Tags.Environment))
	opts = append(opts, profiler.WithVersion(config.Tags.Version))
	return opts
}

func InitTracer(config *config.Config) (func(), error) {
	if !config.Tracing.Enabled {
		return func() {}, nil
	}

	tracer.Start(getTracingOptions(config)...)

	if config.Profiling.Enabled {
		if err := profiler.Start(getProfilingOptions(config)...); err != nil {
			return func() {}, err
		}
	}

	return func() { tracer.Stop(); profiler.Stop() }, nil
}
