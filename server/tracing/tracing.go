package tracing

import (
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/util"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func getTracingOptions(c *config.Config) []tracer.StartOption {
	var opts []tracer.StartOption
	rules := []tracer.SamplingRule{tracer.ServiceRule(util.Service, c.Tracing.SampleRate)}
	opts = append(opts, tracer.WithTraceEnabled(c.Tracing.Enabled))
	opts = append(opts, tracer.WithProfilerEndpoints(c.Tracing.EndpointsEnabled))
	opts = append(opts, tracer.WithProfilerCodeHotspots(c.Tracing.CodeHotspotsEnabled))
	opts = append(opts, tracer.WithSamplingRules(rules))
	opts = append(opts, tracer.WithService(util.Service))
	opts = append(opts, tracer.WithEnv(config.GetEnvironment()))
	opts = append(opts, tracer.WithServiceVersion(util.Version))
	if c.Tracing.WithUDS != "" {
		opts = append(opts, tracer.WithUDS(c.Tracing.WithUDS))
	}
	if c.Tracing.WithAgentAddr != "" {
		opts = append(opts, tracer.WithAgentAddr(c.Tracing.WithAgentAddr))
	}
	if c.Tracing.WithDogStatsdAddr != "" {
		opts = append(opts, tracer.WithAgentAddr(c.Tracing.WithAgentAddr))
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

func InitTracer(config *config.Config) (func(), error) {
	if !config.Tracing.Enabled {
		return func() {}, nil
	}

	tracer.Start(getTracingOptions(config)...)

	if config.Profiling.Enabled {
		if err := profiler.Start(getProfilingOptions()...); err != nil {
			return func() {}, err
		}
	}

	return func() { tracer.Stop(); profiler.Stop() }, nil
}
