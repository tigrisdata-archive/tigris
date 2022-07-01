package tracing

import (
	"github.com/tigrisdata/tigris/server/config"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func getTracingOptions() []tracer.StartOption {
	var opts []tracer.StartOption
	tc := config.DefaultConfig.Tracing
	opts = append(opts, tracer.WithTraceEnabled(tc.Enabled))
	if tc.WithUDS != "" {
		opts = append(opts, tracer.WithUDS(tc.WithUDS))
	}
	if tc.WithAgentAddr != "" {
		opts = append(opts, tracer.WithAgentAddr(tc.WithAgentAddr))
	}
	if tc.WithDogStatsdAddr != "" {
		opts = append(opts, tracer.WithAgentAddr(tc.WithAgentAddr))
	}

	return opts
}

func getProfilingOptions() []profiler.Option {
	var opts []profiler.Option
	opts = append(opts, profiler.WithService(config.DefaultConfig.Tags.Service))
	opts = append(opts, profiler.WithEnv(config.DefaultConfig.Tags.Environment))
	opts = append(opts, profiler.WithVersion(config.DefaultConfig.Tags.Version))
	return opts
}

func InitTracer(config *config.Config) error {
	if !config.Profiling.Enabled {
		return nil
	}

	tracer.Start(getTracingOptions()...)
	defer tracer.Stop()

	if config.Profiling.Enabled {
		if err := profiler.Start(getProfilingOptions()...); err != nil {
			return err
		}
		defer profiler.Stop()
	}

	return nil
}
