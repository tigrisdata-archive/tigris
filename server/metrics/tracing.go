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

package metrics

import (
	"context"

	"github.com/tigrisdata/tigris/server/config"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	KvTracingServiceName        = "kv"
	TxManagerTracingServiceName = "txmanager"
	TraceServiceName            = "tigris.grpc.server"
)

type SpanMeta struct {
	serviceName  string
	resourceName string
	spanType     string
	tags         map[string]string
}

func NewSpanMeta(serviceName string, resourceName string, spanType string, tags map[string]string) *SpanMeta {
	return &SpanMeta{serviceName: serviceName, resourceName: resourceName, spanType: spanType, tags: tags}
}

func (s *SpanMeta) GetSpanOptions() []tracer.StartSpanOption {
	return []tracer.StartSpanOption{
		tracer.ServiceName(s.serviceName),
		tracer.ResourceName(s.resourceName),
		tracer.SpanType(s.spanType),
		tracer.Measured(),
	}
}

func (s *SpanMeta) StartTracing(ctx context.Context, childOnly bool) (context.Context, func()) {
	if !config.DefaultConfig.Tracing.Enabled {
		return ctx, func() {}
	}
	spanOpts := s.GetSpanOptions()
	parentSpan, exists := tracer.SpanFromContext(ctx)
	if exists {
		// This is a child span, parents need to be marked
		spanOpts = append(spanOpts, tracer.ChildOf(parentSpan.Context()))
	}
	if childOnly && !exists {
		// There is no parent span, no need to start tracing here
		return ctx, func() {}
	}
	span := tracer.StartSpan(TraceServiceName, spanOpts...)
	for k, v := range s.tags {
		span.SetTag(k, v)
	}
	ctx = tracer.ContextWithSpan(ctx, span)
	return ctx, func() {
		span.Finish()
	}
}
