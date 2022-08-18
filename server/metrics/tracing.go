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
	"fmt"

	"github.com/tigrisdata/tigris/server/config"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/uber-go/tally"
	"google.golang.org/grpc/status"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	KvTracingServiceName      string = "kv"
	TraceServiceName          string = "tigris.grpc.server"
	SessionManagerServiceName string = "session"
)

type SpanMeta struct {
	serviceName  string
	resourceName string
	spanType     string
	tags         map[string]string
	span         tracer.Span
	parent       *SpanMeta
}

type SpanMetaCtxKey struct {
}

func NewSpanMeta(serviceName string, resourceName string, spanType string, tags map[string]string) *SpanMeta {
	return &SpanMeta{serviceName: serviceName, resourceName: resourceName, spanType: spanType, tags: tags}
}

func SpanMetaFromContext(ctx context.Context) (*SpanMeta, bool) {
	s, ok := ctx.Value(SpanMetaCtxKey{}).(*SpanMeta)
	return s, ok
}

func ClearSpanMetaContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, SpanMetaCtxKey{}, nil)
}

func (s *SpanMeta) CountOkForScope(scope tally.Scope) {
	scope.Tagged(s.GetTags()).Counter("ok").Inc(1)
}

func (s *SpanMeta) CountErrorForScope(scope tally.Scope, err error) {
	scope.Tagged(mergeTags(s.GetTags(), getErrorTags(err))).Counter("error").Inc(1)
}

func (s *SpanMeta) GetTags() map[string]string {
	res := s.tags
	for _, tagKey := range getOkTagKeys() {
		if _, ok := s.tags[tagKey]; !ok {
			res[tagKey] = UnknownValue
		}
	}
	return res
}

func (s *SpanMeta) SaveSpanMetaToContext(ctx context.Context) (context.Context, error) {
	if s.span == nil {
		return nil, fmt.Errorf("Parent span was not created")
	}
	ctx = context.WithValue(ctx, SpanMetaCtxKey{}, s)
	return ctx, nil
}

func (s *SpanMeta) GetSpanOptions() []tracer.StartSpanOption {
	return []tracer.StartSpanOption{
		tracer.ServiceName(s.serviceName),
		tracer.ResourceName(s.resourceName),
		tracer.SpanType(s.spanType),
		tracer.Measured(),
	}
}

func (s *SpanMeta) AddTags(tags map[string]string) {
	for k, v := range tags {
		if _, exists := s.tags[k]; !exists || s.tags[k] == UnknownValue {
			s.tags[k] = v
		}
	}
}

func (s *SpanMeta) StartTracing(ctx context.Context, childOnly bool) context.Context {
	if !config.DefaultConfig.Tracing.Enabled {
		return ctx
	}
	spanOpts := s.GetSpanOptions()
	parentSpanMeta, parentExists := SpanMetaFromContext(ctx)
	if parentExists {
		// This is a child span, parents need to be marked
		spanOpts = append(spanOpts, tracer.ChildOf(parentSpanMeta.span.Context()))
		s.parent = parentSpanMeta
		for _, tag := range getOkTagKeys() {
			s.tags[tag] = s.parent.tags[tag]
		}
	}
	if childOnly && !parentExists {
		// There is no parent span, no need to start tracing here
		return ctx
	}
	s.span = tracer.StartSpan(TraceServiceName, spanOpts...)
	for k, v := range s.tags {
		s.span.SetTag(k, v)
	}
	ctx, err := s.SaveSpanMetaToContext(ctx)
	if err != nil {
		ulog.E(err)
	}
	return ctx
}

func (s *SpanMeta) FinishTracing(ctx context.Context) context.Context {
	if s.span != nil {
		s.span.Finish()
	}
	var err error
	if s.parent != nil {
		ctx, err = s.parent.SaveSpanMetaToContext(ctx)
		if err != nil {
			ulog.E(err)
		}
	} else {
		// This was the top level span meta
		ctx = ClearSpanMetaContext(ctx)
	}
	return ctx
}

func (s *SpanMeta) FinishWithError(ctx context.Context, err error) {
	if s.span == nil {
		return
	}
	errCode := status.Code(err)
	s.span.SetTag("grpc.code", errCode.String())
	errTags := getErrorTags(err)
	for k, v := range errTags {
		s.span.SetTag(k, v)
	}
	finishOptions := []tracer.FinishOption{tracer.WithError(err)}
	s.span.Finish(finishOptions...)
	s.span = nil
	ClearSpanMetaContext(ctx)
}
