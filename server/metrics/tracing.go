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
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/uber-go/tally"
	"google.golang.org/grpc/status"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	KvTracingServiceName      string = "kv"
	TraceServiceName          string = "tigris.grpc.server"
	SessionManagerServiceName string = "session"
	GrpcSpanType              string = "grpc"
	FdbSpanType               string = "fdb"
	SearchSpanType            string = "search"
	SessionSpanType           string = "session"
	AuthSpanType              string = "auth"
)

type SpanMeta struct {
	serviceName  string
	resourceName string
	spanType     string
	tags         map[string]string
	span         tracer.Span
	parent       *SpanMeta
	started      bool
	stopped      bool
	startedAt    time.Time
	stoppedAt    time.Time
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

func (s *SpanMeta) CountOkForScope(scope tally.Scope, tags map[string]string) {
	scope.Tagged(tags).Counter("ok").Inc(1)
}

func (s *SpanMeta) CountErrorForScope(scope tally.Scope, tags map[string]string) {
	scope.Tagged(tags).Counter("error").Inc(1)
}

func (s *SpanMeta) GetServiceName() string {
	return s.serviceName
}

func (s *SpanMeta) GetResourceName() string {
	return s.resourceName
}

func (s *SpanMeta) GetTags() map[string]string {
	return s.tags
}

func (s *SpanMeta) GetRequestOkTags() map[string]string {
	return standardizeTags(s.tags, getRequestOkTagKeys())
}

func (s *SpanMeta) GetRequestErrorTags(err error) map[string]string {
	return standardizeTags(mergeTags(s.tags, getTagsForError(err, "request")), getRequestErrorTagKeys())
}

func (s *SpanMeta) GetFdbOkTags() map[string]string {
	return standardizeTags(s.tags, getFdbOkTagKeys())
}

func (s *SpanMeta) GetFdbErrorTags(err error) map[string]string {
	return standardizeTags(mergeTags(s.tags, getTagsForError(err, "fdb")), getFdbErrorTagKeys())
}

func (s *SpanMeta) GetSearchOkTags() map[string]string {
	return standardizeTags(s.tags, getSearchOkTagKeys())
}

func (s *SpanMeta) GetSearchErrorTags(err error) map[string]string {
	return standardizeTags(mergeTags(s.tags, getTagsForError(err, "search")), getSearchErrorTagKeys())
}

func (s *SpanMeta) GetSessionOkTags() map[string]string {
	return standardizeTags(s.tags, getSessionOkTagKeys())
}

func (s *SpanMeta) GetSessionErrorTags(err error) map[string]string {
	return standardizeTags(mergeTags(s.tags, getTagsForError(err, "session")), getSessionErrorTagKeys())
}

func (s *SpanMeta) GetNamespaceSizeTags() map[string]string {
	return standardizeTags(s.tags, getNameSpaceSizeTagKeys())
}

func (s *SpanMeta) GetDbSizeTags() map[string]string {
	return standardizeTags(s.tags, getDbSizeTagKeys())
}

func (s *SpanMeta) GetCollectionSizeTags() map[string]string {
	return standardizeTags(s.tags, getCollectionSizeTagKeys())
}

func (s *SpanMeta) GetNetworkTags() map[string]string {
	return standardizeTags(s.tags, getNetworkTagKeys())
}

func (s *SpanMeta) GetAuthOkTags() map[string]string {
	return standardizeTags(s.tags, getAuthOkTagKeys())
}

func (s *SpanMeta) GetAuthErrorTags(err error) map[string]string {
	return standardizeTags(mergeTags(s.tags, getTagsForError(err, "auth")), getAuthErrorTagKeys())
}

func (s *SpanMeta) SaveSpanMetaToContext(ctx context.Context) (context.Context, error) {
	if s.span == nil {
		return nil, fmt.Errorf("parent span was not created")
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
		if _, exists := s.tags[k]; !exists || s.tags[k] == request.UnknownValue {
			s.tags[k] = v
			if s.span != nil {
				// The span already exists, set the tag there as well
				s.span.SetTag(k, v)
			}
		}
	}
}

func (s *SpanMeta) RecursiveAddTags(tags map[string]string) {
	s.AddTags(tags)
	if s.parent != nil {
		s.parent.RecursiveAddTags(tags)
	}
}

func (s *SpanMeta) StartTracing(ctx context.Context, childOnly bool) context.Context {
	if !config.DefaultConfig.Tracing.Enabled {
		return ctx
	}
	s.startedAt = time.Now()
	s.started = true
	spanOpts := s.GetSpanOptions()
	parentSpanMeta, parentExists := SpanMetaFromContext(ctx)
	if parentExists {
		// This is a child span, parents need to be marked
		spanOpts = append(spanOpts, tracer.ChildOf(parentSpanMeta.span.Context()))
		s.parent = parentSpanMeta
		// Copy the tags from the parent span
		s.AddTags(parentSpanMeta.GetTags())
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
	if !s.started {
		log.Error().Str("service_name", s.serviceName).Str("resource_name", s.resourceName).Msg("Finish tracing called before starting the trace")
		return ctx
	}
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
	s.stopped = true
	s.stoppedAt = time.Now()
	return ctx
}

func (s *SpanMeta) RecordDuration(scope tally.Scope, tags map[string]string) {
	// Should be called after tracing is finished
	if !s.started || !s.stopped {
		log.Error().Str("service_name", s.serviceName).Str("resource_name", s.resourceName).Msg("RecordDuration was called on a span that was not started ot stopped")
		return
	}
	scope.Tagged(tags).Timer("time").Record(s.stoppedAt.Sub(s.startedAt))
}

func (s *SpanMeta) FinishWithError(ctx context.Context, source string, err error) context.Context {
	if s.span == nil {
		return nil
	}
	errCode := status.Code(err)
	s.span.SetTag("grpc.code", errCode.String())
	errTags := getTagsForError(err, source)
	for k, v := range errTags {
		s.span.SetTag(k, v)
	}
	finishOptions := []tracer.FinishOption{tracer.WithError(err)}
	s.span.Finish(finishOptions...)
	s.span = nil
	ClearSpanMetaContext(ctx)
	s.stopped = true
	s.stoppedAt = time.Now()
	return ctx
}
