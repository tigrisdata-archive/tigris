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

package metrics

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/defaults"
	"github.com/tigrisdata/tigris/server/tracing"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/uber-go/tally"
	"go.opentelemetry.io/otel/attribute"
	opentrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc/status"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	KvTracingServiceName      string = "kv"
	TraceServiceName          string = "tigris.grpc.server"
	SessionManagerServiceName string = "session"
	SecondaryIndexServiceName string = "secondaryindex"
	MetronomeServiceName      string = "metronome"
	GrpcSpanType              string = "grpc"
	FdbSpanType               string = "fdb"
	SearchSpanType            string = "search"
	SecondaryIndexSpanType    string = "secondary_index"
	SessionSpanType           string = "session"
	AuthSpanType              string = "auth"
	MetronomeSpanType         string = "metronome"
)

type Measurement struct {
	serviceName         string
	resourceName        string
	spanType            string
	tags                map[string]string
	jaegerSpan          opentrace.Span
	datadogSpan         ddtracer.Span
	parent              *Measurement
	started             bool
	stopped             bool
	startedAt           time.Time
	stoppedAt           time.Time
	firstDocumentSentAt time.Time
	projectCollTags     map[string]string
	nDocs               int64
	sentBytes           int
	receivedBytes       int
	hasError            bool
	sentFirstInStream   bool

	commitDuration time.Duration
	searchDuration time.Duration
}

type MeasurementCtxKey struct{}

func NewMeasurement(serviceName string, resourceName string, spanType string, tags map[string]string) *Measurement {
	return &Measurement{serviceName: serviceName, resourceName: resourceName, spanType: spanType, tags: tags}
}

func MeasurementFromContext(ctx context.Context) (*Measurement, bool) {
	s, ok := ctx.Value(MeasurementCtxKey{}).(*Measurement)
	return s, ok
}

func ClearMeasurementContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, MeasurementCtxKey{}, nil)
}

func (m *Measurement) CountOkForScope(scope tally.Scope, tags map[string]string) {
	if scope != nil {
		m.countOk(scope, tags)
	}
}

func (m *Measurement) SetError() {
	m.hasError = true
}

func (m *Measurement) IsFirstDocSent() bool {
	return m.sentFirstInStream
}

func (m *Measurement) MarkFirstDocSent() {
	m.firstDocumentSentAt = time.Now()
	m.sentFirstInStream = true
}

func (m *Measurement) countOk(scope tally.Scope, tags map[string]string) {
	m.IncrementCount(scope, tags, "ok", 1)
}

func (m *Measurement) GetNamespaceName() string {
	if namespaceName, found := m.tags["tigris_tenant_name"]; found {
		return namespaceName
	}
	return defaults.UnknownValue
}

func (*Measurement) IncrementCount(scope tally.Scope, tags map[string]string, counterName string, count int64) {
	scope.Tagged(tags).Counter(counterName).Inc(count)
}

func (m *Measurement) SetNDocs(value int64) {
	m.nDocs += value
}

func (m *Measurement) AddSentBytes(value int) {
	m.sentBytes += value
}

func (m *Measurement) AddReceivedBytes(value int) {
	m.receivedBytes += value
}

func (m *Measurement) GetCommitDuration() time.Duration {
	return m.commitDuration
}

func (m *Measurement) GetSearchIndexDuration() time.Duration {
	return m.searchDuration
}

func (m *Measurement) SetCommitDuration(dur time.Duration) {
	m.commitDuration = dur
}

func (m *Measurement) SetSearchIndexDuration(dur time.Duration) {
	m.searchDuration = dur
}

func (*Measurement) CountUnits(reqStatus *RequestStatus, tags map[string]string) {
	if !config.DefaultConfig.GlobalStatus.EmitMetrics {
		return
	}
	readBytes := reqStatus.GetReadBytes()
	writeBytes := reqStatus.GetWriteBytes()
	RequestsReadBytes.Tagged(tags).Counter("bytes").Inc(readBytes)
	RequestsWriteBytes.Tagged(tags).Counter("bytes").Inc(writeBytes)
	RequestsSearchWriteBytes.Tagged(tags).Counter("bytes").Inc(reqStatus.searchWriteBytes)
	RequestsReadUnits.Tagged(tags).Counter("units").Inc(getUnitsFromBytes(readBytes, config.ReadUnitSize))
	RequestsWriteUnits.Tagged(tags).Counter("units").Inc(getUnitsFromBytes(writeBytes, config.WriteUnitSize))
	RequestsDDLDropUnits.Tagged(tags).Counter("units").Inc(reqStatus.GetDDLDropUnits())
	RequestsDDLUpdateUnits.Tagged(tags).Counter("units").Inc(reqStatus.GetDDLUpdateUnits())
	RequestsDDLCreateUnits.Tagged(tags).Counter("units").Inc(reqStatus.GetDDLCreateUnits())
	RequestsCollectionSearchUnits.Tagged(tags).Counter("units").Inc(reqStatus.GetCollectionSearchUnits())
	RequestsApiSearchUnits.Tagged(tags).Counter("units").Inc(reqStatus.GetApiSearchUnits())
	RequestsSearchWriteUnits.Tagged(tags).Counter("units").Inc(getUnitsFromBytes(reqStatus.searchWriteBytes, config.SearchUnitSize))
}

func (m *Measurement) CountErrorForScope(scope tally.Scope, tags map[string]string) {
	if scope != nil {
		m.countError(scope, tags)
	}
}

func (*Measurement) countError(scope tally.Scope, tags map[string]string) {
	scope.Tagged(tags).Counter("error").Inc(1)
}

func (m *Measurement) AddProjectBranchCollTags(project string, branch string, coll string) {
	// For stream requests we will add the tags once based on the flag rather than for every result document
	m.projectCollTags = GetProjectBranchCollTags(project, branch, coll)
	m.RecursiveAddTags(m.projectCollTags)
}

func (m *Measurement) GetProjectCollTags() map[string]string {
	return m.projectCollTags
}

func (m *Measurement) GetServiceName() string {
	return m.serviceName
}

func (m *Measurement) GetResourceName() string {
	return m.resourceName
}

func (m *Measurement) GetTags() map[string]string {
	return m.tags
}

func (m *Measurement) GetRequestOkTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getRequestOkTagKeys()), config.DefaultConfig.Metrics.Requests.FilteredTags)
}

func (m *Measurement) GetGlobalStatusTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getGlobalStatusTagKeys()), config.DefaultConfig.Metrics.Requests.FilteredTags)
}

func (m *Measurement) GetRequestErrorTags(err error) map[string]string {
	return filterTags(standardizeTags(mergeTags(m.tags, getTagsForError(err)), getRequestErrorTagKeys()), config.DefaultConfig.Metrics.Requests.FilteredTags)
}

func (m *Measurement) GetFdbOkTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getFdbOkTagKeys()), config.DefaultConfig.Metrics.Fdb.FilteredTags)
}

func (m *Measurement) GetFdbErrorTags(err error) map[string]string {
	return filterTags(standardizeTags(mergeTags(m.tags, getTagsForError(err)), getFdbErrorTagKeys()), config.DefaultConfig.Metrics.Fdb.FilteredTags)
}

func (m *Measurement) GetMetronomeTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getMetronomeTagKeys()), config.DefaultConfig.Metrics.Metronome.FilteredTags)
}

func (m *Measurement) GetSearchOkTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getSearchOkTagKeys()), config.DefaultConfig.Metrics.Search.FilteredTags)
}

func (m *Measurement) GetSearchErrorTags(err error) map[string]string {
	return filterTags(standardizeTags(mergeTags(m.tags, getTagsForError(err)), getSearchErrorTagKeys()), config.DefaultConfig.Metrics.Search.FilteredTags)
}

func (m *Measurement) GetSecondaryIndexOkTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getSecondaryIndexOkTagKeys()), config.DefaultConfig.Metrics.SecondaryIndex.FilteredTags)
}

func (m *Measurement) GetSecondaryIndexErrorTags(err error) map[string]string {
	return filterTags(standardizeTags(mergeTags(m.tags, getTagsForError(err)), getSecondaryIndexErrorTagKeys()), config.DefaultConfig.Metrics.SecondaryIndex.FilteredTags)
}

func (m *Measurement) GetSessionOkTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getSessionOkTagKeys()), config.DefaultConfig.Metrics.Session.FilteredTags)
}

func (m *Measurement) GetSessionErrorTags(err error) map[string]string {
	return filterTags(standardizeTags(mergeTags(m.tags, getTagsForError(err)), getSessionErrorTagKeys()), config.DefaultConfig.Metrics.Session.FilteredTags)
}

func (m *Measurement) GetNamespaceSizeTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getNameSpaceSizeTagKeys()), config.DefaultConfig.Metrics.Size.FilteredTags)
}

func (m *Measurement) GetDbSizeTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getDbSizeTagKeys()), config.DefaultConfig.Metrics.Size.FilteredTags)
}

func (m *Measurement) GetCollectionSizeTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getCollectionSizeTagKeys()), config.DefaultConfig.Metrics.Size.FilteredTags)
}

func (m *Measurement) GetSearchSizeTagKeys() map[string]string {
	return filterTags(standardizeTags(m.tags, getSearchSizeTagKeys()), config.DefaultConfig.Metrics.Size.FilteredTags)
}

func (m *Measurement) GetSearchIndexSizeTagKeys() map[string]string {
	return filterTags(standardizeTags(m.tags, getSearchIndexSizeTagKeys()), config.DefaultConfig.Metrics.Size.FilteredTags)
}

func (m *Measurement) GetNetworkTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getNetworkTagKeys()), config.DefaultConfig.Metrics.Network.FilteredTags)
}

func (m *Measurement) GetAuthOkTags() map[string]string {
	return filterTags(standardizeTags(m.tags, getAuthOkTagKeys()), config.DefaultConfig.Metrics.Auth.FilteredTags)
}

func (m *Measurement) GetAuthErrorTags(err error) map[string]string {
	return filterTags(standardizeTags(mergeTags(m.tags, getTagsForError(err)), getAuthErrorTagKeys()), config.DefaultConfig.Metrics.Auth.FilteredTags)
}

func (m *Measurement) TimeSinceStart() time.Duration {
	return time.Since(m.startedAt)
}

func (m *Measurement) SaveMeasurementToContext(ctx context.Context) (context.Context, error) {
	if m.datadogSpan == nil && m.jaegerSpan == nil {
		return nil, fmt.Errorf("parent span was not created")
	}
	ctx = context.WithValue(ctx, MeasurementCtxKey{}, m)
	return ctx, nil
}

func (m *Measurement) GetSpanOptions() []ddtracer.StartSpanOption {
	return []ddtracer.StartSpanOption{
		ddtracer.ServiceName(m.serviceName),
		ddtracer.ResourceName(m.resourceName),
		ddtracer.SpanType(m.spanType),
		ddtracer.Measured(),
	}
}

func (m *Measurement) AddTags(tags map[string]string) {
	for k, v := range tags {
		if _, exists := m.tags[k]; !exists || m.tags[k] == defaults.UnknownValue || m.tags[k] == "" {
			m.tags[k] = v
			if m.datadogSpan != nil {
				// The span already exists, set the tag there as well
				m.datadogSpan.SetTag(k, v)
			}
		}
	}
}

func (m *Measurement) RecursiveAddTags(tags map[string]string) {
	m.AddTags(tags)
	if m.parent != nil {
		m.parent.RecursiveAddTags(tags)
	}
}

func (m *Measurement) StartTracing(ctx context.Context, childOnly bool) context.Context {
	m.startedAt = time.Now()
	m.started = true

	log.Trace().Str("started", strconv.FormatBool(m.started)).Str("stopped", strconv.FormatBool(m.stopped)).Str("childonly", strconv.FormatBool(childOnly)).Str("span_type", m.spanType).Msg("StartTracing start")
	if !tracing.IsTracingEnabled(&config.DefaultConfig) && !config.DefaultConfig.Metrics.Enabled {
		log.Trace().Str("span_type", m.spanType).Msg("StartTracing end: Neither tracing, nor metrics are enabled, returning")
		return ctx
	}

	spanOpts := m.GetSpanOptions()
	if parentMeasurement, parentExists := MeasurementFromContext(ctx); parentExists {
		// This is a child span, parents need to be marked
		spanOpts = append(spanOpts, ddtracer.ChildOf(parentMeasurement.datadogSpan.Context()))
		m.parent = parentMeasurement
		// Copy the tags from the parent span
		m.AddTags(parentMeasurement.GetTags())
	} else if childOnly {
		// There is no parent span, no need to start tracing here
		log.Trace().Msg("No parent exists and childonly is set, not tracing")
		return ctx
	}

	m.datadogSpan = ddtracer.StartSpan(TraceServiceName, spanOpts...)
	for k, v := range m.tags {
		m.datadogSpan.SetTag(k, v)
	}
	//}

	if tracing.IsJaegerTracingEnabled(&config.DefaultConfig) {
		var tags []attribute.KeyValue
		for k, v := range m.tags {
			tags = append(tags, attribute.KeyValue{Key: attribute.Key(k), Value: attribute.StringValue(v)})
		}
		ctx, m.jaegerSpan = tracing.OpenTracer.Start(ctx, m.resourceName, opentrace.WithAttributes(tags...))
	}

	ctx, err := m.SaveMeasurementToContext(ctx)
	ulog.E(err)

	log.Trace().Str("started", strconv.FormatBool(m.started)).Str("stopped", strconv.FormatBool(m.stopped)).Str("span_type", m.spanType).Msg("StartTracing end")
	return ctx
}

func (m *Measurement) FinishTracing(ctx context.Context) context.Context {
	if !m.started {
		log.Error().
			Str("service_name", m.serviceName).
			Str("resource_name", m.resourceName).
			Msg("Finish tracing called before starting the trace")
		return ctx
	}

	m.stopped = true
	m.stoppedAt = time.Now()

	log.Trace().Str("started", strconv.FormatBool(m.started)).
		Str("stopped", strconv.FormatBool(m.stopped)).
		Str("span_type", m.spanType).
		Msg("FinishingTracing start")

	if m.datadogSpan != nil {
		m.datadogSpan.Finish()
	}

	if m.jaegerSpan != nil {
		m.jaegerSpan.End()
	}

	if m.parent != nil {
		var err error
		ctx, err = m.parent.SaveMeasurementToContext(ctx)
		ulog.E(err)
	} else {
		// This was the top level span meta
		ctx = ClearMeasurementContext(ctx)
	}

	log.Trace().Str("started", strconv.FormatBool(m.started)).Str("span_type", m.spanType).Str("stopped", strconv.FormatBool(m.stopped)).Msg("FinishingTracing end")
	return ctx
}

func (m *Measurement) RecordDuration(scope tally.Scope, tags map[string]string) {
	var timerEnabled, histogramEnabled bool
	cfg := config.DefaultConfig.Metrics
	switch scope {
	case AuthRespTime, AuthErrorRespTime:
		timerEnabled = config.DefaultConfig.Metrics.Auth.Enabled
	case RequestsRespTime, RequestsErrorRespTime:
		timerEnabled = cfg.Requests.Timer.TimerEnabled
		histogramEnabled = cfg.Requests.Timer.HistogramEnabled
		m.logLongRequest()
	case FdbRespTime, FdbErrorRespTime:
		timerEnabled = cfg.Fdb.Timer.TimerEnabled
		histogramEnabled = cfg.Fdb.Timer.HistogramEnabled
	case SessionRespTime, SessionErrorRespTime:
		timerEnabled = cfg.Session.Timer.TimerEnabled
		histogramEnabled = cfg.Session.Timer.HistogramEnabled
	case SearchRespTime, SearchErrorRespTime:
		timerEnabled = cfg.Search.Timer.TimerEnabled
		histogramEnabled = cfg.Search.Timer.HistogramEnabled
	case SecondaryIndexRespTime, SecondaryIndexErrorRespTime:
		timerEnabled = cfg.SecondaryIndex.Timer.TimerEnabled
		histogramEnabled = cfg.SecondaryIndex.Timer.HistogramEnabled
	case MetronomeResponseTime, MetronomeErrorResponseTime:
		timerEnabled = cfg.Metronome.Timer.TimerEnabled
		histogramEnabled = cfg.Metronome.Timer.HistogramEnabled
	case RequestsRespTimeToFirstDoc:
		// Response time to first document
		m.RecordFirstDocumentDuration(scope, tags)
	}
	if scope != nil && timerEnabled {
		m.recordTimerDuration(scope, tags)
	}
	if scope != nil && histogramEnabled {
		m.recordHistogramDuration(scope, tags)
	}
}

func (m *Measurement) RecordFirstDocumentDuration(scope tally.Scope, tags map[string]string) {
	// Records the duration when the first document was sent to the client. This does not need a stopped
	// tracing scope.
	if !m.started {
		log.Error().
			Str("service_name", m.serviceName).
			Str("resource_name", m.resourceName).
			Str("span_type", m.spanType).
			Msg("recordTimerDuration was called on a span that was not started")
		return
	}
	if !m.sentFirstInStream {
		log.Error().
			Str("service_name", m.serviceName).
			Str("resource_name", m.resourceName).
			Str("span_type", m.spanType).
			Msg("recordTimerDuration was called on a span that was not stopped")
		return
	}
	scope.Tagged(tags).Timer("time").Record(m.firstDocumentSentAt.Sub(m.startedAt))
}

func (m *Measurement) getTag(name string) string {
	if value, ok := m.tags[name]; ok {
		return value
	}
	return "unknown"
}

func (m *Measurement) logLongRequest() {
	totalTime := m.stoppedAt.Sub(m.startedAt)
	grpcMethod := m.getTag("grpc_method")
	if slices.Contains(config.DefaultConfig.Metrics.LongRequestConfig.FilteredMethods, grpcMethod) {
		// No need to log the filtered method, these are known to be long
		return
	}
	if totalTime < config.DefaultConfig.Metrics.LogLongMethodTime {
		return
	}
	log.Error().
		Int64("total time (ms)", int64(totalTime/time.Millisecond)).
		Time("start time", m.startedAt).Time("stop time", m.stoppedAt).
		Str("grpc method", m.getTag(grpcMethod)).
		Int64("threshold", int64(config.DefaultConfig.Metrics.LogLongMethodTime/time.Millisecond)).
		Int64("result documents", m.nDocs).Str("tenant name", m.GetNamespaceName()).
		Int("bytes sent", m.sentBytes).
		Int("bytes received", m.receivedBytes).
		Bool("has error", m.hasError).
		Msg("long method call")
}

func (m *Measurement) recordTimerDuration(scope tally.Scope, tags map[string]string) {
	// Should be called after tracing is finished
	if !m.started {
		log.Error().
			Str("service_name", m.serviceName).
			Str("resource_name", m.resourceName).
			Str("span_type", m.spanType).
			Msg("recordTimerDuration was called on a span that was not started")
		return
	}
	if !m.stopped {
		log.Error().
			Str("service_name", m.serviceName).
			Str("resource_name", m.resourceName).
			Str("span_type", m.spanType).
			Msg("recordTimerDuration was called on a span that was not stopped")
		return
	}
	scope.Tagged(tags).Timer("time").Record(m.stoppedAt.Sub(m.startedAt))
}

func (m *Measurement) recordHistogramDuration(scope tally.Scope, tags map[string]string) {
	if !m.started {
		log.Error().
			Str("service_name", m.serviceName).
			Str("resource_name", m.resourceName).
			Str("span_type", m.spanType).
			Msg("recordHistogramDuration was called on a span that was not started")
		return
	}
	if !m.stopped {
		log.Error().
			Str("service_name", m.serviceName).
			Str("resource_name", m.resourceName).
			Str("span_type", m.spanType).
			Msg("recordHistogramDuration was called on a span that was not stopped")
		return
	}
	scope.Tagged(tags).Histogram("histogram", tally.DefaultBuckets).RecordDuration(m.stoppedAt.Sub(m.startedAt))
}

func (m *Measurement) FinishWithError(ctx context.Context, err error) context.Context {
	if !m.started {
		log.Error().
			Str("service_name", m.serviceName).
			Str("resource_name", m.resourceName).
			Msg("Finish tracing called before starting the trace")
		return ctx
	}

	m.stopped = true
	m.stoppedAt = time.Now()

	if m.datadogSpan == nil && m.jaegerSpan == nil {
		log.Trace().Msg("FinishWithError end: no tracing span sound to finish, returning")
		return ctx
	}
	errCode := status.Code(err)
	m.datadogSpan.SetTag("grpc.code", errCode.String())
	errTags := getTagsForError(err)
	for k, v := range errTags {
		m.datadogSpan.SetTag(k, v)
	}
	finishOptions := []ddtracer.FinishOption{ddtracer.WithError(err)}

	if m.datadogSpan != nil {
		m.datadogSpan.Finish(finishOptions...)
	}

	if m.parent != nil {
		var err error
		ctx, err = m.parent.SaveMeasurementToContext(ctx)
		ulog.E(err)
	} else {
		// This was the top level span meta
		ctx = ClearMeasurementContext(ctx)
	}

	log.Trace().Str("started", strconv.FormatBool(m.started)).
		Str("span_type", m.spanType).
		Str("stopped", strconv.FormatBool(m.stopped)).
		Msg("FinishWithError end")
	return ctx
}
