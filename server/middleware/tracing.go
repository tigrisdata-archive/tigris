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

package middleware

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
)

const (
	TigrisStreamSpan string = "rpcstream"
	TraceSpanType    string = "rpc"
)

type wrappedStream struct {
	*middleware.WrappedServerStream
	spanMeta *metrics.SpanMeta
}

func traceUnary() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		grpcMeta := metrics.GetGrpcEndPointMetadataFromFullMethod(ctx, info.FullMethod, "unary")
		tags := grpcMeta.GetOkTags()
		spanMeta := metrics.NewSpanMeta(util.Service, info.FullMethod, TraceSpanType, tags)
		spanMeta.AddTags(metrics.GetDbCollTagsForReq(req))
		defer metrics.RequestsRespTime.Tagged(spanMeta.GetTags()).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
		ctx = spanMeta.StartTracing(ctx, false)
		resp, err := handler(ctx, req)
		if err != nil {
			// Request had an error
			spanMeta.CountErrorForScope(metrics.OkRequests, err)
			spanMeta.FinishWithError(ctx, err)
			ulog.E(err)
			return nil, err
		}
		// Request was ok
		spanMeta.CountOkForScope(metrics.OkRequests)
		_ = spanMeta.FinishTracing(ctx)
		return resp, err
	}
}

func traceStream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := &wrappedStream{WrappedServerStream: middleware.WrapServerStream(stream)}
		wrapped.WrappedContext = stream.Context()
		grpcMeta := metrics.GetGrpcEndPointMetadataFromFullMethod(wrapped.WrappedContext, info.FullMethod, "stream")
		tags := grpcMeta.GetOkTags()
		spanMeta := metrics.NewSpanMeta(util.Service, info.FullMethod, TraceSpanType, tags)
		defer metrics.RequestsRespTime.Tagged(spanMeta.GetTags()).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
		wrapped.spanMeta = spanMeta
		wrapped.WrappedContext = spanMeta.StartTracing(wrapped.WrappedContext, false)
		err := handler(srv, wrapped)
		if err != nil {
			spanMeta.CountErrorForScope(metrics.OkRequests, err)
			spanMeta.FinishWithError(wrapped.WrappedContext, err)
			ulog.E(err)
			return err
		}
		spanMeta.CountOkForScope(metrics.OkRequests)
		wrapped.WrappedContext = spanMeta.FinishTracing(wrapped.WrappedContext)
		return err
	}
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	parentSpanMeta := w.spanMeta
	childSpanMeta := metrics.NewSpanMeta(TigrisStreamSpan, "RecvMsg", TraceSpanType, parentSpanMeta.GetTags())
	w.WrappedContext = childSpanMeta.StartTracing(w.WrappedContext, true)
	err := w.ServerStream.RecvMsg(m)
	parentSpanMeta.AddTags(metrics.GetDbCollTagsForReq(m))
	childSpanMeta.AddTags(metrics.GetDbCollTagsForReq(m))
	w.WrappedContext = childSpanMeta.FinishTracing(w.WrappedContext)
	return err
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	parentSpanMeta := w.spanMeta
	childSpanMeta := metrics.NewSpanMeta(TigrisStreamSpan, "SendMsg", TraceSpanType, parentSpanMeta.GetTags())
	w.WrappedContext = childSpanMeta.StartTracing(w.WrappedContext, true)
	err := w.ServerStream.SendMsg(m)
	parentSpanMeta.AddTags(metrics.GetDbCollTagsForReq(m))
	childSpanMeta.AddTags(metrics.GetDbCollTagsForReq(m))
	w.WrappedContext = childSpanMeta.FinishTracing(w.WrappedContext)
	return err
}
