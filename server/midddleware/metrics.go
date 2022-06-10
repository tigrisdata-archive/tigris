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
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
	"strings"
)

type grpcReqMetrics struct {
	grpcMethod string
	grpcType   string
}

func newGrpcReqMetrics(grpcMethod string, grpcType string) grpcReqMetrics {
	return grpcReqMetrics{grpcMethod: grpcMethod, grpcType: grpcType}
}

func (g *grpcReqMetrics) getTags() map[string]string {
	fullMethodParts := strings.Split(g.grpcMethod, "/")
	return map[string]string{
		"grpc_method":  fullMethodParts[2],
		"grpc_service": fullMethodParts[1],
		"grpc_type":    g.grpcType,
	}
}

func (g *grpcReqMetrics) getGrpcCounter(name string) tally.Counter {
	tags := g.getTags()
	return metrics.Root.Tagged(tags).Counter(name)
}

func (g *grpcReqMetrics) increaseGrpcCounter(counter tally.Counter, value int64) {
	counter.Inc(value)
}

func (g *grpcReqMetrics) receiveMessage() {
	counter := g.getGrpcCounter("grpc_server_msg_received_total")
	g.increaseGrpcCounter(counter, 1)
}

func (g *grpcReqMetrics) handleMessage() {
	counter := g.getGrpcCounter("grpc_server_msg_handled_total")
	g.increaseGrpcCounter(counter, 1)
}

func (g *grpcReqMetrics) errorMessage() {
	counter := g.getGrpcCounter("grpc_server_msg_error_total")
	g.increaseGrpcCounter(counter, 1)
}

func (g *grpcReqMetrics) okMessage() {
	counter := g.getGrpcCounter("grpc_server_msg_ok_total")
	g.increaseGrpcCounter(counter, 1)
}

func (g *grpcReqMetrics) getTimeHistogram() tally.Histogram {
	tags := g.getTags()
	return metrics.Root.Tagged(tags).Histogram("grpc_server_handling_time_bucket", tally.DefaultBuckets)
}

func UnaryMetricsServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		reqMetrics := newGrpcReqMetrics(info.FullMethod, "unary")
		defer reqMetrics.getTimeHistogram().Start().Stop()
		reqMetrics.receiveMessage()
		resp, err := handler(ctx, req)
		reqMetrics.handleMessage()
		if err != nil {
			reqMetrics.errorMessage()
		} else {
			reqMetrics.okMessage()
		}
		return resp, err
	}
}
func StreamMetricsServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		reqMetrics := newGrpcReqMetrics(info.FullMethod, "stream")
		defer reqMetrics.getTimeHistogram().Start().Stop()
		reqMetrics.receiveMessage()
		wrapper := &recvWrapper{stream}
		reqMetrics.handleMessage()
		err := handler(srv, wrapper)
		if err != nil {
			reqMetrics.okMessage()
		} else {
			reqMetrics.errorMessage()
		}
		return err
	}
}
