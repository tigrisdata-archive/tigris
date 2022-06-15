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
	"errors"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/metrics"
	"google.golang.org/grpc"
)

func receiveMessage(fullMethod string) {
	metrics.GetServerRequestCounter(fullMethod, metrics.ServerRequestsReceivedTotal).Counter.Inc(1)
}

func handleMessage(fullMethod string) {
	metrics.GetServerRequestCounter(fullMethod, metrics.ServerRequestsHandledTotal).Counter.Inc(1)
}

func errorMessage(fullMethod string) {
	metrics.GetServerRequestCounter(fullMethod, metrics.ServerRequestsUnknownErrorTotal).Counter.Inc(1)
}

func specificErrorMessage(fullMethod string, methodType, errSource string, errCode string) {
	var counter *metrics.ServerRequestCounter
	if _, ok := metrics.ServerRequestErrorCodeCounters[fullMethod]; !ok {
		metrics.ServerRequestErrorCodeCounters[fullMethod] = make(map[string]map[string]*metrics.ServerRequestCounter)
	}
	if _, ok := metrics.ServerRequestErrorCodeCounters[fullMethod][errSource]; !ok {
		metrics.ServerRequestErrorCodeCounters[fullMethod][errSource] = make(map[string]*metrics.ServerRequestCounter)
	}
	if _, ok := metrics.ServerRequestErrorCodeCounters[fullMethod][errSource][errCode]; !ok {
		counter = metrics.GetSpecificErrorCounter(fullMethod, methodType, errSource, errCode)
		metrics.ServerRequestErrorCodeCounters[fullMethod][errSource][errCode] = counter
	} else {
		counter = metrics.ServerRequestErrorCodeCounters[fullMethod][errSource][errCode]
	}
	counter.Counter.Inc(1)
}

func okMessage(fullMethod string) {
	metrics.GetServerRequestCounter(fullMethod, metrics.ServerRequestsOkTotal).Counter.Inc(1)
}

func getTimeHistogram(fullMethod string) *metrics.ServerRequestHistogram {
	return metrics.GetServerRequestHistogram(fullMethod, metrics.ServerRequestsHandlingTimeBucket)
}

func UnaryMetricsServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		defer getTimeHistogram(info.FullMethod).Histogram.Start().Stop()
		receiveMessage(info.FullMethod)
		resp, err := handler(ctx, req)
		handleMessage(info.FullMethod)
		if err != nil {
			var terr *api.TigrisError
			var ferr fdb.Error
			if errors.As(err, &terr) {
				specificErrorMessage(info.FullMethod, "unary", "tigris", terr.Code.String())
			} else if errors.As(err, &ferr) {
				specificErrorMessage(info.FullMethod, "unary", "fdb", strconv.Itoa(ferr.Code))
			} else {
				errorMessage(info.FullMethod)
			}
		} else {
			okMessage(info.FullMethod)
		}
		return resp, err
	}
}
func StreamMetricsServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		defer getTimeHistogram(info.FullMethod).Histogram.Start().Stop()
		receiveMessage(info.FullMethod)
		wrapper := &recvWrapper{stream}
		handleMessage(info.FullMethod)
		err := handler(srv, wrapper)
		if err != nil {
			var terr *api.TigrisError
			var ferr fdb.Error
			if errors.As(err, &terr) {
				specificErrorMessage(info.FullMethod, "stream", "tigris", terr.Code.String())
			} else if errors.As(err, &ferr) {
				specificErrorMessage(info.FullMethod, "stream", "fdb", strconv.Itoa(ferr.Code))
			} else {
				errorMessage(info.FullMethod)
			}
		} else {
			okMessage(info.FullMethod)
		}
		return err
	}
}
