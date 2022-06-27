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
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
)

func increaseCounter(fullMethod string, methodType string, counterName string, value int64) {
	tags := metrics.GetPreinitializedTagsFromFullMethod(fullMethod, methodType)
	metrics.Requests.Tagged(tags).Counter(counterName).Inc(value)
}

func countReceivedMessage(fullMethod string, methodType string) {
	// Tags are pre-created when initializing the metrics
	increaseCounter(fullMethod, methodType, "received", 1)
}

func countHandledMessage(fullMethod string, methodType string) {
	// Tags are pre-created when initializing the metrics
	increaseCounter(fullMethod, methodType, "handled", 1)
}

func countOkMessage(fullMethod string, methodType string) {
	// Tags are pre-created when initializing the metrics
	increaseCounter(fullMethod, methodType, "ok", 1)
}

func countUnknownErrorMessage(fullMethod string, methodType string) {
	// tigris_server_requests_error is s scope, so different scope needs to be used here
	tags := metrics.GetPreinitializedTagsFromFullMethod(fullMethod, methodType)
	metrics.ErrorRequests.Tagged(tags).Counter("unknown").Inc(1)
}

func countSpecificErrorMessage(fullMethod string, methodType, errSource string, errCode string) {
	// For specific errors the tags are not pre-initialized because it has the error code in it
	metaData := metrics.GetGrpcEndPointMetadataFromFullMethod(fullMethod, methodType)
	tags := metaData.GetSpecificErrorTags(errSource, errCode)
	metrics.ErrorRequests.Tagged(tags).Counter("specific").Inc(1)
}

func UnaryMetricsServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		methodType := "unary"
		tags := metrics.GetPreinitializedTagsFromFullMethod(info.FullMethod, methodType)
		defer metrics.RequestsRespTime.Tagged(tags).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
		resp, err := handler(ctx, req)
		countReceivedMessage(info.FullMethod, methodType)
		countHandledMessage(info.FullMethod, methodType)
		if err != nil {
			var terr *api.TigrisError
			var ferr fdb.Error
			if errors.As(err, &terr) {
				countSpecificErrorMessage(info.FullMethod, methodType, "tigris_server", terr.Code.String())
			} else if errors.As(err, &ferr) {
				countSpecificErrorMessage(info.FullMethod, methodType, "fdb_server", strconv.Itoa(ferr.Code))
			} else {
				countUnknownErrorMessage(info.FullMethod, methodType)
			}
		} else {
			countOkMessage(info.FullMethod, methodType)
		}
		return resp, err
	}
}
func StreamMetricsServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		methodType := "stream"
		tags := metrics.GetPreinitializedTagsFromFullMethod(info.FullMethod, methodType)
		defer metrics.RequestsRespTime.Tagged(tags).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
		countReceivedMessage(info.FullMethod, methodType)
		wrapper := &recvWrapper{stream}
		countHandledMessage(info.FullMethod, methodType)
		err := handler(srv, wrapper)
		if err != nil {
			var terr *api.TigrisError
			var ferr fdb.Error
			if errors.As(err, &terr) {
				countSpecificErrorMessage(info.FullMethod, methodType, "tigris_server", terr.Code.String())
			} else if errors.As(err, &ferr) {
				countSpecificErrorMessage(info.FullMethod, methodType, "fdb_server", strconv.Itoa(ferr.Code))
			} else {
				countUnknownErrorMessage(info.FullMethod, methodType)
			}
		} else {
			countOkMessage(info.FullMethod, methodType)
		}
		return err
	}
}
