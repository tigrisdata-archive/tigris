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
	"fmt"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/uber-go/tally"
	"strings"

	"google.golang.org/grpc"
)

type RequestEndpointMetadata struct {
	serviceName string
	methodInfo  grpc.MethodInfo
}

func newRequestEndpointMetadata(serviceName string, methodInfo grpc.MethodInfo) RequestEndpointMetadata {
	return RequestEndpointMetadata{serviceName: serviceName, methodInfo: methodInfo}
}

func (g *RequestEndpointMetadata) GetPreInitializedTags() map[string]string {
	return map[string]string{
		"tigris_server_request_method":       g.methodInfo.Name,
		"tigris_server_request_service_name": g.serviceName,
	}
}

func (g *RequestEndpointMetadata) GetSpecificErrorTags(source string, code string) map[string]string {
	return map[string]string{
		"tigris_server_request_method":       g.methodInfo.Name,
		"tigris_server_request_service_name": g.serviceName,
		"tigris_server_request_error_source": source,
		"tigris_server_request_error_code":   code,
	}
}

func (g *RequestEndpointMetadata) getFullMethod() string {
	return fmt.Sprintf("/%s/%s", g.serviceName, g.methodInfo.Name)
}

func GetGrpcEndPointMetadataFromFullMethod(fullMethod string, methodType string) RequestEndpointMetadata {
	var methodInfo grpc.MethodInfo
	methodList := strings.Split(fullMethod, "/")
	svcName := methodList[1]
	methodName := methodList[2]
	if methodType == "unary" {
		methodInfo = grpc.MethodInfo{
			Name:           methodName,
			IsClientStream: false,
			IsServerStream: false,
		}
	} else if methodType == "stream" {
		methodInfo = grpc.MethodInfo{
			Name:           methodName,
			IsClientStream: false,
			IsServerStream: true,
		}
	}
	return newRequestEndpointMetadata(svcName, methodInfo)
}

func GetPreinitializedTagsFromFullMethod(fullMethod string, methodType string) map[string]string {
	metaData := GetGrpcEndPointMetadataFromFullMethod(fullMethod, methodType)
	return metaData.GetPreInitializedTags()
}

func InitServerRequestMetrics(svcName string, methodInfo grpc.MethodInfo) {
	endPointMetadata := newRequestEndpointMetadata(svcName, methodInfo)
	tags := endPointMetadata.GetPreInitializedTags()

	// Counters with default tags
	if config.DefaultConfig.Metrics.Grpc.Enabled && config.DefaultConfig.Metrics.Grpc.Counters {
		// Counters for ok requests
		Requests.Tagged(tags).Counter("ok")

		// Counters for unknown errors
		ErrorRequests.Tagged(tags).Counter("unknown")
	}

	// Specific error counters can't be initialized here because the tags should contain the error code.
	// They are part of the ErrorRequests subscope with different tags. Those are initialized after the first
	// occurrence of the specific error.

	// Response time
	if config.DefaultConfig.Metrics.Grpc.Enabled && config.DefaultConfig.Metrics.Grpc.ResponseTime {
		RequestsRespTime.Tagged(tags).Histogram("histogram", tally.DefaultBuckets)
	}
}

func InitRequestMetricsForServer(s *grpc.Server) {
	for svcName, info := range s.GetServiceInfo() {
		for _, method := range info.Methods {
			InitServerRequestMetrics(svcName, method)
		}
	}
}

func InitializeRequestScopes() {
	server = root.SubScope("server")
	// metric names: tigris_server_requests
	if config.DefaultConfig.Metrics.Grpc.Enabled && config.DefaultConfig.Metrics.Grpc.Counters {
		Requests = server.SubScope("requests")
		// metric names: tigris_server_requests_errors
		ErrorRequests = Requests.SubScope("error")
	}
	if config.DefaultConfig.Metrics.Grpc.Enabled && config.DefaultConfig.Metrics.Grpc.ResponseTime {
		// metric names: tigirs_server_requests_resptime
		RequestsRespTime = server.SubScope("resptime")
	}
}
