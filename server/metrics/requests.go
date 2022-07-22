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
	"strings"

	"github.com/tigrisdata/tigris/server/config"
	"google.golang.org/grpc"
)

const (
	AdminServiceName       = "tigrisdata.admin.v1.Admin"
	SystemTigrisTenantName = "system"
)

type RequestEndpointMetadata struct {
	serviceName string
	methodInfo  grpc.MethodInfo
}

func newRequestEndpointMetadata(serviceName string, methodInfo grpc.MethodInfo) RequestEndpointMetadata {
	return RequestEndpointMetadata{serviceName: serviceName, methodInfo: methodInfo}
}

func (g *RequestEndpointMetadata) GetPreInitializedTags() map[string]string {
	if g.serviceName == AdminServiceName {
		return map[string]string{
			"method":        g.methodInfo.Name,
			"grpc_service":  g.serviceName,
			"tigris_tenant": SystemTigrisTenantName,
		}
	} else {
		return map[string]string{
			"method":        g.methodInfo.Name,
			"grpc_service":  g.serviceName,
			"tigris_tenant": DefaultReportedTigrisTenant,
		}
	}
}

func (g *RequestEndpointMetadata) GetSpecificErrorTags(source string, code string) map[string]string {
	if g.serviceName == AdminServiceName {
		return map[string]string{
			"method":        g.methodInfo.Name,
			"grpc_service":  g.serviceName,
			"source":        source,
			"code":          code,
			"tigris_tenant": SystemTigrisTenantName,
		}
	} else {
		return map[string]string{
			"method":        g.methodInfo.Name,
			"grpc_service":  g.serviceName,
			"source":        source,
			"code":          code,
			"tigris_tenant": DefaultReportedTigrisTenant,
		}
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
