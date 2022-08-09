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
	"strings"

	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
)

const (
	AdminServiceName       = "tigrisdata.admin.v1.Admin"
	SystemTigrisTenantName = "system"
	UnknownValue           = "unknown"
)

type RequestEndpointMetadata struct {
	serviceName   string
	methodInfo    grpc.MethodInfo
	namespaceName string
}

func getNamespaceName(ctx context.Context) string {
	if namespace, _ := request.GetNamespace(ctx); namespace != "" {
		return namespace
	}
	return UnknownValue
}

func newRequestEndpointMetadata(ctx context.Context, serviceName string, methodInfo grpc.MethodInfo) RequestEndpointMetadata {
	return RequestEndpointMetadata{serviceName: serviceName, methodInfo: methodInfo, namespaceName: getNamespaceName(ctx)}
}

func (r *RequestEndpointMetadata) GetMethodName() string {
	return strings.Split(r.methodInfo.Name, "/")[2]
}

func (r *RequestEndpointMetadata) GetServiceType() string {
	if r.methodInfo.IsServerStream {
		return "stream"
	} else {
		return "unary"
	}
}

func (r *RequestEndpointMetadata) GetTags() map[string]string {
	if r.serviceName == AdminServiceName {
		return map[string]string{
			"grpc_method":       r.methodInfo.Name,
			"grpc_service":      r.serviceName,
			"tigris_tenant":     r.namespaceName,
			"grpc_service_type": r.GetServiceType(),
			"db":                UnknownValue,
			"collection":        UnknownValue,
		}
	} else {
		return map[string]string{
			"grpc_method":       r.methodInfo.Name,
			"grpc_service":      r.serviceName,
			"tigris_tenant":     r.namespaceName,
			"grpc_service_type": r.GetServiceType(),
			"db":                UnknownValue,
			"collection":        UnknownValue,
		}
	}
}

func (r *RequestEndpointMetadata) GetSpecificErrorTags(source string, code string) map[string]string {
	if r.serviceName == AdminServiceName {
		return map[string]string{
			"grpc_method":   r.methodInfo.Name,
			"grpc_service":  r.serviceName,
			"error_source":  source,
			"error_code":    code,
			"tigris_tenant": SystemTigrisTenantName,
			"db":            UnknownValue,
			"collection":    UnknownValue,
		}
	} else {
		return map[string]string{
			"grpc_method":   r.methodInfo.Name,
			"grpc_service":  r.serviceName,
			"error_source":  source,
			"error_code":    code,
			"tigris_tenant": UnknownValue,
			"db":            UnknownValue,
		}
	}
}

func (r *RequestEndpointMetadata) getFullMethod() string {
	return fmt.Sprintf("/%s/%s", r.serviceName, r.methodInfo.Name)
}

func GetGrpcEndPointMetadataFromFullMethod(ctx context.Context, fullMethod string, methodType string) RequestEndpointMetadata {
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
	return newRequestEndpointMetadata(ctx, svcName, methodInfo)
}

func InitializeRequestScopes() {
	// metric names: tigris_server_requests
	if config.DefaultConfig.Tracing.Enabled {
		OkRequests = Requests.SubScope("requests")
		// metric names: tigris_server_requests_errors
		ErrorRequests = Requests.SubScope("error")
		// metric names: tigirs_server_requests_resptime
		RequestsRespTime = Requests.SubScope("resptime")
	}
}
