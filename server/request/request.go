// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package request

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/server/config"
	"google.golang.org/grpc"
)

const (
	// DefaultNamespaceName is for "default" namespace in the cluster which means all the databases created are under a single
	// namespace.
	// It is totally fine for a deployment to choose this and just have one namespace. The default assigned value for
	// this namespace is 1.
	DefaultNamespaceName string = "default_namespace"

	DefaultNamespaceId = uint32(1)

	UnknownValue = "unknown"
)

var (
	adminMethods = container.NewHashSet("/tigrisdata.management.v1.Management/createNamespace", "/tigrisdata.management.v1.Management/listNamespaces")
)

type RequestMetadataCtxKey struct {
}

type AccessToken struct {
	Namespace string
	Sub       string
}

type RequestMetadata struct {
	accessToken *AccessToken
	serviceName string
	methodInfo  grpc.MethodInfo
	// this is authenticated namespace
	namespace string
	// this is parsed and set early in request processing chain - before it is authenticated for observability.
	unauthenticatedNamespaceName string
}

func NewRequestEndpointMetadata(ctx context.Context, serviceName string, methodInfo grpc.MethodInfo) RequestMetadata {
	return RequestMetadata{serviceName: serviceName, methodInfo: methodInfo, unauthenticatedNamespaceName: GetNameSpaceFromHeader(ctx)}
}

func GetGrpcEndPointMetadataFromFullMethod(ctx context.Context, fullMethod string, methodType string) RequestMetadata {
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
	return NewRequestEndpointMetadata(ctx, svcName, methodInfo)
}

func (r *RequestMetadata) GetMethodName() string {
	return strings.Split(r.methodInfo.Name, "/")[2]
}

func (r *RequestMetadata) GetServiceType() string {
	if r.methodInfo.IsServerStream {
		return "stream"
	} else {
		return "unary"
	}
}

func (r *RequestMetadata) GetServiceName() string {
	return r.serviceName
}

func (r *RequestMetadata) GetUnAuthenticatedNamespaceName() string {
	return r.unauthenticatedNamespaceName
}

func (r *RequestMetadata) GetMethodInfo() grpc.MethodInfo {
	return r.methodInfo
}

func (r *RequestMetadata) GetInitialTags() map[string]string {
	return map[string]string{
		"grpc_method":       r.methodInfo.Name,
		"grpc_service":      r.serviceName,
		"tigris_tenant":     r.unauthenticatedNamespaceName,
		"grpc_service_type": r.GetServiceType(),
		"env":               config.GetEnvironment(),
		"db":                UnknownValue,
		"collection":        UnknownValue,
	}
}

func (r *RequestMetadata) GetFullMethod() string {
	return fmt.Sprintf("/%s/%s", r.serviceName, r.methodInfo.Name)
}

// NamespaceExtractor - extract the namespace from context
type NamespaceExtractor interface {
	Extract(ctx context.Context) (string, error)
}

type AccessTokenNamespaceExtractor struct {
}

var ErrNamespaceNotFound = errors.NotFound("namespace not found")

func GetRequestMetadata(ctx context.Context) (*RequestMetadata, error) {
	// read token
	value := ctx.Value(RequestMetadataCtxKey{})
	if value != nil {
		if requestMetadata, ok := value.(*RequestMetadata); ok {
			return requestMetadata, nil
		}
	}
	return nil, errors.NotFound("RequestMetadata not found")
}

func SetRequestMetadata(ctx context.Context, metadata RequestMetadata) context.Context {
	requestMetadata, err := GetRequestMetadata(ctx)
	if err == nil && requestMetadata != nil {
		log.Debug().Msg("Overriding RequestMetadata in context")
	}
	requestMetadata = &metadata
	return context.WithValue(ctx, RequestMetadataCtxKey{}, requestMetadata)
}

func SetAccessToken(ctx context.Context, token *AccessToken) context.Context {
	requestMetadata, _ := GetRequestMetadata(ctx)
	if requestMetadata == nil {
		requestMetadata = &RequestMetadata{}
		requestMetadata.accessToken = token
		return context.WithValue(ctx, RequestMetadataCtxKey{}, requestMetadata)
	} else {
		requestMetadata.accessToken = token
		return context.WithValue(ctx, RequestMetadataCtxKey{}, requestMetadata)
	}
}

func SetNamespace(ctx context.Context, namespace string) context.Context {
	requestMetadata, err := GetRequestMetadata(ctx)
	var result = ctx
	if err != nil && requestMetadata == nil {
		requestMetadata = &RequestMetadata{}
		result = context.WithValue(ctx, RequestMetadataCtxKey{}, requestMetadata)
	}
	requestMetadata.namespace = namespace
	return result
}

func GetAccessToken(ctx context.Context) (*AccessToken, error) {
	// read token
	if value := ctx.Value(RequestMetadataCtxKey{}); value != nil {
		if requestMetadata, ok := value.(*RequestMetadata); ok && requestMetadata.accessToken != nil {
			return requestMetadata.accessToken, nil
		}
	}
	return nil, errors.NotFound("Access token not found")
}

func GetNamespace(ctx context.Context) (string, error) {
	// read token
	if value := ctx.Value(RequestMetadataCtxKey{}); value != nil {
		if requestMetadata, ok := value.(*RequestMetadata); ok {
			return requestMetadata.namespace, nil
		}
	}
	return "", ErrNamespaceNotFound
}

func (tokenNamespaceExtractor *AccessTokenNamespaceExtractor) Extract(ctx context.Context) (string, error) {
	// read token
	token, err := GetAccessToken(ctx)
	if err != nil {
		return "unknown", nil
	}

	if namespace := token.Namespace; namespace != "" {
		return namespace, nil
	}

	return "", errors.InvalidArgument("Namespace is empty in the token")
}

func IsAdminApi(fullMethodName string) bool {
	return adminMethods.Contains(fullMethodName)
}

func getTokenFromHeader(header string) (string, error) {
	splits := strings.SplitN(header, " ", 2)
	if len(splits) < 2 {
		return "", fmt.Errorf("could not find token in header")
	}
	return splits[1], nil
}

func getNameSpaceFromToken(token string) string {
	tokenParts := strings.SplitN(token, ".", 3)
	if len(tokenParts) < 3 {
		log.Debug().Msg("Could not split the token into its parts")
		return UnknownValue
	}
	decodedToken, err := base64.RawStdEncoding.DecodeString(tokenParts[1])
	if err != nil {
		log.Debug().Err(err).Msg("Could not base64 decode token")
		return UnknownValue
	}
	namespace, err := jsonparser.GetString(decodedToken, "https://tigris/n", "code")
	if err != nil {
		return UnknownValue
	}
	return namespace
}

func GetNameSpaceFromHeader(ctx context.Context) string {
	if !config.DefaultConfig.Auth.EnableNamespaceIsolation {
		return DefaultNamespaceName
	}
	header := api.GetHeader(ctx, "authorization")
	token, err := getTokenFromHeader(header)
	if err != nil {
		return UnknownValue
	}
	return getNameSpaceFromToken(token)

}
