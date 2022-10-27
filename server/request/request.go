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
	"github.com/tigrisdata/tigris/server/defaults"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc"
)

var (
	adminMethods = container.NewHashSet(api.CreateNamespaceMethodName, api.ListNamespaceMethodName, api.DescribeNamespacesMethodName)
	tenantGetter metadata.TenantGetter
)

type MetadataCtxKey struct{}

type AccessToken struct {
	Namespace string
	Sub       string
}

type Metadata struct {
	accessToken *AccessToken
	serviceName string
	methodInfo  grpc.MethodInfo
	// this is authenticated namespace
	namespace string
	// human readable namespace name
	namespaceName string
	IsHuman       bool
	// this is parsed and set early in request processing chain - before it is authenticated for observability.
	unauthenticatedNamespaceName string
}

func Init(tg metadata.TenantGetter) {
	tenantGetter = tg
}

func NewRequestEndpointMetadata(ctx context.Context, serviceName string, methodInfo grpc.MethodInfo) Metadata {
	ns, utype := GetMetadataFromHeader(ctx)
	return Metadata{serviceName: serviceName, methodInfo: methodInfo, unauthenticatedNamespaceName: ns, IsHuman: utype}
}

func GetGrpcEndPointMetadataFromFullMethod(ctx context.Context, fullMethod string, methodType string) Metadata {
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

func (r *Metadata) GetMethodName() string {
	s := strings.Split(r.methodInfo.Name, "/")
	if len(s) > 2 {
		return s[2]
	}
	return r.methodInfo.Name
}

func (r *Metadata) GetServiceType() string {
	if r.methodInfo.IsServerStream {
		return "stream"
	} else {
		return "unary"
	}
}

func (r *Metadata) GetServiceName() string {
	return r.serviceName
}

func (r *Metadata) GetUnAuthenticatedNamespaceName() string {
	return r.unauthenticatedNamespaceName
}

func (r *Metadata) GetMethodInfo() grpc.MethodInfo {
	return r.methodInfo
}

func (r *Metadata) GetInitialTags(ctx context.Context) map[string]string {
	var tigrisTenantValue string
	var tigrisTenantNameValue string

	if r.namespace == "" {
		// Not authenticated yet, this is currently used in the measure interceptor where all requests should
		// be authenticated
		tigrisTenantValue = r.unauthenticatedNamespaceName
		tenant, err := tenantGetter.GetTenant(ctx, r.unauthenticatedNamespaceName)
		if err != nil {
			// unable to extract the tenant for this unauthenticated namespace-id
			// set it to NA - TODO: add a negative cache here to improve this path
			tigrisTenantNameValue = "NA"
		} else {
			tigrisTenantNameValue = tenant.GetNamespace().Metadata().Name
		}
	} else {
		// Authenticated, the SetNamespace is called from the auth middleware from authFunction
		tigrisTenantValue = r.namespace
		tigrisTenantNameValue = r.namespaceName
	}

	return map[string]string{
		"grpc_method":        r.methodInfo.Name,
		"tigris_tenant":      tigrisTenantValue,
		"tigris_tenant_name": tigrisTenantNameValue,
		"env":                config.GetEnvironment(),
		"db":                 defaults.UnknownValue,
		"collection":         defaults.UnknownValue,
	}
}

func (r *Metadata) GetFullMethod() string {
	return fmt.Sprintf("/%s/%s", r.serviceName, r.methodInfo.Name)
}

// NamespaceExtractor - extract the namespace from context.
type NamespaceExtractor interface {
	Extract(ctx context.Context) (string, error)
}

type AccessTokenNamespaceExtractor struct{}

var ErrNamespaceNotFound = errors.NotFound("namespace not found")

func GetRequestMetadata(ctx context.Context) (*Metadata, error) {
	// read token
	value := ctx.Value(MetadataCtxKey{})
	if value != nil {
		if requestMetadata, ok := value.(*Metadata); ok {
			return requestMetadata, nil
		}
	}
	return nil, errors.NotFound("Metadata not found")
}

func SetRequestMetadata(ctx context.Context, metadata Metadata) context.Context {
	requestMetadata, err := GetRequestMetadata(ctx)
	if err == nil && requestMetadata != nil {
		log.Debug().Msg("Overriding Metadata in context")
	}
	requestMetadata = &metadata
	return context.WithValue(ctx, MetadataCtxKey{}, requestMetadata)
}

func SetAccessToken(ctx context.Context, token *AccessToken) context.Context {
	requestMetadata, _ := GetRequestMetadata(ctx)
	if requestMetadata == nil {
		requestMetadata = &Metadata{}
		requestMetadata.accessToken = token
		return context.WithValue(ctx, MetadataCtxKey{}, requestMetadata)
	} else {
		requestMetadata.accessToken = token
		return context.WithValue(ctx, MetadataCtxKey{}, requestMetadata)
	}
}

func SetNamespace(ctx context.Context, namespace string) context.Context {
	var tenantName string
	requestMetadata, err := GetRequestMetadata(ctx)
	result := ctx
	if err != nil && requestMetadata == nil {
		requestMetadata = &Metadata{}
		result = context.WithValue(ctx, MetadataCtxKey{}, requestMetadata)
	}
	requestMetadata.namespace = namespace

	tenant, err := tenantGetter.GetTenant(ctx, namespace)
	if err != nil {
		ulog.E(err)
	}
	if tenant == nil {
		tenantName = defaults.DefaultNamespaceName
	} else {
		tenantName = tenant.GetNamespace().Metadata().Name
	}
	requestMetadata.namespaceName = metrics.GetTenantNameTagValue(namespace, tenantName)
	return result
}

func GetAccessToken(ctx context.Context) (*AccessToken, error) {
	// read token
	if value := ctx.Value(MetadataCtxKey{}); value != nil {
		if requestMetadata, ok := value.(*Metadata); ok && requestMetadata.accessToken != nil {
			return requestMetadata.accessToken, nil
		}
	}
	return nil, errors.NotFound("Access token not found")
}

func GetNamespace(ctx context.Context) (string, error) {
	// read token
	if value := ctx.Value(MetadataCtxKey{}); value != nil {
		if requestMetadata, ok := value.(*Metadata); ok {
			return requestMetadata.namespace, nil
		}
	}
	return "", ErrNamespaceNotFound
}

func IsHumanUser(ctx context.Context) bool {
	if value := ctx.Value(MetadataCtxKey{}); value != nil {
		if requestMetadata, ok := value.(*Metadata); ok {
			return requestMetadata.IsHuman
		}
	}
	return false
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

// extracts namespace and type of the user from the token.
func getMetadataFromToken(token string) (string, bool) {
	tokenParts := strings.SplitN(token, ".", 3)
	if len(tokenParts) < 3 {
		log.Debug().Msg("Could not split the token into its parts")
		return defaults.UnknownValue, false
	}
	decodedToken, err := base64.RawStdEncoding.DecodeString(tokenParts[1])
	if err != nil {
		log.Debug().Err(err).Msg("Could not base64 decode token")
		return defaults.UnknownValue, false
	}
	namespace, err := jsonparser.GetString(decodedToken, "https://tigris/n", "code")
	if err != nil {
		return defaults.UnknownValue, false
	}
	user, _, _, err := jsonparser.Get(decodedToken, "https://tigris/u")
	if err != nil {
		// no-op
		log.Trace().Err(err).Msg("Failed to read https://tigris/u from access token")
	}
	return namespace, len(user) > 0
}

func GetMetadataFromHeader(ctx context.Context) (string, bool) {
	if !config.DefaultConfig.Auth.EnableNamespaceIsolation {
		return defaults.DefaultNamespaceName, false
	}
	header := api.GetHeader(ctx, "authorization")
	token, err := getTokenFromHeader(header)
	if err != nil {
		return defaults.DefaultNamespaceName, false
	}

	return getMetadataFromToken(token)
}

func isRead(name string) bool {
	if strings.HasPrefix(name, api.ObservabilityMethodPrefix) {
		return true
	}

	// TODO: Probably cherry pick read and write methods
	if strings.HasPrefix(name, api.ManagementMethodPrefix) {
		return true
	}

	switch name {
	case api.ReadMethodName, api.EventsMethodName, api.SearchMethodName, api.SubscribeMethodName:
		return true
	case api.ListCollectionsMethodName, api.ListDatabasesMethodName:
		return true
	case api.DescribeCollectionMethodName, api.DescribeDatabaseMethodName:
		return true
	default:
		return false
	}
}

func isWrite(name string) bool {
	return !isRead(name)
}

func IsRead(ctx context.Context) bool {
	m, _ := grpc.Method(ctx)
	return isRead(m)
}

func IsWrite(ctx context.Context) bool {
	m, _ := grpc.Method(ctx)
	return isWrite(m)
}
