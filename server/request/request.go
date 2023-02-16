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
	"github.com/tigrisdata/tigris/server/types"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc"
)

var (
	adminMethods = container.NewHashSet(api.CreateNamespaceMethodName, api.ListNamespaceMethodName, api.DescribeNamespacesMethodName)
	tenantGetter metadata.TenantGetter
)

type MetadataCtxKey struct{}

type Metadata struct {
	accessToken *types.AccessToken
	serviceName string
	methodInfo  grpc.MethodInfo
	// The namespace id (uuid) of the request. The metadata extractor sets it when the request is not yet
	// authenticated, and auth interceptor updates it
	namespace string
	// human readable namespace name
	namespaceName string
	IsHuman       bool

	// this will hold the information about the project and collection under target
	// this will be set to empty string for requests which are not project/collection specific
	project    string
	collection string

	// Current user/application
	Sub string
}

func Init(tg metadata.TenantGetter) {
	tenantGetter = tg
}

func NewRequestMetadata(ctx context.Context) Metadata {
	ns, utype, sub := GetMetadataFromHeader(ctx)
	md := Metadata{IsHuman: utype, Sub: sub}
	md.SetNamespace(ctx, ns)
	return md
}

func NewRequestEndpointMetadata(ctx context.Context, serviceName string, methodInfo grpc.MethodInfo, db string, coll string) Metadata {
	ns, utype, sub := GetMetadataFromHeader(ctx)
	md := Metadata{serviceName: serviceName, methodInfo: methodInfo, IsHuman: utype, Sub: sub}
	md.SetNamespace(ctx, ns)
	return md
}

func GetGrpcEndPointMetadataFromFullMethod(ctx context.Context, fullMethod string, methodType string, req interface{}) Metadata {
	project, coll := GetProjectAndColl(req)
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
	return NewRequestEndpointMetadata(ctx, svcName, methodInfo, project, coll)
}

func (m *Metadata) SetProject(project string) {
	m.project = project
}

func (m *Metadata) SetCollection(collection string) {
	m.collection = collection
}

func (m *Metadata) GetProject() string {
	return m.project
}

func (m *Metadata) GetCollection() string {
	return m.collection
}

func (m *Metadata) SetAccessToken(token *types.AccessToken) {
	m.accessToken = token
}

func (m *Metadata) GetNamespace() string {
	return m.namespace
}

func (m *Metadata) GetNamespaceName() string {
	return m.namespaceName
}

func (m *Metadata) GetMethodName() string {
	s := strings.Split(m.methodInfo.Name, "/")
	if len(s) > 2 {
		return s[2]
	}
	return m.methodInfo.Name
}

func (m *Metadata) GetServiceType() string {
	if m.methodInfo.IsServerStream {
		return "stream"
	} else {
		return "unary"
	}
}

func (m *Metadata) GetServiceName() string {
	return m.serviceName
}

func (m *Metadata) GetMethodInfo() grpc.MethodInfo {
	return m.methodInfo
}

func (m *Metadata) GetInitialTags() map[string]string {
	return map[string]string{
		"grpc_method":        m.methodInfo.Name,
		"tigris_tenant":      m.namespace,
		"tigris_tenant_name": m.GetTigrisNamespaceNameTag(),
		"env":                config.GetEnvironment(),
		"project":            defaults.UnknownValue,
		"collection":         defaults.UnknownValue,
	}
}

func GetProjectAndColl(req interface{}) (string, string) {
	project := ""
	coll := ""
	if req != nil {
		if rc, ok := req.(api.RequestWithProjectAndCollection); ok {
			project = rc.GetProject()
			coll = rc.GetCollection()
		} else if r, ok := req.(api.RequestWithProject); ok {
			project = r.GetProject()
		}
	}
	return project, coll
}

func (m *Metadata) GetFullMethod() string {
	return fmt.Sprintf("/%s/%s", m.serviceName, m.methodInfo.Name)
}

func (m *Metadata) SaveToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, MetadataCtxKey{}, m)
}

func (m *Metadata) SetNamespace(ctx context.Context, namespace string) {
	m.namespace = namespace
	if !config.DefaultConfig.Auth.EnableNamespaceIsolation {
		m.namespaceName = defaults.DefaultNamespaceName
		return
	}
	tenant, err := tenantGetter.GetTenant(ctx, namespace)
	if err != nil {
		m.namespaceName = defaults.DefaultNamespaceName
		ulog.E(err)
		return
	}

	if tenant == nil {
		m.namespaceName = defaults.DefaultNamespaceName
	} else {
		m.namespaceName = tenant.GetNamespace().Metadata().Name
	}
}

func (m *Metadata) GetTigrisNamespaceNameTag() string {
	return metrics.GetTenantNameTagValue(m.namespace, m.namespaceName)
}

// NamespaceExtractor - extract the namespace from context.
type NamespaceExtractor interface {
	Extract(ctx context.Context) (string, error)
}

type AccessTokenNamespaceExtractor struct{}

var ErrNamespaceNotFound = errors.NotFound("namespace not found")

func GetRequestMetadataFromContext(ctx context.Context) (*Metadata, error) {
	// read token
	value := ctx.Value(MetadataCtxKey{})
	if value != nil {
		if requestMetadata, ok := value.(*Metadata); ok {
			return requestMetadata, nil
		}
	}
	return nil, errors.NotFound("Metadata not found")
}

func GetAccessToken(ctx context.Context) (*types.AccessToken, error) {
	// read token
	if value := ctx.Value(MetadataCtxKey{}); value != nil {
		if requestMetadata, ok := value.(*Metadata); ok && requestMetadata.accessToken != nil {
			return requestMetadata.accessToken, nil
		}
	}
	return nil, errors.NotFound("Access token not found")
}

func GetCurrentSub(ctx context.Context) (string, error) {
	tkn, err := GetAccessToken(ctx)
	if err != nil {
		return "", err
	}
	return tkn.Sub, nil
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
	token, _ := GetAccessToken(ctx)
	if token == nil {
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
func getMetadataFromToken(token string) (string, bool, string) {
	tokenParts := strings.SplitN(token, ".", 3)
	if len(tokenParts) < 3 {
		log.Debug().Msg("Could not split the token into its parts")
		return defaults.UnknownValue, false, ""
	}

	decodedToken, err := base64.RawStdEncoding.DecodeString(tokenParts[1])
	if err != nil {
		log.Debug().Err(err).Msg("Could not base64 decode token")
		return defaults.UnknownValue, false, ""
	}

	tigrisClaimObj, _, _, err := jsonparser.Get(decodedToken, "https://tigris")
	if err != nil {
		return defaults.UnknownValue, false, ""
	}

	namespaceCode, err := jsonparser.GetString(tigrisClaimObj, "nc")
	if err != nil {
		return defaults.UnknownValue, false, ""
	}

	userEmail, err := jsonparser.GetString(tigrisClaimObj, "ue")
	if err != nil {
		// no-op
		log.Trace().Err(err).Msg("Failed to read ue from access token")
	}
	
	sub, err := jsonparser.GetString(decodedToken, "sub")
	if err != nil {
		log.Trace().Err(err).Msg("Failed to read sub from access token")
		return defaults.UnknownValue, false, ""
	}
	return namespaceCode, len(userEmail) > 0, sub
}

func GetMetadataFromHeader(ctx context.Context) (string, bool, string) {
	if !config.DefaultConfig.Auth.EnableNamespaceIsolation {
		return defaults.DefaultNamespaceName, false, ""
	}
	header := api.GetHeader(ctx, "authorization")
	token, err := getTokenFromHeader(header)
	if err != nil {
		return defaults.DefaultNamespaceName, false, ""
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
