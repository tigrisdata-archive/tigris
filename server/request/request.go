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
	"fmt"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/tigrisdata/tigris/server/config"

	api "github.com/tigrisdata/tigris/api/server/v1"
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

type RequestMetadataCtxKey struct {
}

type AccessToken struct {
	Namespace string
	Sub       string
}

type RequestMetadata struct {
	accessToken *AccessToken
	namespace   string
}

// NamespaceExtractor - extract the namespace from context
type NamespaceExtractor interface {
	Extract(ctx context.Context) (string, error)
}

type AccessTokenNamespaceExtractor struct {
}

var ErrNamespaceNotFound = api.Errorf(api.Code_NOT_FOUND, "namespace not found")

func GetRequestMetadata(ctx context.Context) (*RequestMetadata, error) {
	// read token
	value := ctx.Value(RequestMetadataCtxKey{})
	if value != nil {
		if requestMetadata, ok := value.(*RequestMetadata); ok {
			return requestMetadata, nil
		}
	}
	return nil, api.Errorf(api.Code_NOT_FOUND, "RequestMetadata not found")
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
	value := ctx.Value(RequestMetadataCtxKey{})
	if value != nil {
		if requestMetadata, ok := value.(*RequestMetadata); ok {
			return requestMetadata.accessToken, nil
		}
	}
	return nil, api.Errorf(api.Code_NOT_FOUND, "Access token not found")
}

func GetNamespace(ctx context.Context) (string, error) {
	// read token
	value := ctx.Value(RequestMetadataCtxKey{})
	if value != nil {
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

	if token != nil {
		namespace := token.Namespace
		if namespace != "" {
			return namespace, nil
		}
	}
	return "", api.Errorf(api.Code_INVALID_ARGUMENT, "Namespace does not exist")
}

func IsAdminApi(fullMethodName string) bool {
	return strings.HasPrefix(fullMethodName, "/tigrisdata.admin.v1.Admin/")
}

func getTokenFromHeader(header string) ([]byte, error) {
	splits := strings.SplitN(header, " ", 2)
	if len(splits) < 2 {
		return nil, fmt.Errorf("could not find token in header")
	}
	return []byte(splits[1]), nil
}

func getNameSpaceFromToken(token []byte) string {
	namespace, err := jsonparser.GetString(token, "https://tigris/n", "code")
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
