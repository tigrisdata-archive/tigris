package middleware

import (
	"context"

	api "github.com/tigrisdata/tigris/api/server/v1"
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
	isAdminApi  bool
}

// NamespaceExtractor - extract the namespace from context
type NamespaceExtractor interface {
	Extract(ctx context.Context) (string, error)
}

type AccessTokenNamespaceExtractor struct {
}

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

func setAccessToken(ctx context.Context, token *AccessToken) context.Context {
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

func setNamespace(ctx context.Context, namespace string) context.Context {
	requestMetadata, err := GetRequestMetadata(ctx)
	var result = ctx
	if err != nil && requestMetadata == nil {
		requestMetadata = &RequestMetadata{}
		result = context.WithValue(ctx, RequestMetadataCtxKey{}, requestMetadata)
	}
	requestMetadata.namespace = namespace
	return result
}

func setIsAdminApi(ctx context.Context, isAdminApi bool) context.Context {
	requestMetadata, err := GetRequestMetadata(ctx)
	var result = ctx
	if err != nil && requestMetadata == nil {
		requestMetadata = &RequestMetadata{}
		result = context.WithValue(ctx, RequestMetadataCtxKey{}, requestMetadata)
	}
	requestMetadata.isAdminApi = isAdminApi
	return result
}
func IsAdminApi(ctx context.Context) (bool, error) {
	// read metadata
	value := ctx.Value(RequestMetadataCtxKey{})
	if value != nil {
		if requestMetadata, ok := value.(*RequestMetadata); ok {
			return requestMetadata.isAdminApi, nil
		}
	}
	return false, api.Errorf(api.Code_NOT_FOUND, "Access token not found")
}
func GetAccessToken(ctx context.Context) (*AccessToken, error) {
	// read metadata
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
	return "", api.Errorf(api.Code_NOT_FOUND, "Namespace not found")
}

func (tokenNamespaceExtractor *AccessTokenNamespaceExtractor) Extract(ctx context.Context) (string, error) {
	// read token
	token, err := GetAccessToken(ctx)
	if err != nil {
		return "", err
	}

	if token != nil {
		namespace := token.Namespace
		if namespace != "" {
			return namespace, nil
		}
	}
	return "", api.Errorf(api.Code_INVALID_ARGUMENT, "Namespace does not exist")
}
