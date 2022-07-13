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
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

func TestRequestMetadata(t *testing.T) {

	t.Run("extraction of namespace name", func(t *testing.T) {
		ctx := context.TODO()
		ctx = context.WithValue(ctx, RequestMetadataCtxKey{}, &RequestMetadata{
			accessToken: &AccessToken{
				Namespace: "test-namespace-1",
				Sub:       "test@tigrisdata.com",
			},
			namespace: "test-namespace-1",
		})
		extractor := &AccessTokenNamespaceExtractor{}
		namespaceName, err := extractor.Extract(ctx)
		require.Nil(t, err)
		require.Equal(t, "test-namespace-1", namespaceName)
	})

	t.Run("extraction of token", func(t *testing.T) {
		ctx := context.TODO()
		ctx = context.WithValue(ctx, RequestMetadataCtxKey{}, &RequestMetadata{
			accessToken: &AccessToken{
				Namespace: "test-namespace-1",
				Sub:       "test@tigrisdata.com",
			},
			namespace: "test-namespace-1",
		})
		accessToken, err := GetAccessToken(ctx)
		require.Nil(t, err)
		require.Equal(t, "test@tigrisdata.com", accessToken.Sub)
	})

	t.Run("extraction of namespace name - failure", func(t *testing.T) {
		ctx := context.TODO()
		extractor := &AccessTokenNamespaceExtractor{}
		namespaceName, _ := extractor.Extract(ctx)
		require.Equal(t, "unknown", namespaceName)
	})

	t.Run("extraction of token - failure", func(t *testing.T) {
		ctx := context.TODO()
		token, err := GetAccessToken(ctx)
		require.Nil(t, token)
		require.Equal(t, api.Errorf(api.Code_NOT_FOUND, "Access token not found"), err)
	})

	t.Run("isAdmin test", func(t *testing.T) {
		require.True(t, IsAdminApi("/tigrisdata.admin.v1.Admin/CreateNamespace"))
		require.True(t, IsAdminApi("/tigrisdata.admin.v1.Admin/ListNamespaces"))
		require.False(t, IsAdminApi("/.HealthAPI/Health"))
		require.False(t, IsAdminApi("some-random"))
	})

}
