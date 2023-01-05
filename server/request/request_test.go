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
	"testing"

	"github.com/bmizerany/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/types"
)

func TestRequestMetadata(t *testing.T) {
	t.Run("extraction of namespace name", func(t *testing.T) {
		ctx := context.TODO()
		ctx = context.WithValue(ctx, MetadataCtxKey{}, &Metadata{
			accessToken: &types.AccessToken{
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
		ctx = context.WithValue(ctx, MetadataCtxKey{}, &Metadata{
			accessToken: &types.AccessToken{
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
		require.Equal(t, errors.NotFound("Access token not found"), err)
	})

	t.Run("isAdmin test", func(t *testing.T) {
		require.True(t, IsAdminApi("/tigrisdata.management.v1.Management/CreateNamespace"))
		require.True(t, IsAdminApi("/tigrisdata.management.v1.Management/ListNamespaces"))
		require.False(t, IsAdminApi("/.HealthAPI/Health"))
		require.False(t, IsAdminApi("some-random"))
	})

	t.Run("Test get namespace from token 1", func(t *testing.T) {
		// base64 encoding of {"https://tigris/u":{"email":"test@tigrisdata.com"},"https://tigris/n":{"code":"test-namespace"},"iss":"https://test-issuer.com/","sub":"google-oauth2|1","aud":["https://tigris-api-test"],"iat":1662745495,"exp":1662831895,"azp":"test","scope":"openid profile email","org_id":"test"}
		testToken := "header.eyJodHRwczovL3RpZ3Jpcy91Ijp7ImVtYWlsIjoidGVzdEB0aWdyaXNkYXRhLmNvbSJ9LCJodHRwczovL3RpZ3Jpcy9uIjp7ImNvZGUiOiJ0ZXN0LW5hbWVzcGFjZSJ9LCJpc3MiOiJodHRwczovL3Rlc3QtaXNzdWVyLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEiLCJhdWQiOlsiaHR0cHM6Ly90aWdyaXMtYXBpLXRlc3QiXSwiaWF0IjoxNjYyNzQ1NDk1LCJleHAiOjE2NjI4MzE4OTUsImF6cCI6InRlc3QiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIiwib3JnX2lkIjoidGVzdCJ9.signature" //nolint:gosec
		ns, utype, sub := getMetadataFromToken(testToken)
		assert.Equal(t, "test-namespace", ns)
		assert.Equal(t, true, utype)
		assert.Equal(t, "google-oauth2|1", sub)
	})

	t.Run("Test get namespace from token 2", func(t *testing.T) {
		// base64 encoding of {"https://tigris/u":{},"https://tigris/n":{"code":"test-namespace"},"iss":"https://test-issuer.com/","sub":"google-oauth2|1","aud":["https://tigris-api-test"],"iat":1662745495,"exp":1662831895,"azp":"test","scope":"openid profile email","org_id":"test"}
		testToken := "header.ewogICJodHRwczovL3RpZ3Jpcy91Ijoge30sCiAgImh0dHBzOi8vdGlncmlzL24iOiB7CiAgICAiY29kZSI6ICJ0ZXN0LW5hbWVzcGFjZSIKICB9LAogICJpc3MiOiAiaHR0cHM6Ly90ZXN0LWlzc3Vlci5jb20vIiwKICAic3ViIjogImdvb2dsZS1vYXV0aDJ8MSIsCiAgImF1ZCI6IFsKICAgICJodHRwczovL3RpZ3Jpcy1hcGktdGVzdCIKICBdLAogICJpYXQiOiAxNjYyNzQ1NDk1LAogICJleHAiOiAxNjYyODMxODk1LAogICJhenAiOiAidGVzdCIsCiAgInNjb3BlIjogIm9wZW5pZCBwcm9maWxlIGVtYWlsIiwKICAib3JnX2lkIjogInRlc3QiCn0.signature" //nolint:gosec
		ns, utype, sub := getMetadataFromToken(testToken)
		assert.Equal(t, "test-namespace", ns)
		assert.Equal(t, true, utype)
		assert.Equal(t, "google-oauth2|1", sub)
	})

	t.Run("Test get namespace from token 3", func(t *testing.T) {
		// base64 encoding of {"https://tigris/n":{"code":"test-namespace"},"iss":"https://test-issuer.com/","sub":"google-oauth2|1","aud":["https://tigris-api-test"],"iat":1662745495,"exp":1662831895,"azp":"test","scope":"openid profile email","org_id":"test"}
		testToken := "header.eyJodHRwczovL3RpZ3Jpcy9uIjp7ImNvZGUiOiJ0ZXN0LW5hbWVzcGFjZSJ9LCJpc3MiOiJodHRwczovL3Rlc3QtaXNzdWVyLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEiLCJhdWQiOlsiaHR0cHM6Ly90aWdyaXMtYXBpLXRlc3QiXSwiaWF0IjoxNjYyNzQ1NDk1LCJleHAiOjE2NjI4MzE4OTUsImF6cCI6InRlc3QiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIiwib3JnX2lkIjoidGVzdCJ9.signature" //nolint:gosec
		ns, utype, sub := getMetadataFromToken(testToken)
		assert.Equal(t, "test-namespace", ns)
		assert.Equal(t, false, utype)
		assert.Equal(t, "google-oauth2|1", sub)
	})
}
