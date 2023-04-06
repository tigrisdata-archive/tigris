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
		require.True(t, IsAdminApi("/tigrisdata.management.v1.Management/DeleteNamespace"))
		require.False(t, IsAdminApi("/.HealthAPI/Health"))
		require.False(t, IsAdminApi("some-random"))
	})

	t.Run("Test get namespace from token 1", func(t *testing.T) {
		// base64 encoding of {"https://tigris":{"ue":"test@tigrisdata.com","nc":"test-namespace"},"iss":"https://test-issuer.com/","sub":"google-oauth2|1","aud":["https://tigris-api-test"],"iat":1662745495,"exp":1662831895,"azp":"test","scope":"openid profile email","org_id":"test"}
		testToken := "header.eyJodHRwczovL3RpZ3JpcyI6eyJ1ZSI6InRlc3RAdGlncmlzZGF0YS5jb20iLCJuYyI6InRlc3QtbmFtZXNwYWNlIn0sImlzcyI6Imh0dHBzOi8vdGVzdC1pc3N1ZXIuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MSIsImF1ZCI6WyJodHRwczovL3RpZ3Jpcy1hcGktdGVzdCJdLCJpYXQiOjE2NjI3NDU0OTUsImV4cCI6MTY2MjgzMTg5NSwiYXpwIjoidGVzdCIsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwiLCJvcmdfaWQiOiJ0ZXN0In0=.signature" //nolint:gosec
		ns, utype, sub := getMetadataFromToken(testToken)
		assert.Equal(t, "test-namespace", ns)
		assert.Equal(t, true, utype)
		assert.Equal(t, "google-oauth2|1", sub)
	})

	t.Run("Test get namespace from token 2 (backward compatibility)", func(t *testing.T) {
		// base64 encoding of {"https://tigris/n":{"code":"test_namespace"},"iss":"https://some-issuer","sub":"google2|12","aud":"https://test","iat":1676573357,"exp":1676659757,"azp":"123","gty":"client-credentials"}
		testToken := "header.eyJodHRwczovL3RpZ3Jpcy9uIjp7ImNvZGUiOiJ0ZXN0X25hbWVzcGFjZSJ9LCJpc3MiOiJodHRwczovL3NvbWUtaXNzdWVyIiwic3ViIjoiZ29vZ2xlMnwxMiIsImF1ZCI6Imh0dHBzOi8vdGVzdCIsImlhdCI6MTY3NjU3MzM1NywiZXhwIjoxNjc2NjU5NzU3LCJhenAiOiIxMjMiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMifQ==.signature" //nolint:gosec
		ns, utype, sub := getMetadataFromToken(testToken)
		assert.Equal(t, "test_namespace", ns)
		assert.Equal(t, false, utype)
		assert.Equal(t, "google2|12", sub)
	})

	//
	t.Run("Test get namespace from token 3", func(t *testing.T) {
		// base64 encoding of {"https://tigris":{"nc":"test-namespace","ue":"test@test.com"},"iss":"https://test-issuer.com/","sub":"google-oauth2|2","aud":["https://tigris-api-test"],"iat":1662745495,"exp":1662831895,"azp":"test","scope":"openid profile email","org_id":"test"}
		testToken := "header.eyJodHRwczovL3RpZ3JpcyI6eyJuYyI6InRlc3QtbmFtZXNwYWNlIiwidWUiOiJ0ZXN0QHRlc3QuY29tIn0sImlzcyI6Imh0dHBzOi8vdGVzdC1pc3N1ZXIuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MiIsImF1ZCI6WyJodHRwczovL3RpZ3Jpcy1hcGktdGVzdCJdLCJpYXQiOjE2NjI3NDU0OTUsImV4cCI6MTY2MjgzMTg5NSwiYXpwIjoidGVzdCIsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwiLCJvcmdfaWQiOiJ0ZXN0In0=.signature" //nolint:gosec
		ns, utype, sub := getMetadataFromToken(testToken)
		assert.Equal(t, "test-namespace", ns)
		assert.Equal(t, true, utype)
		assert.Equal(t, "google-oauth2|2", sub)
	})

	t.Run("Test get namespace from token 4", func(t *testing.T) {
		// base64 encoding of {"https://tigris":{"nc":"test-namespace"},"iss":"https://test-issuer.com/","sub":"google-oauth2|1","aud":["https://tigris-api-test"],"iat":1662745495,"exp":1662831895,"azp":"test","scope":"openid profile email","org_id":"test"}
		testToken := "header.eyJodHRwczovL3RpZ3JpcyI6eyJuYyI6InRlc3QtbmFtZXNwYWNlIn0sImlzcyI6Imh0dHBzOi8vdGVzdC1pc3N1ZXIuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MSIsImF1ZCI6WyJodHRwczovL3RpZ3Jpcy1hcGktdGVzdCJdLCJpYXQiOjE2NjI3NDU0OTUsImV4cCI6MTY2MjgzMTg5NSwiYXpwIjoidGVzdCIsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwiLCJvcmdfaWQiOiJ0ZXN0In0=.signature" //nolint:gosec
		ns, utype, sub := getMetadataFromToken(testToken)
		assert.Equal(t, "test-namespace", ns)
		assert.Equal(t, false, utype)
		assert.Equal(t, "google-oauth2|1", sub)
	})
}
