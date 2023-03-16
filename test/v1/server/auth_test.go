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

//go:build integration

package server

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/services/v1/auth"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

const (
	Authorization = "Authorization"
	Bearer        = "bearer "
	tokenFilePath = "/etc/test-token.jwt"
)

func readToken(t *testing.T) string {
	tokenBytes, err := os.ReadFile(tokenFilePath)
	require.NoError(t, err)
	return string(tokenBytes)
}

func createTestNamespace(e2 *httpexpect.Expect, token string) {
	// create namespace
	var createNamespacePayload = make(map[string]string)
	createNamespacePayload["name"] = "tigris_test"
	createNamespacePayload["id"] = "tigris_test"
	_ = e2.POST(namespaceOperation("create")).
		WithHeader(Authorization, Bearer+token).
		WithJSON(createNamespacePayload).
		Expect().
		Status(http.StatusOK)
}
func TestGoTrueAuthProvider(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	token := readToken(t)
	createTestNamespace(e2, token)

	// create project
	testProject := "auth_test"
	_ = e2.POST(createProjectUrl(testProject)).WithHeader(Authorization, Bearer+token).Expect().Status(http.StatusOK)

	// create app key
	var createAppKeyPayload = make(map[string]string)
	createAppKeyPayload["name"] = "test_key"
	createAppKeyPayload["description"] = "This key is used for integration test purpose."
	createAppKeyPayload["project"] = testProject

	createdAppKey := e2.POST(appKeysOperation("auth_test", "create")).
		WithHeader(Authorization, Bearer+token).WithJSON(createAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("created_app_key")
	require.NotNil(t, createdAppKey)
	id := createdAppKey.Object().Value("id").String()
	secret := createdAppKey.Object().Value("secret").String()

	name := createdAppKey.Object().Value("name").String()
	description := createdAppKey.Object().Value("description").String()
	project := createdAppKey.Object().Value("project").String()

	require.Equal(t, "test_key", name.Raw())
	require.Equal(t, "This key is used for integration test purpose.", description.Raw())
	require.Equal(t, testProject, project.Raw())
	require.True(t, int(id.Length().Raw()) == 30+len(auth.ClientIdPrefix))         // length + prefix
	require.True(t, int(secret.Length().Raw()) == 50+len(auth.ClientSecretPrefix)) // length + prefix

	// update
	var updateAppKeyPayload = make(map[string]string)
	updateAppKeyPayload["id"] = id.Raw()
	updateAppKeyPayload["description"] = "[updated]This key is used for integration test purpose."
	updatedAppKey := e2.POST(appKeysOperation("auth_test", "update")).
		WithHeader(Authorization, Bearer+token).WithJSON(updateAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("updated_app_key")
	// updates only in description
	require.Equal(t, id.Raw(), updatedAppKey.Object().Value("id").Raw())
	require.Equal(t, "[updated]This key is used for integration test purpose.", updatedAppKey.Object().Value("description").Raw())

	// rotate
	var rotateAppKeyPayload = make(map[string]string)
	rotateAppKeyPayload["id"] = id.Raw()
	rotatedKey := e2.POST(appKeysOperation("auth_test", "rotate")).
		WithHeader(Authorization, Bearer+token).WithJSON(updateAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_key")
	require.Equal(t, id.Raw(), rotatedKey.Object().Value("id").Raw())
	require.NotEqual(t, secret.Raw(), rotatedKey.Object().Value("secret").Raw())
	require.True(t, len(rotatedKey.Object().Value("secret").String().Raw()) == 50+len(auth.ClientSecretPrefix))

	// list
	appKeys := e2.GET(appKeysOperation("auth_test", "get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_keys").Array()
	require.Equal(t, 1, int(appKeys.Length().Raw()))

	retrievedAppKey := appKeys.Element(0)
	require.Equal(t, id.Raw(), retrievedAppKey.Object().Value("id").String().Raw())
	require.NotNil(t, retrievedAppKey.Object().Value("secret").String().Raw())
	require.Equal(t, name.Raw(), retrievedAppKey.Object().Value("name").String().Raw())
	require.Equal(t, "[updated]This key is used for integration test purpose.", retrievedAppKey.Object().Value("description").String().Raw())
	require.NotNil(t, retrievedAppKey.Object().Value("project").String().Raw())
	require.True(t, strings.HasPrefix(retrievedAppKey.Object().Value("created_by").String().Raw(), "gt|"))

	// delete
	deletedResponse := e2.DELETE(appKeysOperation("auth_test", "delete")).
		WithHeader(Authorization, Bearer+token).WithJSON(updateAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		Value("deleted").
		Boolean()
	require.True(t, deletedResponse.Raw())
}

func TestMultipleAppsCreation(t *testing.T) {
	testStartTime := time.Now()

	e2 := expectLow(t, config.GetBaseURL2())
	testProject := "auth_test"
	token := readToken(t)
	_ = e2.POST(createProjectUrl(testProject)).WithHeader(Authorization, Bearer+token).Expect()

	for i := 0; i < 5; i++ {
		var createAppKeyPayload = make(map[string]string)
		createAppKeyPayload["name"] = fmt.Sprintf("test_key_%d", i)
		createAppKeyPayload["description"] = "This key is used for integration test purpose."
		createAppKeyPayload["project"] = testProject

		createdAppKey := e2.POST(appKeysOperation("auth_test", "create")).
			WithHeader(Authorization, Bearer+token).WithJSON(createAppKeyPayload).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object().Value("created_app_key")
		require.NotNil(t, createdAppKey)
		generatedClientId := createdAppKey.Object().Value("id").String().Raw()
		generatedClientSecret := createdAppKey.Object().Value("secret").String().Raw()

		require.True(t, strings.HasPrefix(generatedClientId, auth.ClientIdPrefix))
		require.True(t, strings.HasPrefix(generatedClientSecret, auth.ClientSecretPrefix))
	}

	appKeys := e2.GET(appKeysOperation("auth_test", "get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_keys").Array()

	require.Equal(t, 5, int(appKeys.Length().Raw()))
	for _, value := range appKeys.Iter() {
		createdAt := int64(value.Object().Value("created_at").Number().Raw())
		require.True(t, createdAt >= testStartTime.UnixMilli())
	}
}

func TestListAppKeys(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	testProject := "auth_test"
	token := readToken(t)

	_ = e2.POST(createProjectUrl(fmt.Sprintf("%s%d", testProject, 0))).WithHeader(Authorization, Bearer+token).Expect()
	_ = e2.POST(createProjectUrl(fmt.Sprintf("%s%d", testProject, 1))).WithHeader(Authorization, Bearer+token).Expect()

	for i := 0; i < 5; i++ {
		var createAppKeyPayload = make(map[string]string)
		projectForThisKey := fmt.Sprintf("%s%d", testProject, i%2)
		createAppKeyPayload["name"] = fmt.Sprintf("test_key_%d", i)
		createAppKeyPayload["description"] = "This key is used for integration test purpose."
		createAppKeyPayload["project"] = projectForThisKey

		createdAppKey := e2.POST(appKeysOperation(projectForThisKey, "create")).
			WithHeader(Authorization, Bearer+token).WithJSON(createAppKeyPayload).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object().Value("created_app_key")
		require.NotNil(t, createdAppKey)
	}

	appKeysEven := e2.GET(appKeysOperation(testProject+"0", "get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_keys").Array()

	// 3 even app keys should be retrieved
	require.Equal(t, 3, int(appKeysEven.Length().Raw()))

	appKeysOdd := e2.GET(appKeysOperation(testProject+"1", "get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_keys").Array()

	// 2 odd app keys should be retrieved
	require.Equal(t, 2, int(appKeysOdd.Length().Raw()))
}

func TestEmptyListAppKeys(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	testProject := "TestEmptyListAppKeys"
	token := readToken(t)

	_ = e2.POST(createProjectUrl(testProject)).WithHeader(Authorization, Bearer+token).Expect()

	appKeys := e2.GET(appKeysOperation(testProject, "get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().Object().Raw()
	var emptyMap = make(map[string]interface{})
	require.Equal(t, emptyMap, appKeys)
}

func TestCreateAccessToken(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	testProject := "auth_test"
	token := readToken(t)

	var createAppKeyPayload = make(map[string]string)
	createAppKeyPayload["name"] = "test_key"
	createAppKeyPayload["description"] = "This key is used for integration test purpose."
	createAppKeyPayload["project"] = testProject

	createdAppKey := e2.POST(appKeysOperation("auth_test", "create")).
		WithHeader(Authorization, Bearer+token).WithJSON(createAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("created_app_key")
	require.NotNil(t, createdAppKey)

	id := createdAppKey.Object().Value("id").String()
	secret := createdAppKey.Object().Value("secret").String()

	getAccessTokenResponse := e2.POST(getAuthToken()).
		WithFormField("client_id", id.Raw()).
		WithFormField("client_secret", secret.Raw()).
		WithFormField("grant_type", "client_credentials").
		Expect()
	getAccessTokenResponse.Status(http.StatusOK)

	accessToken := getAccessTokenResponse.JSON().Object().Value("access_token").String().Raw()
	require.True(t, accessToken != "")
	require.NotNil(t, getAccessTokenResponse.JSON().Object().Value("expires_in"))

	// use access token
	_ = e2.POST(createProjectUrl("new-project")).WithHeader(Authorization, Bearer+accessToken).Expect().Status(http.StatusOK)
}

func TestCreateAccessTokenUsingInvalidCreds(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	getAccessTokenResponse := e2.POST(getAuthToken()).
		WithFormField("client_id", "invalid-id").
		WithFormField("client_secret", "invalid-password").
		WithFormField("grant_type", "client_credentials").
		Expect()
	getAccessTokenResponse.Status(http.StatusUnauthorized)
	errorMessage := getAccessTokenResponse.JSON().Object().Value("error").Object().Value("message").String().Raw()
	require.Equal(t, "Invalid credentials", errorMessage)
}

func TestAuthFailure(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	testProject := "auth_test"
	authFailureErrorResponse := e2.POST(createProjectUrl(testProject)).Expect().
		Status(http.StatusUnauthorized).
		JSON().
		Object().
		Value("error").
		Object()
	require.Equal(t, "UNAUTHENTICATED", authFailureErrorResponse.Value("code").String().Raw())
	require.Equal(t, "request unauthenticated with bearer", authFailureErrorResponse.Value("message").String().Raw())
}
