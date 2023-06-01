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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/services/v1/auth"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

const (
	Authorization         = "Authorization"
	Bearer                = "bearer "
	RSATokenFilePath      = "../../docker/test-token-rsa.jwt"          //nolint:gosec
	HSTokenFilePath       = "../../docker/test-token-hs.jwt"           //nolint:gosec
	HSTokenAFilePath      = "../../docker/test-token-A-hs.jwt"         //nolint:gosec
	HSTokenBFilePath      = "../../docker/test-token-B-hs.jwt"         //nolint:gosec
	OwnerTokenFilePath    = "../../docker/test-token-rsa-owner.jwt"    //nolint:gosec
	EditorTokenFilePath   = "../../docker/test-token-rsa-editor.jwt"   //nolint:gosec
	ReadOnlyTokenFilePath = "../../docker/test-token-rsa-readonly.jwt" //nolint:gosec
)

func readToken(t *testing.T, file string) string {
	tokenBytes, err := os.ReadFile(file)
	require.NoError(t, err)
	return string(tokenBytes)
}

func createNamespaceWithToken(t *testing.T, name string, token string) *httpexpect.Response {
	e := expectLow(t, config.GetBaseURL2())
	return e.POST(getCreateNamespaceURL()).
		WithHeader(Authorization, Bearer+token).
		WithJSON(Map{"name": name}).
		Expect()
}

func createTestNamespace(t *testing.T, token string) {
	e2 := expectLow(t, config.GetBaseURL2())
	createNamespacePayload := Map{
		"name": "tigris_test_name",
		"id":   "tigris_test",
	}
	_ = e2.POST(namespaceOperation("create")).
		WithHeader(Authorization, Bearer+token).
		WithJSON(createNamespacePayload).Expect()

	createNamespacePayload2 := Map{
		"name": "tigris_test_name_non_admin",
		"id":   "tigris_test_non_admin",
	}
	_ = e2.POST(namespaceOperation("create")).
		WithHeader(Authorization, Bearer+token).
		WithJSON(createNamespacePayload2).Expect()
}

func TestHS256TokenValidation(t *testing.T) {
	token := readToken(t, HSTokenFilePath)
	createTestNamespace(t, token)

	// create project
	testProject := "TestHS256TokenValidation"
	deleteProject2(t, testProject, token)
	createProject2(t, testProject, token).Status(http.StatusOK)
}

func TestMultipleAudienceSupport(t *testing.T) {
	tokenA := readToken(t, HSTokenAFilePath)
	tokenB := readToken(t, HSTokenBFilePath)

	createTestNamespace(t, tokenA)

	// create project and test the successful response
	testProjectA := "TestMultipleAudienceSupportA"
	testProjectB := "TestMultipleAudienceSupportB"

	deleteProject2(t, testProjectA, tokenA)
	deleteProject2(t, testProjectB, tokenB)
	createProject2(t, testProjectA, tokenA).Status(http.StatusOK)
	createProject2(t, testProjectB, tokenB).Status(http.StatusOK)
}

func TestGoTrueAuthProvider(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	token := readToken(t, RSATokenFilePath)

	createTestNamespace(t, token)

	// create project
	testProject := "auth_test"
	deleteProject2(t, testProject, token)
	createProject2(t, testProject, token).Status(http.StatusOK)

	// create app key
	createdAppKey := createAppKey(e2, token, "test_key", "auth_test", "")
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
	updateAppKeyPayload := Map{
		"id":          id.Raw(),
		"description": "[updated]This key is used for integration test purpose.",
	}
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

	found := false
	for i := 0; i < int(appKeys.Length().Raw()); i++ {
		k := appKeys.Element(i)
		if k.Object().Value("id").Raw() == id.Raw() {
			found = true
			require.NotEqual(t, secret.Raw(), rotatedKey.Object().Value("secret").Raw())
		}
	}

	require.True(t, found)

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

func cleanupAppKeys(e *httpexpect.Expect, token string, project string) {
	globalAppKeys := e.GET(globalAppKeysOperation("get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object()

	if len(globalAppKeys.Keys().Raw()) > 0 {
		arr := globalAppKeys.Value("app_keys").Array()
		for i := 0; i < int(arr.Length().Raw()); i++ {
			k := arr.Element(i)
			deleteGlobalAppKey(e, token, k.Object().Value("id").String().Raw())
		}
	}

	if project != "" {
		localAppKeys := e.GET(appKeysOperation(project, "get")).
			WithHeader(Authorization, Bearer+token).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object()

		if len(localAppKeys.Keys().Raw()) > 0 {
			arr := localAppKeys.Value("app_keys").Array()
			for _, lKey := range arr.Iter() {
				deleteAppKey(e, token, lKey.Object().Value("id").String().Raw(), project)
			}
		}
	}
}

func TestGlobalAppKeys(t *testing.T) {
	e := expectLow(t, config.GetBaseURL2())
	token := readToken(t, RSATokenFilePath)

	createTestNamespace(t, token)

	cleanupAppKeys(e, token, "")

	// create app key
	createdAppKey := createGlobalAppKey(e, token, "test_key")
	require.NotNil(t, createdAppKey)
	id := createdAppKey.Object().Value("id").String()
	secret := createdAppKey.Object().Value("secret").String()

	name := createdAppKey.Object().Value("name").String()
	description := createdAppKey.Object().Value("description").String()

	require.Equal(t, "test_key", name.Raw())
	require.Equal(t, "This key is used for integration test purpose.", description.Raw())
	require.Equal(t, 30+len(auth.GlobalClientIdPrefix), int(id.Length().Raw()))         // length + prefix
	require.Equal(t, 50+len(auth.GlobalClientSecretPrefix), int(secret.Length().Raw())) // length + prefix

	// update
	updateGlobalAppKeyPayload := Map{
		"id":          id.Raw(),
		"description": "[updated]This key is used for integration test purpose.",
	}
	updatedAppKey := e.PUT(globalAppKeysOperation("update")).
		WithHeader(Authorization, Bearer+token).WithJSON(updateGlobalAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("updated_app_key")
	// updates only in description
	require.Equal(t, id.Raw(), updatedAppKey.Object().Value("id").Raw())
	require.Equal(t, "[updated]This key is used for integration test purpose.", updatedAppKey.Object().Value("description").Raw())

	// rotate
	rotateGlobalAppKeyPayload := Map{
		"id": id.Raw(),
	}
	rotatedKey := e.POST(globalAppKeysOperation("rotate")).
		WithHeader(Authorization, Bearer+token).WithJSON(rotateGlobalAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_key")
	require.Equal(t, id.Raw(), rotatedKey.Object().Value("id").Raw())
	require.NotEqual(t, secret.Raw(), rotatedKey.Object().Value("secret").Raw())
	require.Equal(t, 50+len(auth.GlobalClientSecretPrefix), len(rotatedKey.Object().Value("secret").String().Raw()))

	// list
	globalAppKeys := e.GET(globalAppKeysOperation("get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_keys").Array()

	found := false
	for i := 0; i < int(globalAppKeys.Length().Raw()); i++ {
		k := globalAppKeys.Element(i)
		if k.Object().Value("id").Raw() == id.Raw() {
			found = true
			require.NotEqual(t, secret.Raw(), rotatedKey.Object().Value("secret").Raw())
		}
	}

	require.True(t, found)

	retrievedAppKey := globalAppKeys.Element(0)
	require.Equal(t, id.Raw(), retrievedAppKey.Object().Value("id").String().Raw())
	require.NotNil(t, retrievedAppKey.Object().Value("secret").String().Raw())
	require.Equal(t, name.Raw(), retrievedAppKey.Object().Value("name").String().Raw())
	require.Equal(t, "[updated]This key is used for integration test purpose.", retrievedAppKey.Object().Value("description").String().Raw())
	require.True(t, strings.HasPrefix(retrievedAppKey.Object().Value("created_by").String().Raw(), "gt|"))

	// delete
	resp := deleteGlobalAppKey(e, token, id.Raw())
	resp.Status(http.StatusOK)
	require.True(t, resp.JSON().Object().
		Value("deleted").Boolean().Raw(),
	)
}

func TestGlobalAndLocalAppKeys(t *testing.T) {
	e := expectLow(t, config.GetBaseURL2())
	token := readToken(t, RSATokenFilePath)

	proj := "TestGlobalAndLocalAppKeys"
	createTestNamespace(t, token)
	createProject2(t, proj, token)

	cleanupAppKeys(e, token, proj)

	// create two global app key
	_ = createGlobalAppKey(e, token, "g1")
	_ = createGlobalAppKey(e, token, "g2")

	// create three local app keys
	_ = createAppKey(e, token, "l1", proj, "")
	_ = createAppKey(e, token, "l2", proj, "")
	_ = createAppKey(e, token, "l3", proj, "")

	// list
	globalAppKeys := e.GET(globalAppKeysOperation("get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_keys").Array()
	require.Equal(t, float64(2), globalAppKeys.Length().Raw())
	var globalKeysMap = make(map[string]int32)
	for _, gKey := range globalAppKeys.Iter() {
		globalKeysMap[gKey.Object().Value("name").String().Raw()]++
	}
	require.Equal(t, int32(1), globalKeysMap["g1"])
	require.Equal(t, int32(1), globalKeysMap["g2"])

	localAppKeys := e.GET(appKeysOperation(proj, "get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("app_keys").Array()
	require.Equal(t, float64(3), localAppKeys.Length().Raw())
	var localKeysMap = make(map[string]int32)
	for _, lKey := range localAppKeys.Iter() {
		localKeysMap[lKey.Object().Value("name").String().Raw()]++
	}
	require.Equal(t, int32(1), localKeysMap["l1"])
	require.Equal(t, int32(1), localKeysMap["l2"])
	require.Equal(t, int32(1), localKeysMap["l3"])
}

func deleteAppKey(e *httpexpect.Expect, token string, id string, project string) *httpexpect.Response {
	deleteGlobalAppKeyPayload := Map{
		"id": id,
	}
	return e.DELETE(appKeysOperation(project, "delete")).
		WithHeader(Authorization, Bearer+token).WithJSON(deleteGlobalAppKeyPayload).
		Expect()
}

func deleteGlobalAppKey(e *httpexpect.Expect, token string, id string) *httpexpect.Response {
	deleteGlobalAppKeyPayload := Map{
		"id": id,
	}
	return e.DELETE(globalAppKeysOperation("delete")).
		WithHeader(Authorization, Bearer+token).WithJSON(deleteGlobalAppKeyPayload).
		Expect()
}

func createAppKey(e *httpexpect.Expect, token string, name string, project string, keyType string) *httpexpect.Value {
	createAppKeyPayload := Map{
		"name":        name,
		"description": "This key is used for integration test purpose.",
		"project":     project,
	}
	if keyType != "" {
		createAppKeyPayload["key_type"] = keyType
	}
	return e.POST(appKeysOperation(project, "create")).
		WithHeader(Authorization, Bearer+token).WithJSON(createAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("created_app_key")
}

func createGlobalAppKey(e *httpexpect.Expect, token string, name string) *httpexpect.Value {
	createGlobalAppKeyPayload := Map{
		"name":        name,
		"description": "This key is used for integration test purpose.",
	}
	return e.POST(globalAppKeysOperation("create")).
		WithHeader(Authorization, Bearer+token).WithJSON(createGlobalAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().Value("created_app_key")
}
func TestMultipleAppsCreation(t *testing.T) {
	testStartTime := time.Now()

	e2 := expectLow(t, config.GetBaseURL2())
	testProject := "TestMultipleAppsCreation2"
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)
	deleteProject2(t, testProject, token)
	createProject2(t, testProject, token).Status(http.StatusOK)

	for i := 0; i < 5; i++ {
		createdAppKey := createAppKey(e2, token, fmt.Sprintf("test_key_%d", i), testProject, "")
		require.NotNil(t, createdAppKey)
		generatedClientId := createdAppKey.Object().Value("id").String().Raw()
		generatedClientSecret := createdAppKey.Object().Value("secret").String().Raw()

		require.True(t, strings.HasPrefix(generatedClientId, auth.ClientIdPrefix))
		require.True(t, strings.HasPrefix(generatedClientSecret, auth.ClientSecretPrefix))
	}

	appKeys := e2.GET(appKeysOperation(testProject, "get")).
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
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)

	deleteProject2(t, fmt.Sprintf("%s%d", testProject, 0), token)
	deleteProject2(t, fmt.Sprintf("%s%d", testProject, 1), token)
	createProject2(t, fmt.Sprintf("%s%d", testProject, 0), token).Status(http.StatusOK)
	createProject2(t, fmt.Sprintf("%s%d", testProject, 1), token).Status(http.StatusOK)

	for i := 0; i < 5; i++ {
		projectForThisKey := fmt.Sprintf("%s%d", testProject, i%2)
		createdAppKey := createAppKey(e2, token, fmt.Sprintf("test_key_%d", i), projectForThisKey, "")
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
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)

	deleteProject2(t, testProject, token)
	createProject2(t, testProject, token).Status(http.StatusOK)

	appKeys := e2.GET(appKeysOperation(testProject, "get")).
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK).
		JSON().Object().Raw()
	require.Equal(t, make(map[string]any), appKeys)
}

func TestApiKeyUsage(t *testing.T) {
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)
	e := expectLow(t, config.GetBaseURL2())
	testProject := "TestApiKey"

	createdApiKey := createAppKey(e, token, "test_api_key", testProject, auth.AppKeyTypeApiKey)
	require.NotNil(t, createdApiKey)
	key := createdApiKey.Object().Value("id").String().Raw()
	require.Equal(t, 125, len(key)) // 120 fixed + 5 prefix

	// use the api key
	listProjectsResp := listProjects(t, key)
	listProjectsResp.Status(http.StatusOK).JSON()
}

func TestApiKeyCrud(t *testing.T) {
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)
	e := expectLow(t, config.GetBaseURL2())
	testProject := "TestApiKeyCrud"

	// create
	_ = createAppKey(e, token, "test_api_key_1", testProject, auth.AppKeyTypeApiKey)
	createdApiKey := createAppKey(e, token, "test_api_key_2", testProject, auth.AppKeyTypeApiKey)
	require.NotNil(t, createdApiKey)
	key := createdApiKey.Object().Value("id").String().Raw()
	require.Equal(t, 125, len(key)) // 120 fixed + 5 prefix

	// read
	appKeys := listAppKeys(t, e, testProject, token)
	found := false
	for i := 0; i < int(appKeys.Length().Raw()); i++ {
		k := appKeys.Element(i)
		if k.Object().Value("id").Raw() == key {
			require.False(t, found)
			found = true
			require.Equal(t, "test_api_key_2", k.Object().Value("name").String().Raw())
			require.Equal(t, auth.AppKeyTypeApiKey, k.Object().Value("key_type").String().Raw())
		}
	}
	require.True(t, found)

	// update
	updateAppKeyPayload := Map{
		"id":          key,
		"name":        "[updated] test_api_key",
		"description": "[updated]This key is used for integration test purpose.",
	}
	updateResp := e.POST(appKeysOperation(testProject, "update")).
		WithHeader(Authorization, Bearer+token).
		WithJSON(updateAppKeyPayload).
		Expect().
		Status(http.StatusOK)
	require.NotNil(t, updateResp)

	// read back post update
	appKeys = listAppKeys(t, e, testProject, token)
	found = false
	for i := 0; i < int(appKeys.Length().Raw()); i++ {
		k := appKeys.Element(i)
		if k.Object().Value("id").Raw() == key {
			require.False(t, found)
			found = true
			require.Equal(t, "[updated] test_api_key", k.Object().Value("name").String().Raw())
			require.Equal(t, "[updated]This key is used for integration test purpose.", k.Object().Value("description").String().Raw())
			require.Equal(t, auth.AppKeyTypeApiKey, k.Object().Value("key_type").String().Raw())
		}
	}
	require.True(t, found)

	// delete
	deleteAppKeyPayload := Map{
		"id": key,
	}
	deletedResponse := e.DELETE(appKeysOperation(testProject, "delete")).
		WithHeader(Authorization, Bearer+token).WithJSON(deleteAppKeyPayload).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object().
		Value("deleted").
		Boolean()
	require.True(t, deletedResponse.Raw())

	// read back
	appKeys = listAppKeys(t, e, testProject, token)
	found = false
	for i := 0; i < int(appKeys.Length().Raw()); i++ {
		k := appKeys.Element(i)
		if k.Object().Value("id").Raw() == key {
			found = true
		}
	}
	require.False(t, found)
}

func TestWhoAmI(t *testing.T) {
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)
	e := expectLow(t, config.GetBaseURL2())

	res := e.GET("/v1/observability/whoami").
		WithHeader(Authorization, Bearer+token).
		Expect().
		Status(http.StatusOK)
	resJSON := res.JSON().Object()

	require.Equal(t, "JWT", resJSON.Value("auth_method").String().Raw())
	require.Equal(t, "tigris_test", resJSON.Value("namespace").String().Raw())
	require.Equal(t, "gt|5f6812e4-6f4b-44d5-bc06-5ebe33e936ec", resJSON.Value("sub").String().Raw())
	require.Equal(t, "machine", resJSON.Value("user_type").String().Raw())

}

func TestCreateAccessToken(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	testProject := "auth_test"
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)
	createdAppKey := createAppKey(e2, token, "test_key", testProject, "")
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
	deleteProject2(t, "new-project-1", token)
	createProject2(t, "new-project-1", accessToken).Status(http.StatusOK)

	deleteProject2(t, "new-project-2", token)
	// use access token bypassing auth caches
	_ = e2.POST(getProjectURL("new-project-2", "create")).
		WithHeader(Authorization, Bearer+accessToken).
		WithHeader(api.HeaderBypassAuthCache, "true").
		Expect().
		Status(http.StatusOK)

	deleteProject2(t, "new-project-3", token)
	// use access token with cache
	createProject2(t, "new-project-3", accessToken).Status(http.StatusOK)
}

func TestCreateGlobalAccessToken(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)

	createGlobalAppKeyPayload := Map{
		"name":        "test_key",
		"description": "This key is used for integration test purpose.",
	}

	createdAppKey := e2.POST(globalAppKeysOperation("create")).
		WithHeader(Authorization, Bearer+token).WithJSON(createGlobalAppKeyPayload).
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
	createProject2(t, "TestCreateGlobalAccessToken-project-1", accessToken).Status(http.StatusOK)

	deleteProject2(t, "TestCreateGlobalAccessToken-project-1", token)

	deleteProject2(t, "TestCreateGlobalAccessToken-project-2", token) //cleanup

	// use access token bypassing auth caches
	_ = e2.POST(getProjectURL("TestCreateGlobalAccessToken-project-2", "create")).
		WithHeader(Authorization, Bearer+accessToken).
		WithHeader(api.HeaderBypassAuthCache, "true").
		Expect().
		Status(http.StatusOK)
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
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)
	authFailureErrorResponse := createProject2(t, "auth_test", "").
		Status(http.StatusUnauthorized).
		JSON().
		Object().
		Value("error").
		Object()
	require.Equal(t, "UNAUTHENTICATED", authFailureErrorResponse.Value("code").String().Raw())
	require.Equal(t, "request unauthenticated with bearer", authFailureErrorResponse.Value("message").String().Raw())

	authFailureErrorResponse = createProject2(t, "auth_test", "aaa").
		Status(http.StatusUnauthorized).
		JSON().
		Object().
		Value("error").
		Object()
	require.Equal(t, "UNAUTHENTICATED", authFailureErrorResponse.Value("code").String().Raw())
	require.Equal(t, "Failed to authenticate", authFailureErrorResponse.Value("message").String().Raw())
}

func TestUserInvitations(t *testing.T) {
	token := readToken(t, RSATokenFilePath)
	createTestNamespace(t, token)

	deleteUserInvitations(t, "a@hello.com", "PENDING", token)
	deleteUserInvitations(t, "b@hello.com", "PENDING", token)
	deleteUserInvitations(t, "c@hello.com", "PENDING", token)
	deleteUserInvitations(t, "d@hello.com", "PENDING", token)
	deleteUserInvitations(t, "b@hello.com", "ACCEPTED", token)

	createUserInvitation(t, "a@hello.com", "editor_a", "TestUserInvitations", token).
		Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		String().
		Equal(auth.CreatedStatus)
	createUserInvitation(t, "b@hello.com", "editor_b", "TestUserInvitations", token).
		Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		String().
		Equal(auth.CreatedStatus)
	createUserInvitation(t, "c@hello.com", "editor_c", "TestUserInvitations", token).
		Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		String().
		Equal(auth.CreatedStatus)
	createUserInvitation(t, "d@hello.com", "editor_c", "TestUserInvitations", token).
		Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		String().
		Equal(auth.CreatedStatus)
	createUserInvitation(t, "a@hello.com", "editor_a", "TestUserInvitations", token).
		Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		String().
		Equal(auth.CreatedStatus)

	listUserInvitations1 := listUserInvitations(t, token)
	listUserInvitations1.Status(http.StatusOK)
	invitations1 := listUserInvitations1.JSON().Object().Value("invitations").Array()
	//require.Equal(t, float64(4), invitations1.Length().Raw())

	emailCountMap1 := make(map[string]int)
	for _, value := range invitations1.Iter() {
		emailCountMap1[value.Object().Value("email").String().Raw()]++
	}

	require.Equal(t, 4, len(emailCountMap1))
	require.Equal(t, 1, emailCountMap1["a@hello.com"])
	require.Equal(t, 1, emailCountMap1["b@hello.com"])
	require.Equal(t, 1, emailCountMap1["c@hello.com"])
	require.Equal(t, 1, emailCountMap1["d@hello.com"])

	// delete invitation
	deleteUserInvitations(t, "c@hello.com", "PENDING", token).
		Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		String().
		Equal(auth.DeletedStatus)

	// list user invitations
	listUserInvitations2 := listUserInvitations(t, token)
	listUserInvitations2.Status(http.StatusOK)
	invitations2 := listUserInvitations2.JSON().Object().Value("invitations").Array()
	require.Equal(t, float64(3), invitations2.Length().Raw())

	emailCountMap2 := make(map[string]int)
	for _, value := range invitations2.Iter() {
		emailCountMap2[value.Object().Value("email").String().Raw()]++
	}
	require.Equal(t, 3, len(emailCountMap2))
	require.Equal(t, 1, emailCountMap2["a@hello.com"])
	require.Equal(t, 1, emailCountMap2["b@hello.com"])
	require.Equal(t, 1, emailCountMap2["d@hello.com"])

	// call gotrue to get the code
	invitationCode := getInvitationCode(t, "tigris_test", "b@hello.com")

	// verify - valid code
	verificationRes1 := verifyUserInvitations(t, "b@hello.com", invitationCode, token, false)
	verificationRes1.Status(http.StatusOK)
	require.Equal(t, "tigris_test", verificationRes1.JSON().Object().Value("tigris_namespace").String().Raw())
	require.Equal(t, "tigris_test_name", verificationRes1.JSON().Object().Value("tigris_namespace_name").String().Raw())

	// verify - same valid code - it should fail as the invitation is marked as accepted
	verificationRes2 := verifyUserInvitations(t, "b@hello.com", invitationCode, token, false)
	verificationRes2.Status(http.StatusUnauthorized)

	// dry verification
	invitationCodeC := getInvitationCode(t, "tigris_test", "d@hello.com")
	for i := 0; i < 5; i++ {
		// verify - valid code
		verificationRes := verifyUserInvitations(t, "d@hello.com", invitationCodeC, token, true)
		verificationRes.Status(http.StatusOK)
		require.Equal(t, "tigris_test", verificationRes1.JSON().Object().Value("tigris_namespace").String().Raw())
		require.Equal(t, "tigris_test_name", verificationRes1.JSON().Object().Value("tigris_namespace_name").String().Raw())
	}

	// verify - invalid code
	verificationRes3 := verifyUserInvitations(t, "b@hello.com", "invalid-code", token, false)
	verificationRes3.Status(http.StatusUnauthorized)
}

func TestAuthzOwner(t *testing.T) {
	token := readToken(t, OwnerTokenFilePath)
	createTestNamespace(t, token)

	deleteProject2(t, "TestAuthzOwner", token)

	// create project should be allowed
	createProject2(t, "TestAuthzOwner", token).
		Status(http.StatusOK)

	// creating namespace should NOT be allowed
	createNamespaceWithToken(t, "TestAuthzOwner", token).Status(http.StatusForbidden)
}

func TestAuthzEditor(t *testing.T) {
	token := readToken(t, EditorTokenFilePath)
	createTestNamespace(t, token)

	deleteProject2(t, "TestAuthzEditor", token)

	// create project should be allowed
	createProject2(t, "TestAuthzEditor", token).
		Status(http.StatusOK)

	// creating namespace is not be allowed
	resp := createNamespaceWithToken(t, "TestAuthzEditor", token).Status(http.StatusForbidden).Body().Raw()
	require.Equal(t, "{\"error\":{\"code\":\"PERMISSION_DENIED\",\"message\":\"You are not allowed to perform operation: /tigrisdata.management.v1.Management/CreateNamespace\"}}", resp)
}

func TestAuthzReadonly(t *testing.T) {
	// listing projects should be allowed.
	token := readToken(t, ReadOnlyTokenFilePath)
	listProjects(t, token).Status(http.StatusOK)

	// create project should NOT be allowed
	resp := createProject2(t, "TestAuthzReadonly", token).
		Status(http.StatusForbidden).Body().Raw()

	require.Equal(t, "{\"error\":{\"code\":\"PERMISSION_DENIED\",\"message\":\"You are not allowed to perform operation: /tigrisdata.v1.Tigris/CreateProject\"}}", resp)
}

func createProject2(t *testing.T, projectName string, token string) *httpexpect.Response {
	e2 := expectLow(t, config.GetBaseURL2())

	if token != "" {
		return e2.POST(getProjectURL(projectName, "create")).
			WithHeader(Authorization, Bearer+token).
			Expect()
	}

	return e2.POST(getProjectURL(projectName, "create")).Expect()
}

func deleteProject2(t *testing.T, projectName string, token string) {
	e2 := expectLow(t, config.GetBaseURL2())
	_ = e2.DELETE(getProjectURL(projectName, "delete")).
		WithHeader(Authorization, Bearer+token).
		Expect()
}

func getInvitationCode(t *testing.T, namespace string, email string) string {
	e2 := expectLow(t, config.GetGotrueURL())
	invitations := e2.GET("/invitations", namespace).WithQueryString(fmt.Sprintf("tigris_namespace=%s", namespace)).
		Expect().JSON().Array()
	for _, value := range invitations.Iter() {
		status := value.Object().Value("status").String().Raw()
		thisEmail := value.Object().Value("email").String().Raw()

		if thisEmail == email && status == "PENDING" {
			return value.Object().Value("code").String().Raw()
		}
	}
	return ""
}

func createUserInvitation(t *testing.T, email string, role string, invitationCreatedByName string, token string) *httpexpect.Response {
	e2 := expectLow(t, config.GetBaseURL2())

	invitationInfos := make([]api.InvitationInfo, 1)
	payload := make(map[string][]api.InvitationInfo)
	invitationInfos[0] = api.InvitationInfo{
		Email:                email,
		Role:                 role,
		InvitationSentByName: invitationCreatedByName,
	}
	payload["invitations"] = invitationInfos
	return e2.POST(invitationUrl("create")).
		WithJSON(payload).
		WithHeader(Authorization, Bearer+token).
		Expect()
}

func listUserInvitations(t *testing.T, token string) *httpexpect.Response {
	e2 := expectLow(t, config.GetBaseURL2())

	return e2.GET(invitationUrl("list")).
		WithHeader(Authorization, Bearer+token).
		Expect()
}

func listUsers(t *testing.T, token string) *httpexpect.Response {
	e2 := expectLow(t, config.GetBaseURL2())

	return e2.GET(listUsersUrl()).
		WithHeader(Authorization, Bearer+token).
		Expect()
}

func listProjects(t *testing.T, token string) *httpexpect.Response {
	e2 := expectLow(t, config.GetBaseURL2())
	return e2.GET(listProjectsUrl()).
		WithHeader(Authorization, Bearer+token).
		Expect()
}

func verifyUserInvitations(t *testing.T, email string, code string, token string, dry bool) *httpexpect.Response {
	e2 := expectLow(t, config.GetBaseURL2())
	payload := make(map[string]any)
	payload["email"] = email
	payload["code"] = code
	payload["dry"] = dry
	return e2.POST(invitationUrl("verify")).
		WithJSON(payload).
		WithHeader(Authorization, Bearer+token).
		Expect()
}

func deleteUserInvitations(t *testing.T, email string, status string, token string) *httpexpect.Response {
	payload := make(map[string]string)
	payload["email"] = email
	payload["status"] = status
	e2 := expectLow(t, config.GetBaseURL2())
	return e2.DELETE(invitationUrl("delete")).
		WithJSON(payload).
		WithHeader(Authorization, Bearer+token).
		Expect()
}

func invitationUrl(operation string) string {
	return fmt.Sprintf("/v1/auth/namespace/invitations/%s", operation)
}

func listUsersUrl() string {
	return "/v1/auth/namespace/users"
}

func listProjectsUrl() string {
	return "/v1/projects"
}
