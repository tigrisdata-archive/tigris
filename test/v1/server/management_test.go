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
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/gavv/httpexpect.v1"
)

func TestCreateNamespace(t *testing.T) {
	listResp := listNamespaces(t)
	namespaces := listResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("namespaces").
		Array().
		Raw()
	// JSON number maps to float64 in Go
	var previousMaxId float64 = 0
	for _, namespace := range namespaces {
		if converted, ok := namespace.(map[string]any); ok {
			if converted["code"].(float64) > previousMaxId {
				previousMaxId = converted["code"].(float64)
			}
		}
	}

	displayName := fmt.Sprintf("namespace-a-%x", rand.Int63()) //nolint:gosec
	createResp := createNamespace(t, displayName)
	createRespMsg := createResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("message").String().Raw()
	assert.True(t, strings.HasPrefix(createRespMsg, fmt.Sprintf("Namespace created, with code=%d, and id=", (uint32)(previousMaxId+1))))

	createdNamespace := createResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("namespace").Raw()
	require.NotNil(t, createdNamespace)
	createdNamespaceMap := createdNamespace.(map[string]any)
	assert.Equal(t, displayName, createdNamespaceMap["name"])
	assert.Equal(t, previousMaxId+1, createdNamespaceMap["code"])
}

func TestListNamespaces(t *testing.T) {
	name := fmt.Sprintf("namespace-b-%x", rand.Int63()) //nolint:gosec
	_ = createNamespace(t, name)
	resp := listNamespaces(t)
	namespaces := resp.Status(http.StatusOK).
		JSON().
		Object().
		Value("namespaces").
		Array().
		Raw()
	found := false
	for _, namespace := range namespaces {
		if converted, ok := namespace.(map[string]any); ok {
			if converted["name"] == name {
				found = true
			}
		}
	}
	assert.True(t, found)
}

func TestGetSingleNamespace(t *testing.T) {
	name := fmt.Sprintf("namespace-b-%x", rand.Int63())         //nolint:gosec
	namespaceId := fmt.Sprintf("namespace-id-%x", rand.Int63()) //nolint:gosec

	_ = createNamespaceWithId(t, namespaceId, name)
	resp := getSingleNamespace(t, namespaceId)
	namespaces := resp.Status(http.StatusOK).
		JSON().
		Object().
		Value("namespaces").
		Array()
	assert.Equal(t, float64(1), namespaces.Length().Raw())

	namespace := namespaces.Element(0).Object()
	assert.Equal(t, namespaceId, namespace.Value("id").String().Raw())
	assert.Equal(t, name, namespace.Value("name").String().Raw())
	assert.True(t, namespace.Value("code").Number().Raw() > 1)
}

func TestApplications(t *testing.T) {
	errMsg := "{\"error\":{\"code\":\"INTERNAL\",\"message\":\"authentication not enabled on this server\"}}"

	e := expect(t)
	for _, api := range []string{
		"/v1/projects/p1/apps/keys/create",
		"/v1/projects/p1/apps/keys/update",
		"/v1/projects/p1/apps/keys/delete",
		"/v1/projects/p1/apps/keys/rotate",
	} {
		if strings.Contains(api, "delete") {
			e.DELETE(api).Expect().Status(http.StatusInternalServerError).
				Body().Equal(errMsg)
		} else {
			e.POST(api).Expect().Status(http.StatusInternalServerError).
				Body().Equal(errMsg)
		}
	}

	e.GET("/v1/projects/p1/apps/keys").Expect().Status(http.StatusInternalServerError).
		Body().Equal(errMsg)
}

func createNamespace(t *testing.T, name string) *httpexpect.Response {
	e := expect(t)
	return e.POST(getCreateNamespaceURL()).
		WithJSON(Map{"name": name}).
		Expect()
}

func createNamespaceWithId(t *testing.T, id string, name string) *httpexpect.Response {
	e := expect(t)
	return e.POST(getCreateNamespaceURL()).
		WithJSON(Map{"id": id, "name": name}).
		Expect()
}

func listNamespaces(t *testing.T) *httpexpect.Response {
	e := expect(t)
	return e.GET(listNamespaceUrl()).
		WithJSON(Map{}).
		Expect()
}

func getSingleNamespace(t *testing.T, namespaceId string) *httpexpect.Response {
	e := expect(t)
	return e.GET(namespaceInfoUrl(namespaceId)).
		Expect()
}

func getCreateNamespaceURL() string {
	return "/v1/management/namespaces/create"
}

func listNamespaceUrl() string {
	return "/v1/management/namespaces"
}

func namespaceInfoUrl(namespaceId string) string {
	return fmt.Sprintf("/v1/management/namespaces/%s", namespaceId)
}
func userMetaRequest(t *testing.T, token string, op string, key string, m Map) *httpexpect.Response {
	e2 := expectAuthLow(t)
	return e2.POST(getUserMetaURL(op, key)).
		WithHeader(Authorization, Bearer+token).
		WithJSON(m).
		Expect()
}

func nsMetaRequest(t *testing.T, token string, op string, key string, m Map) *httpexpect.Response {
	e2 := expectAuthLow(t)
	return e2.POST(getNSMetaURL(op, key)).
		WithHeader(Authorization, Bearer+token).
		WithJSON(m).
		Expect()
}

func getUserMetaURL(op string, key string) string {
	return fmt.Sprintf("/v1/management/users/metadata/%s/%s", key, op)
}

func getNSMetaURL(op string, key string) string {
	return fmt.Sprintf("/v1/management/namespace/metadata/%s/%s", key, op)
}

func TestUserMetadata(t *testing.T) {
	token := readToken(t, RSATokenFilePath)

	e2 := expectAuthLow(t)
	_ = e2.POST(getCreateNamespaceURL()).
		WithHeader(Authorization, Bearer+token).
		WithJSON(Map{"name": "tigris_test", "id": "tigris_test"}).
		Expect() // 200 or 409

	testKey := fmt.Sprintf("test_key_%x", rand.Int63()) //nolint:gosec

	resp := userMetaRequest(t, token, "insert", testKey, Map{"value": "value1"})
	resp.Status(http.StatusOK)

	resp = userMetaRequest(t, token, "insert", testKey, Map{"value": "value2"})
	resp.Status(http.StatusInternalServerError)

	resp = userMetaRequest(t, token, "get", testKey, nil)
	resp.Status(http.StatusOK)
	resp.JSON().Object().Value("metadataKey").Equal(testKey)
	resp.JSON().Object().Value("value").Equal("value1")

	resp = userMetaRequest(t, token, "update", testKey, Map{"value": "value3"})
	resp.Status(http.StatusOK)

	resp = userMetaRequest(t, token, "get", testKey, nil)
	resp.Status(http.StatusOK)
	resp.JSON().Object().Value("metadataKey").Equal(testKey)
	resp.JSON().Object().Value("value").Equal("value3")

	resp = userMetaRequest(t, token, "get", "non_existent_key", nil)
	resp.Status(http.StatusOK)
}

func TestNamespaceMetadata(t *testing.T) {
	token := readToken(t, RSATokenFilePath)

	e2 := expectAuthLow(t)
	_ = e2.POST(getCreateNamespaceURL()).
		WithHeader(Authorization, Bearer+token).
		WithJSON(Map{"name": "tigris_test", "id": "tigris_test"}).
		Expect() // 200 or 409

	testKey := fmt.Sprintf("test_key_%x", rand.Int63()) //nolint:gosec

	resp := nsMetaRequest(t, token, "insert", testKey, Map{"value": "value1"})
	resp.Status(http.StatusOK)

	resp = nsMetaRequest(t, token, "insert", testKey, Map{"value": "value2"})
	resp.Status(http.StatusInternalServerError)

	resp = nsMetaRequest(t, token, "get", testKey, nil)
	resp.Status(http.StatusOK)
	resp.JSON().Object().Value("metadataKey").Equal(testKey)
	resp.JSON().Object().Value("value").Equal("value1")

	resp = nsMetaRequest(t, token, "update", testKey, Map{"value": "value3"})
	resp.Status(http.StatusOK)

	resp = nsMetaRequest(t, token, "get", testKey, nil)
	resp.Status(http.StatusOK)
	resp.JSON().Object().Value("metadataKey").Equal(testKey)
	resp.JSON().Object().Value("value").Equal("value3")

	resp = nsMetaRequest(t, token, "get", "non_existent_key", nil)
	resp.Status(http.StatusOK)
}
