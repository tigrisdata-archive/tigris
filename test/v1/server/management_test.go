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

package server

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

type AdminTestMap map[string]interface{}

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
		if converted, ok := namespace.(map[string]interface{}); ok {
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
	createdNamespaceMap := createdNamespace.(map[string]interface{})
	assert.Equal(t, displayName, createdNamespaceMap["name"])
	assert.Equal(t, previousMaxId+1, createdNamespaceMap["code"])
}

func adminExpect(s httpexpect.LoggerReporter) *httpexpect.Expect {
	return httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  config.GetBaseURL(),
		Reporter: httpexpect.NewAssertReporter(s),
	})
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
		if converted, ok := namespace.(map[string]interface{}); ok {
			if converted["name"] == name {
				found = true
			}
		}
	}
	assert.True(t, found)
}

func TestApplications(t *testing.T) {
	errMsg := "{\"error\":{\"code\":\"INTERNAL\",\"message\":\"authentication not enabled on this server\"}}"

	e := adminExpect(t)
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
	e := adminExpect(t)
	return e.POST(getCreateNamespaceURL()).
		WithJSON(AdminTestMap{"name": name}).
		Expect()
}

func listNamespaces(t *testing.T) *httpexpect.Response {
	e := adminExpect(t)
	return e.POST(listNamespaceUrl()).
		WithJSON(AdminTestMap{}).
		Expect()
}

func getCreateNamespaceURL() string {
	return "/v1/management/namespaces/create"
}

func listNamespaceUrl() string {
	return "/v1/management/namespaces/list"
}
