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

package server

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
			if converted["id"].(float64) > previousMaxId {
				previousMaxId = converted["id"].(float64)
			}
		}
	}

	displayName := fmt.Sprintf("namespace-a-%x", rand.Int63()) //nolint:golint,gosec
	createResp := createNamespace(t, displayName)
	createRespMsg := createResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("message").String().Raw()
	assert.True(t, strings.HasPrefix(createRespMsg, fmt.Sprintf("Namespace created, with id=%d, and name=", (uint32)(previousMaxId+1))))

	createdNamespace := createResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("namespace").Raw()
	createdNamespaceMap := createdNamespace.(map[string]interface{})
	assert.Equal(t, displayName, createdNamespaceMap["display_name"])
	assert.Equal(t, previousMaxId+1, createdNamespaceMap["id"])
}

func adminExpect(s httpexpect.LoggerReporter) *httpexpect.Expect {
	return httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  config.GetBaseURL(),
		Reporter: httpexpect.NewAssertReporter(s),
	})
}

func TestListNamespaces(t *testing.T) {
	displayName := fmt.Sprintf("namespace-b-%x", rand.Int63()) //nolint:golint,gosec
	_ = createNamespace(t, displayName)
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
			if converted["display_name"] == displayName {
				found = true
			}
		}
	}
	assert.True(t, found)
}

func createNamespace(t *testing.T, displayName string) *httpexpect.Response {
	e := adminExpect(t)
	return e.POST(getCreateNamespaceURL(displayName)).
		WithJSON(AdminTestMap{}).
		Expect()
}

func listNamespaces(t *testing.T) *httpexpect.Response {
	e := adminExpect(t)
	return e.POST(listNamespaceUrl()).
		WithJSON(AdminTestMap{}).
		Expect()
}

func getCreateNamespaceURL(displayName string) string {
	return fmt.Sprintf("/v1/management/namespaces/%s/create", displayName)
}

func listNamespaceUrl() string {
	return "/v1/management/namespaces/list"
}
