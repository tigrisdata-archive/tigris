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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/server/services/v1/cache"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

type CacheTestMap map[string]interface{}

func TestCreateCache(t *testing.T) {
	project := setupTestsOnlyProject(t)

	createCacheResp := createCache(t, project, "c1")
	status := createCacheResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		Raw()

	assert.Equal(t, cache.CreatedStatus, status)

	createCacheAgainResp := createCache(t, project, "c1")
	code := createCacheAgainResp.Status(http.StatusConflict).
		JSON().
		Object().
		Value("error").
		Object().
		Value("code").
		Raw()

	assert.Equal(t, "ALREADY_EXISTS", code)
}

func TestDeleteCache(t *testing.T) {
	project := setupTestsOnlyProject(t)
	createCache(t, project, "c1")

	deleteCacheResp := deleteCache(t, project, "c1")
	status := deleteCacheResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		Raw()

	assert.Equal(t, cache.DeletedStatus, status)

	deleteCacheAgainResp := deleteCache(t, project, "c1")
	code := deleteCacheAgainResp.Status(http.StatusNotFound).
		JSON().
		Object().
		Value("error").
		Object().
		Value("code").
		Raw()

	assert.Equal(t, "NOT_FOUND", code)
}

func TestListCaches(t *testing.T) {
	project := setupTestsOnlyProject(t)
	for i := 1; i <= 5; i++ {
		cacheName := fmt.Sprintf("cache_%d", i)
		createCache(t, project, cacheName)
	}

	listCachesResp := listCaches(t, project)
	caches := listCachesResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("caches").
		Array()

	assert.Equal(t, 5, len(caches.Raw()))
	for i := 1; i <= 5; i++ {
		cacheName := fmt.Sprintf("cache_%d", i)
		var contains = false
		for _, c := range caches.Iter() {
			if c.Object().Value("name").Raw() == cacheName {
				if contains {
					assert.Failf(t, "Cache name %s is present more than once", cacheName)
				}
				contains = true
				break
			}
		}
		assert.Truef(t, contains, "Cache name: %s is found in list caches response", cacheName)
	}
}

func TestCacheCRUD(t *testing.T) {
	project := setupTestsOnlyProject(t)
	cacheName := "c1"
	createCache(t, project, cacheName)

	// set 5 keys
	for i := 1; i <= 5; i++ {
		setResp := setCacheKey(t, project, cacheName, fmt.Sprintf("k%d", i), fmt.Sprintf("value-%d", i))
		status := setResp.Status(http.StatusOK).
			JSON().
			Object().
			Value("status").
			Raw()
		assert.Equal(t, cache.SetStatus, status)
	}

	// get 5 keys
	for i := 1; i <= 5; i++ {
		getCacheKeyResp := getCacheKey(t, project, cacheName, fmt.Sprintf("k%d", i))
		value := getCacheKeyResp.Status(http.StatusOK).
			JSON().
			Object().
			Value("value").
			Raw()
		assert.Equal(t, fmt.Sprintf("value-%d", i), value)
	}

	// list all keys
	listKeysResp := listCacheKeys(t, project, cacheName)
	keys := listKeysResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("keys").
		Array().
		Raw()
	assert.Equal(t, 5, len(keys))

	// delete a key
	delKeyResp := delCacheKey(t, project, cacheName, "k1")
	delKeyRespStatus := delKeyResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		Raw()
	assert.Equal(t, cache.DeletedStatus, delKeyRespStatus)

	// list all keys
	listKeysResp2 := listCacheKeys(t, project, cacheName)
	keys2 := listKeysResp2.Status(http.StatusOK).
		JSON().
		Object().
		Value("keys").
		Array().
		Raw()
	assert.Equal(t, 4, len(keys2))

	// delete already deleted key
	delKeyResp2 := delCacheKey(t, project, cacheName, "k1")
	delKeyRespMessage2 := delKeyResp2.Status(http.StatusOK).
		JSON().
		Object().
		Value("message").
		Raw()
	assert.Equal(t, "Entries deleted count# 0", delKeyRespMessage2)

	// read a deleted key
	delKeyResp3 := getCacheKey(t, project, cacheName, "k1")
	delKeyRespCode3 := delKeyResp3.Status(http.StatusNotFound).
		JSON().
		Object().
		Value("error").
		Object().
		Value("code").
		Raw()
	assert.Equal(t, "NOT_FOUND", delKeyRespCode3)
}

func TestCacheSetWithGet(t *testing.T) {
	project := setupTestsOnlyProject(t)
	cacheName := "c1"
	createCache(t, project, cacheName)

	getSetResp := GetSetCacheKey(t, project, cacheName, "k1", "v1")
	status := getSetResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		Raw()
	assert.Equal(t, cache.SetStatus, status)

	getSetResp1 := GetSetCacheKey(t, project, cacheName, "k1", "v2")
	oldValue := getSetResp1.Status(http.StatusOK).
		JSON().
		Object().
		Value("old_value").
		Raw()
	assert.Equal(t, "v1", oldValue)

	getSetResp2 := GetSetCacheKey(t, project, cacheName, "k1", "v3")
	oldValue1 := getSetResp2.Status(http.StatusOK).
		JSON().
		Object().
		Value("old_value").
		Raw()
	assert.Equal(t, "v2", oldValue1)
}

func setCacheKey(t *testing.T, project string, cache string, key string, value string) *httpexpect.Response {
	e := cacheExpect(t)
	return e.POST(cacheKVOperationURL(project, cache, key, "set")).
		WithJSON(CacheTestMap{
			"value": value,
		}).
		Expect()
}

func GetSetCacheKey(t *testing.T, project string, cache string, key string, value string) *httpexpect.Response {
	e := cacheExpect(t)
	return e.POST(cacheKVOperationURL(project, cache, key, "getset")).
		WithJSON(CacheTestMap{
			"value": value,
		}).
		Expect()
}

func getCacheKey(t *testing.T, project string, cache string, key string) *httpexpect.Response {
	e := cacheExpect(t)
	return e.GET(cacheKVOperationURL(project, cache, key, "get")).
		Expect()
}

func delCacheKey(t *testing.T, project string, cache string, key string) *httpexpect.Response {
	e := cacheExpect(t)
	return e.DELETE(cacheKVOperationURL(project, cache, key, "delete")).
		Expect()
}

func listCacheKeys(t *testing.T, project string, cache string) *httpexpect.Response {
	e := cacheExpect(t)
	return e.GET(fmt.Sprintf("/v1/projects/%s/caches/%s/kv/keys", project, cache)).
		Expect()
}

func createCache(t *testing.T, project string, cache string) *httpexpect.Response {
	e := cacheExpect(t)
	return e.POST(cacheOperationURL(project, cache, "create")).
		WithJSON(CacheTestMap{}).
		Expect()
}

func listCaches(t *testing.T, project string) *httpexpect.Response {
	e := cacheExpect(t)
	return e.GET(fmt.Sprintf("/v1/projects/%s/caches/list", project)).
		Expect()
}

func deleteCache(t *testing.T, project string, cache string) *httpexpect.Response {
	e := cacheExpect(t)
	return e.DELETE(cacheOperationURL(project, cache, "delete")).
		WithJSON(CacheTestMap{}).
		Expect()
}

func cacheExpect(s httpexpect.LoggerReporter) *httpexpect.Expect {
	return httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  config.GetBaseURL(),
		Reporter: httpexpect.NewAssertReporter(s),
	})
}

func cacheOperationURL(project string, cache string, operation string) string {
	return fmt.Sprintf("/v1/projects/%s/caches/%s/%s", project, cache, operation)
}

func cacheKVOperationURL(project string, cache string, key string, operation string) string {
	return fmt.Sprintf("/v1/projects/%s/caches/%s/kv/%s/%s", project, cache, key, operation)
}
