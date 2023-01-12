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
	"bytes"
	"fmt"
	"net/http"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/lib/json"
	"github.com/tigrisdata/tigris/server/services/v1/cache"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

type CacheTestMap map[string]interface{}

// getCacheName generates per test cache name
func getCacheName(t *testing.T) string {
	return fmt.Sprintf("%s", t.Name())
}

func TestCreateCache(t *testing.T) {
	project := setupTestsOnlyProject(t)
	cn := getCacheName(t)

	createCacheResp := createCache(t, project, cn)
	status := createCacheResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		Raw()

	assert.Equal(t, cache.CreatedStatus, status)

	createCacheAgainResp := createCache(t, project, cn)
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
	cn := getCacheName(t)

	createCache(t, project, cn)

	deleteCacheResp := deleteCache(t, project, cn)
	status := deleteCacheResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		Raw()

	assert.Equal(t, cache.DeletedStatus, status)

	deleteCacheAgainResp := deleteCache(t, project, cn)
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
	cacheName := getCacheName(t)
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
	listKeysResp := listCacheKeys(t, project, cacheName, 0)
	bb := listKeysResp.Status(http.StatusOK).Body().Raw()
	dd := json.NewDecoder(bytes.NewBufferString(bb))
	i := 0
	for dd.More() {
		var r struct {
			Result struct {
				Keys []string
			}
		}
		err := dd.Decode(&r)
		require.NoError(t, err)

		i += len(r.Result.Keys)
	}
	assert.Equal(t, 5, i)

	// delete a key
	delKeyResp := delCacheKey(t, project, cacheName, "k1")
	delKeyRespStatus := delKeyResp.Status(http.StatusOK).
		JSON().
		Object().
		Value("status").
		Raw()
	assert.Equal(t, cache.DeletedStatus, delKeyRespStatus)

	// list all keys
	listKeysResp2 := listCacheKeys(t, project, cacheName, 0)
	bb = listKeysResp2.Status(http.StatusOK).Body().Raw()
	dd = json.NewDecoder(bytes.NewBufferString(bb))
	i = 0
	for dd.More() {
		var r struct {
			Result struct {
				Keys []string
			}
		}
		err := dd.Decode(&r)
		require.NoError(t, err)

		i += len(r.Result.Keys)
	}
	assert.Equal(t, 4, i)

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

func TestCacheKeysScan(t *testing.T) {
	project := setupTestsOnlyProject(t)
	cacheName := getCacheName(t)
	createCache(t, project, cacheName)
	// set 501 kvs
	for i := 0; i < 501; i++ {
		setResp := setCacheKey(t, project, cacheName, fmt.Sprintf("k%d", i), fmt.Sprintf("value-%d", i))
		status := setResp.Status(http.StatusOK).
			JSON().
			Object().
			Value("status").
			Raw()
		assert.Equal(t, cache.SetStatus, status)
	}
	var allKeys []string

	respLines := listCacheKeysAndRead(t, project, cacheName)
	for _, respLine := range respLines {
		var doc map[string]jsoniter.RawMessage
		require.NoError(t, jsoniter.Unmarshal(respLine["result"], &doc))

		var thisBatchKeys []string
		_ = jsoniter.Unmarshal(doc["keys"], &thisBatchKeys)
		allKeys = append(allKeys, thisBatchKeys...)
	}

	assert.Equal(t, 501, len(allKeys))
	for i := 0; i < 501; i++ {
		keyToSearch := fmt.Sprintf("k%d", i)
		var contains = false
		for _, key := range allKeys {
			if key == keyToSearch {
				contains = true
				break
			}
		}
		require.Truef(t, contains, "key %s not found", keyToSearch)
	}
}

func TestCacheSetWithGet(t *testing.T) {
	project := setupTestsOnlyProject(t)
	cacheName := getCacheName(t)
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

func listCacheKeys(t *testing.T, project string, cache string, cursor int64) *httpexpect.Response {
	e := cacheExpect(t)
	return e.GET(fmt.Sprintf("/v1/projects/%s/caches/%s/keys", project, cache)).WithQuery("cursor", cursor).
		Expect()
}

func listCacheKeysAndRead(t *testing.T, project string, cache string) []map[string]jsoniter.RawMessage {
	e := cacheExpect(t)

	str := e.GET(fmt.Sprintf("/v1/projects/%s/caches/%s/keys", project, cache)).
		Expect().
		Status(http.StatusOK).
		Body().
		Raw()

	var resp []map[string]jsoniter.RawMessage
	dec := json.NewDecoder(bytes.NewReader([]byte(str)))
	for dec.More() {
		var mp map[string]jsoniter.RawMessage
		require.NoError(t, dec.Decode(&mp))
		resp = append(resp, mp)
	}

	return resp
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
	return fmt.Sprintf("/v1/projects/%s/caches/%s/%s/%s", project, cache, key, operation)
}
