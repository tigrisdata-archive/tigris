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

package metrics

import (
	"context"
	"fmt"
	"testing"

	"github.com/tigrisdata/tigris/server/config"

	"github.com/stretchr/testify/assert"
)

type FakeError struct{}

func (f *FakeError) Error() string {
	return "fake error for testing tags"
}

func TestTracing(t *testing.T) {
	t.Run("Test NewSpanMeta", func(t *testing.T) {
		tags := GetGlobalTags()
		testSpanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		assert.Equal(t, "test.service.name", testSpanMeta.serviceName)
		assert.Equal(t, "TestResource", testSpanMeta.resourceName)
		assert.Equal(t, "rpc", testSpanMeta.spanType)
		assert.Equal(t, tags, testSpanMeta.tags)
		assert.Nil(t, testSpanMeta.span)
		assert.Nil(t, testSpanMeta.parent)
	})

	t.Run("Test GetSpanOptions", func(t *testing.T) {
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		options := spanMeta.GetSpanOptions()
		assert.Greater(t, len(options), 2)
	})

	t.Run("Test parent and child spans", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		ctx := context.Background()
		parentSpanMeta := NewSpanMeta("parent.service", "parent.resource", "rpc", GetGlobalTags())
		ctx = parentSpanMeta.StartTracing(ctx, false)
		childSpanMeta := NewSpanMeta("child.service", "child.resource", "rpc", GetGlobalTags())
		ctx = childSpanMeta.StartTracing(ctx, true)
		assert.NotNil(t, childSpanMeta.span)
		assert.NotNil(t, childSpanMeta.parent)
		assert.Equal(t, "parent.service", childSpanMeta.parent.serviceName)
		grandChildSpanMeta := NewSpanMeta("grandchild.service", "grandchild.resource", "rpc", GetGlobalTags())
		ctx = grandChildSpanMeta.StartTracing(ctx, true)
		assert.NotNil(t, grandChildSpanMeta.span)
		assert.NotNil(t, grandChildSpanMeta.parent)
		assert.Equal(t, "child.service", grandChildSpanMeta.parent.serviceName)
		ctx = grandChildSpanMeta.FinishTracing(ctx)
		loadedChildSpanMeta, ok := SpanMetaFromContext(ctx)
		assert.NotNil(t, loadedChildSpanMeta.span)
		assert.True(t, ok)
		assert.Equal(t, "child.service", loadedChildSpanMeta.serviceName)
		ctx = childSpanMeta.FinishTracing(ctx)
		loadedParentSpanMeta, ok := SpanMetaFromContext(ctx)
		assert.NotNil(t, loadedParentSpanMeta)
		assert.True(t, ok)
		assert.Equal(t, "parent.service", loadedParentSpanMeta.serviceName)
		ctx = parentSpanMeta.FinishTracing(ctx)
		loadedNilMeta, ok := SpanMetaFromContext(ctx)
		assert.Nil(t, loadedNilMeta)
		assert.False(t, ok)
	})

	t.Run("Test counters", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		InitializeMetrics()
		ctx := context.Background()
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		spanMeta.StartTracing(ctx, false)
		spanMeta.FinishTracing(ctx)
		spanMeta.RecordDuration(RequestsRespTime, spanMeta.GetRequestOkTags())
		spanMeta.CountOkForScope(OkRequests, spanMeta.GetRequestOkTags())
		err := fmt.Errorf("hello error")
		spanMeta.CountErrorForScope(ErrorRequests, spanMeta.GetRequestErrorTags(err))
	})

	t.Run("Test request tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		InitializeMetrics()
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		spanMeta.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})
		requestOkTags := spanMeta.GetRequestOkTags()
		for _, key := range getRequestOkTagKeys() {
			assert.NotNil(t, requestOkTags[key])
		}
		assert.Equal(t, len(getRequestOkTagKeys()), len(requestOkTags))

		requestTimerTags := spanMeta.GetRequestTimerTags()
		for _, key := range getRequestTimerTagKeys() {
			assert.NotNil(t, requestTimerTags[key])
		}
		assert.Equal(t, len(getRequestTimerTagKeys()), len(requestOkTags))

		requestErrorTags := spanMeta.GetRequestErrorTags(&FakeError{})
		for _, key := range getRequestErrorTagKeys() {
			assert.NotNil(t, requestErrorTags[key])
		}
		assert.Equal(t, len(getRequestErrorTagKeys()), len(requestErrorTags))
	})

	t.Run("Test fdb tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		InitializeMetrics()
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		spanMeta.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})
		fdbOkTags := spanMeta.GetFdbOkTags()
		for _, key := range getFdbOkTagKeys() {
			assert.NotNil(t, fdbOkTags[key])
		}
		assert.Equal(t, len(getFdbOkTagKeys()), len(fdbOkTags))

		fdbTimerTags := spanMeta.GetFdbTimerTags()
		for _, key := range getFdbTimerTagKeys() {
			assert.NotNil(t, fdbTimerTags[key])
		}
		assert.Equal(t, len(getFdbTimerTagKeys()), len(fdbTimerTags))

		fdbErrorTags := spanMeta.GetFdbErrorTags(&FakeError{})
		for _, key := range getFdbErrorTagKeys() {
			assert.NotNil(t, fdbErrorTags[key])
		}
		assert.Equal(t, len(getFdbErrorTagKeys()), len(fdbErrorTags))
	})

	t.Run("Test search tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		InitializeMetrics()
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		spanMeta.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})
		searchOkTags := spanMeta.GetSearchOkTags()
		for _, key := range getSearchOkTagKeys() {
			assert.NotNil(t, searchOkTags[key])
		}
		assert.Equal(t, len(getSearchOkTagKeys()), len(searchOkTags))

		searchTimerTags := spanMeta.GetSearchTimerTags()
		for _, key := range getSearchTimerTagKeys() {
			assert.NotNil(t, searchTimerTags[key])
		}
		assert.Equal(t, len(getSearchTimerTagKeys()), len(searchTimerTags))

		searchErrorTags := spanMeta.GetSearchErrorTags(&FakeError{})
		for _, key := range getSearchErrorTagKeys() {
			assert.NotNil(t, searchErrorTags[key])
		}
		assert.Equal(t, len(getSearchErrorTagKeys()), len(searchErrorTags))
	})

	t.Run("Test session tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		InitializeMetrics()
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		spanMeta.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})
		sessionOkTags := spanMeta.GetSessionOkTags()
		for _, key := range getSessionOkTagKeys() {
			assert.NotNil(t, sessionOkTags[key])
		}
		assert.Equal(t, len(getSessionOkTagKeys()), len(sessionOkTags))

		sessionTimerTags := spanMeta.GetSessionTimerTags()
		for _, key := range getSessionTimerTagKeys() {
			assert.NotNil(t, sessionTimerTags[key])
		}
		assert.Equal(t, len(getSessionTimerTagKeys()), len(sessionTimerTags))

		sessionErrorTags := spanMeta.GetSessionErrorTags(&FakeError{})
		for _, key := range getSessionErrorTagKeys() {
			assert.NotNil(t, sessionErrorTags[key])
		}
		assert.Equal(t, len(getSessionErrorTagKeys()), len(sessionErrorTags))
	})

	t.Run("Test size tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		InitializeMetrics()
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		spanMeta.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})

		namespaceSizeTags := spanMeta.GetNamespaceSizeTags()
		for _, key := range getNameSpaceSizeTagKeys() {
			assert.NotNil(t, namespaceSizeTags[key])
		}
		assert.Equal(t, len(getNameSpaceSizeTagKeys()), len(namespaceSizeTags))

		dbSizeTags := spanMeta.GetDbSizeTags()
		for _, key := range getDbSizeTagKeys() {
			assert.NotNil(t, dbSizeTags[key])
		}
		assert.Equal(t, len(getDbSizeTagKeys()), len(dbSizeTags))

		collSizeTags := spanMeta.GetCollectionSizeTags()
		for _, key := range getCollectionSizeTagKeys() {
			assert.NotNil(t, collSizeTags[key])
		}
		assert.Equal(t, len(getCollectionSizeTagKeys()), len(collSizeTags))
	})

	t.Run("Test network tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		InitializeMetrics()
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		spanMeta.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})

		networkTags := spanMeta.GetNetworkTags()
		for _, key := range getNetworkTagKeys() {
			assert.NotNil(t, networkTags[key])
		}
		assert.Equal(t, len(getNetworkTagKeys()), len(networkTags))
	})
}
