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

package metrics

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/tracing"
)

type FakeError struct{}

func (*FakeError) Error() string {
	return "fake error for testing tags"
}

func TestTracing(t *testing.T) {
	t.Run("Test NewMeasurement", func(t *testing.T) {
		tags := GetGlobalTags()
		testMeasurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		assert.Equal(t, "test.service.name", testMeasurement.serviceName)
		assert.Equal(t, "TestResource", testMeasurement.resourceName)
		assert.Equal(t, "rpc", testMeasurement.spanType)
		assert.Equal(t, tags, testMeasurement.tags)
		assert.Nil(t, testMeasurement.datadogSpan)
		assert.Nil(t, testMeasurement.parent)
	})

	t.Run("Test GetSpanOptions", func(t *testing.T) {
		testMeasurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		options := testMeasurement.GetSpanOptions()
		assert.Greater(t, len(options), 2)
	})

	// TODO: Make this a test table
	t.Run("Test parent and child spans", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		config.DefaultConfig.Metrics.Enabled = true
		config.DefaultConfig.Tracing.Jaeger.Enabled = true
		config.DefaultConfig.Tracing.Datadog.Enabled = true
		closer, err := tracing.InitTracer(&config.DefaultConfig)
		if err != nil {
			assert.True(t, false)
		}
		defer closer()
		ctx := context.Background()
		parentMeasurement := NewMeasurement("parent.service", "parent.resource", "rpc", GetGlobalTags())
		ctx = parentMeasurement.StartTracing(ctx, false)
		childMeasurement := NewMeasurement("child.service", "child.resource", "rpc", GetGlobalTags())
		ctx = childMeasurement.StartTracing(ctx, true)
		assert.NotNil(t, childMeasurement.datadogSpan)
		assert.NotNil(t, childMeasurement.parent)
		assert.Equal(t, "parent.service", childMeasurement.parent.serviceName)
		grandChildMeasurement := NewMeasurement("grandchild.service", "grandchild.resource", "rpc", GetGlobalTags())
		ctx = grandChildMeasurement.StartTracing(ctx, true)
		assert.NotNil(t, grandChildMeasurement.datadogSpan)
		assert.NotNil(t, grandChildMeasurement.parent)
		assert.Equal(t, "child.service", grandChildMeasurement.parent.serviceName)
		ctx = grandChildMeasurement.FinishTracing(ctx)
		loadedChildMeasurement, ok := MeasurementFromContext(ctx)
		assert.NotNil(t, loadedChildMeasurement.datadogSpan)
		assert.True(t, ok)
		assert.Equal(t, "child.service", loadedChildMeasurement.serviceName)
		ctx = childMeasurement.FinishTracing(ctx)
		loadedParentMeasurement, ok := MeasurementFromContext(ctx)
		assert.NotNil(t, loadedParentMeasurement)
		assert.True(t, ok)
		assert.Equal(t, "parent.service", loadedParentMeasurement.serviceName)
		ctx = parentMeasurement.FinishTracing(ctx)
		loadedNilMeta, ok := MeasurementFromContext(ctx)
		assert.Nil(t, loadedNilMeta)
		assert.False(t, ok)
	})

	t.Run("Test counters", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		config.DefaultConfig.Metrics.Enabled = true
		config.DefaultConfig.Tracing.Datadog.Enabled = true
		config.DefaultConfig.Tracing.Jaeger.Enabled = true
		closer, err := tracing.InitTracer(&config.DefaultConfig)
		if err != nil {
			assert.True(t, false)
		}
		defer closer()
		InitializeMetrics()
		ctx := context.Background()
		measurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		measurement.StartTracing(ctx, false)
		measurement.FinishTracing(ctx)
		measurement.RecordDuration(RequestsRespTime, measurement.GetRequestOkTags())
		measurement.CountOkForScope(RequestsOkCount, measurement.GetRequestOkTags())
		err = fmt.Errorf("hello error")
		measurement.CountErrorForScope(RequestsErrorCount, measurement.GetRequestErrorTags(err))
	})

	t.Run("Test request tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		config.DefaultConfig.Metrics.Enabled = true
		InitializeMetrics()
		measurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		measurement.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})
		requestOkTags := measurement.GetRequestOkTags()
		for _, key := range getRequestOkTagKeys() {
			assert.NotNil(t, requestOkTags[key])
		}
		assert.Equal(t, len(getRequestOkTagKeys()), len(requestOkTags))

		requestErrorTags := measurement.GetRequestErrorTags(&FakeError{})
		for _, key := range getRequestErrorTagKeys() {
			assert.NotNil(t, requestErrorTags[key])
		}
		assert.Equal(t, len(getRequestErrorTagKeys()), len(requestErrorTags))
	})

	t.Run("Test fdb tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		config.DefaultConfig.Metrics.Enabled = true
		InitializeMetrics()
		measurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		measurement.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})
		fdbOkTags := measurement.GetFdbOkTags()
		for _, key := range getFdbOkTagKeys() {
			assert.NotNil(t, fdbOkTags[key])
		}
		assert.Equal(t, len(getFdbOkTagKeys()), len(fdbOkTags))

		fdbErrorTags := measurement.GetFdbErrorTags(&FakeError{})
		for _, key := range getFdbErrorTagKeys() {
			assert.NotNil(t, fdbErrorTags[key])
		}
		assert.Equal(t, len(getFdbErrorTagKeys()), len(fdbErrorTags))
	})

	t.Run("Test search tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		config.DefaultConfig.Metrics.Enabled = true
		InitializeMetrics()
		measurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		measurement.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})
		searchOkTags := measurement.GetSearchOkTags()
		for _, key := range getSearchOkTagKeys() {
			assert.NotNil(t, searchOkTags[key])
		}
		assert.Equal(t, len(getSearchOkTagKeys()), len(searchOkTags))

		searchErrorTags := measurement.GetSearchErrorTags(&FakeError{})
		for _, key := range getSearchErrorTagKeys() {
			assert.NotNil(t, searchErrorTags[key])
		}
		assert.Equal(t, len(getSearchErrorTagKeys()), len(searchErrorTags))
	})

	t.Run("Test session tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		config.DefaultConfig.Metrics.Enabled = true
		InitializeMetrics()
		measurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		measurement.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})
		sessionOkTags := measurement.GetSessionOkTags()
		for _, key := range getSessionOkTagKeys() {
			assert.NotNil(t, sessionOkTags[key])
		}
		assert.Equal(t, len(getSessionOkTagKeys()), len(sessionOkTags))

		sessionErrorTags := measurement.GetSessionErrorTags(&FakeError{})
		for _, key := range getSessionErrorTagKeys() {
			assert.NotNil(t, sessionErrorTags[key])
		}
		assert.Equal(t, len(getSessionErrorTagKeys()), len(sessionErrorTags))
	})

	t.Run("Test size tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		config.DefaultConfig.Metrics.Enabled = true
		InitializeMetrics()
		measurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		measurement.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})

		namespaceSizeTags := measurement.GetNamespaceSizeTags()
		for _, key := range getNameSpaceSizeTagKeys() {
			assert.NotNil(t, namespaceSizeTags[key])
		}
		assert.Equal(t, len(getNameSpaceSizeTagKeys()), len(namespaceSizeTags))

		dbSizeTags := measurement.GetDbSizeTags()
		for _, key := range getDbSizeTagKeys() {
			assert.NotNil(t, dbSizeTags[key])
		}
		assert.Equal(t, len(getDbSizeTagKeys()), len(dbSizeTags))

		collSizeTags := measurement.GetCollectionSizeTags()
		for _, key := range getCollectionSizeTagKeys() {
			assert.NotNil(t, collSizeTags[key])
		}
		assert.Equal(t, len(getCollectionSizeTagKeys()), len(collSizeTags))

		searchSizeTags := measurement.GetSearchSizeTagKeys()
		for _, key := range getSearchSizeTagKeys() {
			assert.NotNil(t, searchSizeTags[key])
		}
		assert.Equal(t, len(getSearchSizeTagKeys()), len(searchSizeTags))

		searchIndexSizeTags := measurement.GetSearchIndexSizeTagKeys()
		for _, key := range getSearchIndexSizeTagKeys() {
			assert.NotNil(t, searchIndexSizeTags[key])
		}
		assert.Equal(t, len(getSearchIndexSizeTagKeys()), len(searchIndexSizeTags))
	})

	t.Run("Test network tags", func(t *testing.T) {
		config.DefaultConfig.Tracing.Enabled = true
		config.DefaultConfig.Metrics.Enabled = true
		InitializeMetrics()
		measurement := NewMeasurement("test.service.name", "TestResource", "rpc", GetGlobalTags())
		measurement.AddTags(map[string]string{
			"brokentag": "brokenvalue",
		})

		networkTags := measurement.GetNetworkTags()
		for _, key := range getNetworkTagKeys() {
			assert.NotNil(t, networkTags[key])
		}
		assert.Equal(t, len(getNetworkTagKeys()), len(networkTags))
	})
}
