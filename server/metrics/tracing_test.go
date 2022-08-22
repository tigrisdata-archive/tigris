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
		spanMeta := NewSpanMeta("test.service.name", "TestResource", "rpc", GetGlobalTags())
		defer RequestsRespTime.Tagged(spanMeta.GetTags()).Timer("time").Start().Stop()
		spanMeta.CountOkForScope(OkRequests)
		err := fmt.Errorf("hello error")
		spanMeta.CountErrorForScope(ErrorRequests, err)
	})
}
