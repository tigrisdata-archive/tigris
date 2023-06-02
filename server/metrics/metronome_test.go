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
	"testing"
)

func TestMetronomeMetrics(t *testing.T) {
	InitializeMetrics()
	t.Run("Test measuring metronome ok requests", func(t *testing.T) {
		InitializeMetrics()
		ctx := context.TODO()
		op := "test_operation"
		me := NewMeasurement(MetronomeServiceName, op, MetronomeSpanType, GetMetronomeBaseTags(op))
		me.StartTracing(ctx, false)
		me.AddTags(GetMetronomeResponseCodeTags(200))
		me.FinishTracing(ctx)
		me.CountOkForScope(MetronomeRequestOk, me.GetMetronomeTags())
		me.RecordDuration(MetronomeResponseTime, me.GetMetronomeTags())
	})

	t.Run("Test measuring metronome error requests", func(t *testing.T) {
		InitializeMetrics()
		ctx := context.TODO()
		op := "test_operation"
		me := NewMeasurement(MetronomeServiceName, op, MetronomeSpanType, GetMetronomeBaseTags(op))
		me.StartTracing(ctx, false)
		me.AddTags(GetMetronomeResponseCodeTags(500))
		me.FinishTracing(ctx)
		me.CountErrorForScope(MetronomeRequestError, me.GetMetronomeTags())
		me.RecordDuration(MetronomeErrorResponseTime, me.GetMetronomeTags())
	})
}
