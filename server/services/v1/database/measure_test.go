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

package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/server/metrics"
)

func TestCountDDLCreateUnit(t *testing.T) {
	t.Run("Test measurement helpers", func(t *testing.T) {
		ctx := context.Background()
		rs := metrics.NewRequestStatus("test_tenant")
		ctx = rs.SaveRequestStatusToContext(ctx)

		rs.AddReadBytes(int64(16384))
		rs.AddWriteBytes(int64(16384))
		countDDLCreateUnit(ctx)

		assert.Equal(t, int64(0), rs.GetReadBytes())
		assert.Equal(t, int64(0), rs.GetWriteBytes())
		assert.Equal(t, int64(1), rs.GetDDLCreateUnits())
	})
}

func TestCountDDLUpdateUnit(t *testing.T) {
	t.Run("Test measurement helpers", func(t *testing.T) {
		ctx := context.Background()
		rs := metrics.NewRequestStatus("test_tenant")
		ctx = rs.SaveRequestStatusToContext(ctx)

		rs.AddReadBytes(int64(16384))
		rs.AddWriteBytes(int64(16384))
		countDDLUpdateUnit(ctx, true)

		assert.Equal(t, int64(0), rs.GetReadBytes())
		assert.Equal(t, int64(0), rs.GetWriteBytes())
		assert.Equal(t, int64(1), rs.GetDDLUpdateUnits())
	})
}

func TestCountDDLDropUnit(t *testing.T) {
	t.Run("Test measurement helpers", func(t *testing.T) {
		ctx := context.Background()
		rs := metrics.NewRequestStatus("test_tenant")
		ctx = rs.SaveRequestStatusToContext(ctx)

		rs.AddReadBytes(int64(16384))
		rs.AddWriteBytes(int64(16384))
		countDDLDropUnit(ctx)

		assert.Equal(t, int64(0), rs.GetReadBytes())
		assert.Equal(t, int64(0), rs.GetWriteBytes())
		assert.Equal(t, int64(1), rs.GetDDLDropUnits())
	})
}
