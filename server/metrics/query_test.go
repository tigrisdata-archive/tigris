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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryMetrics(t *testing.T) {
	t.Run("Test streaming query metrics", func(t *testing.T) {
		qm := StreamingQueryMetrics{}
		qm.SetReadType("test_value")
		qm.SetSort(false)
		tags := qm.GetTags()
		assert.Equal(t, "test_value", tags["read_type"])
		assert.Equal(t, "false", tags["sort"])
	})

	t.Run("Test search query metrics", func(t *testing.T) {
		qm := SearchQueryMetrics{}
		qm.SetSearchType("test_value")
		qm.SetSort(true)
		tags := qm.GetTags()
		assert.Equal(t, "test_value", tags["search_type"])
		assert.Equal(t, "true", tags["sort"])
	})

	t.Run("Test write query metrics", func(t *testing.T) {
		qm := WriteQueryMetrics{}
		qm.SetWriteType("test_value")
		tags := qm.GetTags()
		assert.Equal(t, "test_value", tags["write_type"])
	})
}
