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

	"github.com/tigrisdata/tigris/server/config"
)

func TestSizeMetrics(t *testing.T) {
	var testSize int64 = 1000
	testNamespace := "test_namespace"
	testNamespaceName := "test_namespace"
	testDb := "test_db"
	testCollection := "test_collection"

	config.DefaultConfig.Tracing.Enabled = true
	config.DefaultConfig.Metrics.Enabled = true
	InitializeMetrics()

	t.Run("Update namespace size metrics", func(t *testing.T) {
		UpdateNameSpaceSizeMetrics(testNamespace, testNamespaceName, testSize)
	})

	t.Run("Update database size metrics", func(t *testing.T) {
		UpdateDbSizeMetrics(testNamespace, testNamespaceName, testDb, testSize)
	})

	t.Run("Update collection size metrics", func(t *testing.T) {
		UpdateCollectionSizeMetrics(testNamespace, testNamespaceName, testDb, testCollection, testSize)
	})
}
