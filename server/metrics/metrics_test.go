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
	"os"
	"testing"

	"github.com/tigrisdata/tigris/server/config"
	ulog "github.com/tigrisdata/tigris/util/log"
)

func TestInitializeMetrics(t *testing.T) {
	t.Run("Test Global tags", func(t *testing.T) {
		globalTags := GetGlobalTags()
		keysToCheck := []string{"service", "env", "version"}
		for _, key := range keysToCheck {
			if _, ok := globalTags[key]; !ok {
				t.Errorf("Key %s not found in global tags", key)
			}
		}
	})

	t.Run("Initialize metrics", func(t *testing.T) {
		config.DefaultConfig.Metrics.Enabled = true
		// Will panic if the high level structure cannot be created
		InitializeMetrics()

		SchemaReadOutdated("proj1", "branch1", "coll1")
		SchemaUpdateRepaired("proj1", "branch1", "coll1")
	})
}

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled", Format: "console"})

	os.Exit(m.Run())
}
