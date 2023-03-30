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
	"github.com/tigrisdata/tigris/server/config"
)

func TestGetUnitsFromBytes(t *testing.T) {
	t.Run("Test getUnitsFromBytes", func(t *testing.T) {
		assert.Equal(t, int64(0), getUnitsFromBytes(int64(0), config.WriteUnitSize))
		assert.Equal(t, int64(1), getUnitsFromBytes(int64(1), config.WriteUnitSize))
		assert.Equal(t, int64(1), getUnitsFromBytes(int64(4095), config.WriteUnitSize))
		assert.Equal(t, int64(1), getUnitsFromBytes(int64(4096), config.WriteUnitSize))
		assert.Equal(t, int64(2), getUnitsFromBytes(int64(4097), config.WriteUnitSize))
		assert.Equal(t, int64(4), getUnitsFromBytes(int64(16384), config.WriteUnitSize))
	})
}
