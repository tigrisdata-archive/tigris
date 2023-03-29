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

package billing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
)

func TestNewProvider(t *testing.T) {
	t.Run("billing is disabled", func(t *testing.T) {
		provider := NewProvider()

		_, ok := provider.(*noop)
		require.True(t, ok)

		_, ok = provider.(*Metronome)
		require.False(t, ok)
	})

	t.Run("billing is enabled", func(t *testing.T) {
		config.DefaultConfig.Billing.Metronome.Enabled = true
		provider := NewProvider()

		_, ok := provider.(*noop)
		require.False(t, ok)

		_, ok = provider.(*Metronome)
		require.True(t, ok)
	})
}
