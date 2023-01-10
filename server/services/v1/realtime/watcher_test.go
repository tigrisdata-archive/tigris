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

package realtime

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/store/cache"
)

func TestWatcher(t *testing.T) {
	ctx := context.TODO()
	cacheS := cache.NewCache(config.GetTestCacheConfig())

	t.Run("create_register", func(t *testing.T) {
		stream, err := cacheS.CreateStream(ctx, "ch_test")
		require.NoError(t, err)
		defer func() {
			_ = stream.Delete(ctx)
		}()

		groups, err := stream.GetConsumerGroups(ctx)
		require.NoError(t, err)
		require.Len(t, groups, 1)
		require.Equal(t, cache.DefaultGroup, groups[0].Name)
		w, err := CreateAndRegisterWatcher(ctx, "device1", "", stream)
		require.NoError(t, err)
		require.NotNil(t, w)

		groups, err = stream.GetConsumerGroups(ctx)
		require.NoError(t, err)
		require.Len(t, groups, 2)
		groupsName := make([]string, 2)
		for i, g := range groups {
			groupsName[i] = g.Name
		}
		sort.Strings(groupsName)
		require.Equal(t, "device1", groupsName[1])
		w.Disconnect()
		w.watchEvents()
		groups, err = stream.GetConsumerGroups(ctx)
		require.NoError(t, err)
		require.Len(t, groups, 1)
	})
}
