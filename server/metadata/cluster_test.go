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

package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/server/transaction"
)

var testClusterPayload = &ClusterMetadata{
	ID: uuid.New(),
}

func initClusterTest(t *testing.T) (*ClusterSubspace, transaction.Tx, func()) {
	c := NewClusterStore(newTestNameRegistry(t))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = kvStore.DropTable(ctx, c.SubspaceName)

	tm := transaction.NewManager(kvStore)
	tx, err := tm.StartTx(ctx)
	require.NoError(t, err)

	return c, tx, func() {
		assert.NoError(t, tx.Rollback(ctx))

		_ = kvStore.DropTable(ctx, c.SubspaceName)
	}
}

func TestClusterSubspace(t *testing.T) {
	t.Run("put_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx, cleanup := initClusterTest(t)
		defer cleanup()

		require.Equal(t, errors.InvalidArgument("invalid nil payload"), u.insert(ctx, tx, nil))
	})

	t.Run("put_get_1", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx, cleanup := initClusterTest(t)
		defer cleanup()

		require.NoError(t, u.insert(ctx, tx, testClusterPayload))
		cluster, err := u.Get(ctx, tx)
		require.NoError(t, err)
		require.Equal(t, testClusterPayload, cluster)

		// already exists
		require.Error(t, u.insert(ctx, tx, testClusterPayload))

		// empty payload
		require.Error(t, u.insert(ctx, tx, nil))
	})

	t.Run("put_get_2", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx, cleanup := initClusterTest(t)
		defer cleanup()

		appPayload := &ClusterMetadata{
			ID: uuid.New(),
		}

		require.NoError(t, u.insert(ctx, tx, appPayload))
		cluster, err := u.Get(ctx, tx)
		require.NoError(t, err)
		require.Equal(t, appPayload, cluster)
	})

	t.Run("put_get_update_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx, cleanup := initClusterTest(t)
		defer cleanup()

		require.NoError(t, u.insert(ctx, tx, testClusterPayload))
		cluster, err := u.Get(ctx, tx)
		require.NoError(t, err)
		require.Equal(t, testClusterPayload, cluster)

		updatedClusterPayload := &ClusterMetadata{
			ID: uuid.New(),
		}
		require.NoError(t, u.Update(ctx, tx, updatedClusterPayload))
		cluster, err = u.Get(ctx, tx)
		require.NoError(t, err)
		require.Equal(t, updatedClusterPayload, cluster)
	})

	t.Run("put_get_delete_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u, tx, cleanup := initClusterTest(t)
		defer cleanup()

		require.NoError(t, u.insert(ctx, tx, testClusterPayload))
		cluster, err := u.Get(ctx, tx)
		require.NoError(t, err)
		require.Equal(t, testClusterPayload, cluster)

		require.NoError(t, u.delete(ctx, tx))
		_, err = u.Get(ctx, tx)
		require.Equal(t, errors.ErrNotFound, err)
	})
}
