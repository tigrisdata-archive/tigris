package metadata

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisdb/server/config"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"testing"
)

func TestMetaVersion(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kv, err := kv.NewFoundationDB(fdbCfg)
	require.NoError(t, err)

	t.Run("read versions", func(t *testing.T) {
		m := &MetaVersion{}
		ctx := context.TODO()
		tm := transaction.NewManager(kv)
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		first, err := m.Read(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)

		second, err := m.Read(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, first, second)
	})
	t.Run("bump and read", func(t *testing.T) {
		m := &MetaVersion{}
		ctx := context.TODO()
		tm := transaction.NewManager(kv)
		tx, err := tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		first, err := m.Read(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		require.NoError(t, m.Increment(ctx, tx))
		require.NoError(t, tx.Commit(ctx))

		tx, err = tm.StartTxWithoutTracking(ctx)
		require.NoError(t, err)
		second, err := m.Read(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.NotEqual(t, first, second)
	})
}
