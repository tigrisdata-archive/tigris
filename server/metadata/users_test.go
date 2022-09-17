package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

func TestUserSubspace(t *testing.T) {
	fdbCfg, err := config.GetTestFDBConfig("../../..")
	require.NoError(t, err)

	kvStore, err := kv.NewKeyValueStore(fdbCfg)
	require.NoError(t, err)

	t.Run("put_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u := NewUserStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, u.UserSubspaceName())

		userPayload := []byte(`{
								"user_name": "123",
								"last_visit": "123123"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.Equal(t, errors.InvalidArgument("invalid empty userId"), u.InsertUserMetadata(ctx, tx, 1, User, "", "meta-key-1", userPayload))
		require.Equal(t, errors.InvalidArgument("invalid nil payload"), u.InsertUserMetadata(ctx, tx, 1, User, "some-valid-user-id", "meta-key-1", nil))
		require.Equal(t, errors.InvalidArgument("invalid namespace, id must be greater than 0"), u.InsertUserMetadata(ctx, tx, 0, User, "user-id-1", "meta-key-1", userPayload))
		_ = kvStore.DropTable(ctx, u.UserSubspaceName())
	})

	t.Run("put_get_1", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u := NewUserStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, u.UserSubspaceName())

		userPayload := []byte(`{
								"user_name": "123",
								"last_visit": "123123"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, u.InsertUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1", userPayload))
		user, err := u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, userPayload, user)

		_ = kvStore.DropTable(ctx, u.UserSubspaceName())
	})

	t.Run("put_get_2", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u := NewUserStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, u.UserSubspaceName())

		appPayload := []byte(`{
								"app_id": "123",
								"last_login": "123123"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, u.InsertUserMetadata(ctx, tx, 1, Application, "app-id-123", "meta-key-1", appPayload))
		user, err := u.GetUserMetadata(ctx, tx, 1, Application, "app-id-123", "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, appPayload, user)

		_ = kvStore.DropTable(ctx, u.UserSubspaceName())
	})

	t.Run("put_get_update_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u := NewUserStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, u.UserSubspaceName())

		userPayload := []byte(`{
								"user_name": "123",
								"last_visit": "123123"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, u.InsertUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1", userPayload))
		user, err := u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, userPayload, user)

		updatedUserPayload := []byte(`{
								"user_name": "123",
								"last_visit": "456456"
								}`)
		require.NoError(t, u.UpdateUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1", updatedUserPayload))
		user, err = u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, updatedUserPayload, user)

		_ = kvStore.DropTable(ctx, u.UserSubspaceName())
	})

	t.Run("put_get_deleteuser_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u := NewUserStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, u.UserSubspaceName())

		metaVal1 := []byte(`{
								"meta-key-1": "val1",
								"meta-key-2": "val2"
								}`)

		metaVal2 := []byte(`{
								"meta-key-1": "val1",
								"meta-key-2": "val2"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, u.InsertUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1", metaVal1))
		require.NoError(t, u.InsertUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-2", metaVal2))

		metaValRetrieved, err := u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, metaVal1, metaValRetrieved)

		metaValRetrieved, err = u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-2")
		require.NoError(t, err)
		require.Equal(t, metaVal2, metaValRetrieved)

		require.NoError(t, u.DeleteUser(ctx, tx, 1, User, "user-id-1"))

		metaValRetrieved, err = u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1")
		require.NoError(t, err)
		require.Nil(t, metaValRetrieved)

		metaValRetrieved, err = u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-2")
		require.NoError(t, err)
		require.Nil(t, metaValRetrieved)

		_ = kvStore.DropTable(ctx, u.UserSubspaceName())
	})

	t.Run("put_get_delete_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		u := NewUserStore(&TestMDNameRegistry{
			SchemaSB: "test_schema",
		})
		_ = kvStore.DropTable(ctx, u.UserSubspaceName())

		userPayload := []byte(`{
								"user_name": "123",
								"last_visit": "123123"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, u.InsertUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1", userPayload))
		user, err := u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, userPayload, user)

		require.NoError(t, u.DeleteUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1"))
		user, err = u.GetUserMetadata(ctx, tx, 1, User, "user-id-1", "meta-key-1")
		require.NoError(t, err)
		require.Nil(t, user)

		_ = kvStore.DropTable(ctx, u.UserSubspaceName())
	})

}
