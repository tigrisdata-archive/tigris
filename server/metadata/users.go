package metadata

import (
	"context"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

const (
	UserSubspaceName = "user"
)

// UserSubspace is used to store metadata about Tigris users.
type UserSubspace struct {
	MDNameRegistry
}

var (
	userVersion = []byte{0x01}
)

type UserType uint32

const (
	User        UserType = 0
	Application UserType = 1
)

func NewUserStore(mdNameRegistry MDNameRegistry) *UserSubspace {
	return &UserSubspace{
		MDNameRegistry: mdNameRegistry,
	}
}

func (u *UserSubspace) InsertUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string, metadataKey string, payload []byte) error {
	if err := validateArgsFull(namespaceId, userId, metadataKey, payload); err != nil {
		return err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId), []byte(metadataKey))
	if err := tx.Insert(ctx, key, internal.NewTableData(payload)); err != nil {
		log.Debug().Str("key", key.String()).Str("value", string(payload)).Err(err).Msg("storing user failed")
		return err
	}
	log.Debug().Str("key", key.String()).Str("value", string(payload)).Msg("storing user succeed")

	return nil
}

func (u *UserSubspace) GetUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string, metadataKey string) ([]byte, error) {
	if err := validateArgsPartial1(namespaceId, userId, metadataKey); err != nil {
		return nil, err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId), []byte(metadataKey))
	it, err := tx.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	var row kv.KeyValue
	for it.Next(&row) {
		return row.Data.RawData, nil
	}
	return nil, nil
}

func (u *UserSubspace) UpdateUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string, metadataKey string, payload []byte) error {
	if err := validateArgsFull(namespaceId, userId, metadataKey, payload); err != nil {
		return err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId), []byte(metadataKey))

	_, err := tx.Update(ctx, key, func(data *internal.TableData) (*internal.TableData, error) {
		return internal.NewTableData(payload), nil
	})

	if err != nil {
		return err
	}
	return nil
}

func (u *UserSubspace) DeleteUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string, metadataKey string) error {
	if err := validateArgsPartial1(namespaceId, userId, metadataKey); err != nil {
		return err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId), []byte(metadataKey))
	err := tx.Delete(ctx, key)
	if err != nil {
		log.Debug().Str("key", key.String()).Err(err).Msg("Delete user failed")
		return err
	}
	log.Debug().Str("key", key.String()).Msg("Delete user succeed")
	return nil
}

func (u *UserSubspace) DeleteUser(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string) error {
	if err := validateArgsPartial2(namespaceId, userId); err != nil {
		return err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId))
	err := tx.Delete(ctx, key)
	if err != nil {
		log.Debug().Str("key", key.String()).Err(err).Msg("Delete user failed")
		return err
	}
	log.Debug().Str("key", key.String()).Msg("Delete user succeed")
	return nil
}

func validateArgsFull(namespaceId uint32, userId string, metadataKey string, payload []byte) error {
	if err := validateArgsPartial1(namespaceId, userId, metadataKey); err != nil {
		return err
	}
	if payload == nil {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "invalid nil payload")
	}
	return nil
}
func validateArgsPartial1(namespaceId uint32, userId string, metadataKey string) error {
	if err := validateArgsPartial2(namespaceId, userId); err != nil {
		return err
	}
	if metadataKey == "" {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "invalid empty metadataKey")
	}
	return nil
}

func validateArgsPartial2(namespaceId uint32, userId string) error {
	if namespaceId < 1 {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "invalid namespace, id must be greater than 0")
	}
	if userId == "" {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "invalid empty userId")
	}

	return nil
}
