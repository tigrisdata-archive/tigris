// Copyright 2022 Tigris Data, Inc.
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

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
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

var userVersion = []byte{0x01}

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
	if err := validateUserArgs(namespaceId, userId, metadataKey, payload); err != nil {
		return err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId), []byte(metadataKey))
	if err := tx.Insert(ctx, key, internal.NewTableData(payload)); err != nil {
		log.Debug().Str("key", key.String()).Str("value", string(payload)).Err(err).Msg("storing user metadata failed")
		return err
	}

	log.Debug().Str("key", key.String()).Str("value", string(payload)).Msg("storing user metadata succeed")

	return nil
}

func (u *UserSubspace) GetUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string, metadataKey string) ([]byte, error) {
	if err := validateUserArgsPartial1(namespaceId, userId, metadataKey); err != nil {
		return nil, err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId), []byte(metadataKey))
	it, err := tx.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	var row kv.KeyValue
	if it.Next(&row) {
		log.Debug().Str("key", key.String()).Str("value", string(row.Data.RawData)).Msg("reading user metadata succeed")
		return row.Data.RawData, nil
	}

	return nil, it.Err()
}

func (u *UserSubspace) UpdateUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string, metadataKey string, payload []byte) error {
	if err := validateUserArgs(namespaceId, userId, metadataKey, payload); err != nil {
		return err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId), []byte(metadataKey))

	_, err := tx.Update(ctx, key, func(data *internal.TableData) (*internal.TableData, error) {
		return internal.NewTableData(payload), nil
	})
	if err != nil {
		return err
	}
	log.Debug().Str("key", key.String()).Str("value", string(payload)).Msg("update user metadata succeed")
	return nil
}

func (u *UserSubspace) DeleteUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string, metadataKey string) error {
	if err := validateUserArgsPartial1(namespaceId, userId, metadataKey); err != nil {
		return err
	}
	key := keys.NewKey(u.UserSubspaceName(), userVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)), []byte(userId), []byte(metadataKey))
	err := tx.Delete(ctx, key)
	if err != nil {
		log.Debug().Str("key", key.String()).Err(err).Msg("Delete user metadata failed")
		return err
	}
	log.Debug().Str("key", key.String()).Msg("Delete user metadata  succeed")
	return nil
}

func (u *UserSubspace) DeleteUser(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType, userId string) error {
	if err := validateUserArgsPartial2(namespaceId, userId); err != nil {
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

func validateUserArgs(namespaceId uint32, userId string, metadataKey string, payload []byte) error {
	if err := validateUserArgsPartial1(namespaceId, userId, metadataKey); err != nil {
		return err
	}
	if payload == nil {
		return errors.InvalidArgument("invalid nil payload")
	}
	return nil
}

func validateUserArgsPartial1(namespaceId uint32, userId string, metadataKey string) error {
	if err := validateUserArgsPartial2(namespaceId, userId); err != nil {
		return err
	}
	if metadataKey == "" {
		return errors.InvalidArgument("invalid empty metadataKey")
	}
	return nil
}

func validateUserArgsPartial2(namespaceId uint32, userId string) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}
	if userId == "" {
		return errors.InvalidArgument("invalid empty userId")
	}

	return nil
}
