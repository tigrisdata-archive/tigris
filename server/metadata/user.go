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

	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
)

// UserSubspace is used to store metadata about Tigris users.
type UserSubspace struct {
	metadataSubspace
}

const userMetaValueVersion = 1

const userMetaKeyVersion byte = 1

type UserType uint32

const (
	User        UserType = 0
	Application UserType = 1
)

func NewUserStore(mdNameRegistry *NameRegistry) *UserSubspace {
	return &UserSubspace{
		metadataSubspace{
			SubspaceName: mdNameRegistry.UserSubspaceName(),
			KeyVersion:   []byte{userMetaKeyVersion},
		},
	}
}

func (u *UserSubspace) getKey(namespaceId uint32, userType UserType, userId string, metadataKey string) keys.Key {
	if metadataKey != "" {
		return keys.NewKey(u.SubspaceName, u.KeyVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)),
			[]byte(userId), []byte(metadataKey))
	}

	return keys.NewKey(u.SubspaceName, u.KeyVersion, UInt32ToByte(namespaceId), UInt32ToByte(uint32(userType)),
		[]byte(userId))
}

func (u *UserSubspace) InsertUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType,
	userId string, metadataKey string, payload []byte,
) error {
	return u.insertPayload(ctx, tx,
		u.validateArgs(namespaceId, userId, &metadataKey, &payload),
		u.getKey(namespaceId, userType, userId, metadataKey),
		userMetaValueVersion,
		payload,
	)
}

func (u *UserSubspace) GetUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType,
	userId string, metadataKey string,
) ([]byte, error) {
	payload, err := u.getPayload(ctx, tx,
		u.validateArgs(namespaceId, userId, &metadataKey, nil),
		u.getKey(namespaceId, userType, userId, metadataKey),
	)
	if err != nil {
		return nil, err
	}

	return payload.RawData, nil
}

func (u *UserSubspace) UpdateUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType,
	userId string, metadataKey string, payload []byte,
) error {
	return u.updatePayload(ctx, tx,
		u.validateArgs(namespaceId, userId, &metadataKey, &payload),
		u.getKey(namespaceId, userType, userId, metadataKey),
		userMetaValueVersion,
		payload,
	)
}

func (u *UserSubspace) DeleteUserMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType,
	userId string, metadataKey string,
) error {
	return u.deleteMetadata(ctx, tx,
		u.validateArgs(namespaceId, userId, &metadataKey, nil),
		u.getKey(namespaceId, userType, userId, metadataKey),
	)
}

func (u *UserSubspace) DeleteUser(ctx context.Context, tx transaction.Tx, namespaceId uint32, userType UserType,
	userId string,
) error {
	return u.deleteMetadata(ctx, tx,
		u.validateArgs(namespaceId, userId, nil, nil),
		u.getKey(namespaceId, userType, userId, ""),
	)
}

func (*UserSubspace) validateArgs(namespaceId uint32, userId string, metadataKey *string, payload *[]byte) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}

	if userId == "" {
		return errors.InvalidArgument("invalid empty userId")
	}

	if metadataKey != nil && *metadataKey == "" {
		return errors.InvalidArgument("invalid empty metadataKey")
	}

	if payload != nil && *payload == nil {
		return errors.InvalidArgument("invalid nil payload")
	}

	return nil
}
