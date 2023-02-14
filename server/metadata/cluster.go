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

	"github.com/google/uuid"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
)

// ClusterMetadata keeps cluster wide metadata.
type ClusterMetadata struct {
	ID uuid.UUID
}

// ClusterSubspace is used to store metadata about Tigris clusters.
type ClusterSubspace struct {
	metadataSubspace
}

var (
	clusterID          = uuid.Nil.String()
	clusterMetadataKey = "cluster"
)

const (
	clusterMetaValueVersion int32 = 1
	clusterMetaKeyVersion   byte  = 1
)

func NewClusterStore(nameRegistry *NameRegistry) *ClusterSubspace {
	return &ClusterSubspace{
		metadataSubspace{
			SubspaceName: nameRegistry.ClusterSubspaceName(),
			KeyVersion:   []byte{clusterMetaKeyVersion},
		},
	}
}

func (u *ClusterSubspace) getKey(clusterID string, clusterMetadataKey string) keys.Key {
	return keys.NewKey(u.SubspaceName, u.KeyVersion, []byte(clusterID), []byte(clusterMetadataKey))
}

func (u *ClusterSubspace) insert(ctx context.Context, tx transaction.Tx, metadata *ClusterMetadata) error {
	return u.insertMetadata(ctx, tx,
		u.validateArgs(clusterID, &clusterMetadataKey, &metadata),
		u.getKey(clusterID, clusterMetadataKey),
		clusterMetaValueVersion,
		metadata)
}

func (u *ClusterSubspace) Get(ctx context.Context, tx transaction.Tx) (*ClusterMetadata, error) {
	var metadata ClusterMetadata

	if err := u.getMetadata(ctx, tx,
		u.validateArgs(clusterID, &clusterMetadataKey, nil),
		u.getKey(clusterID, clusterMetadataKey),
		&metadata,
	); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (u *ClusterSubspace) Update(ctx context.Context, tx transaction.Tx, metadata *ClusterMetadata) error {
	return u.updateMetadata(ctx, tx,
		u.validateArgs(clusterID, &clusterMetadataKey, &metadata),
		u.getKey(clusterID, clusterMetadataKey),
		clusterMetaValueVersion,
		metadata)
}

func (u *ClusterSubspace) delete(ctx context.Context, tx transaction.Tx) error {
	return u.deleteMetadata(ctx, tx,
		u.validateArgs(clusterID, nil, nil),
		u.getKey(clusterID, clusterMetadataKey),
	)
}

func (u *ClusterSubspace) validateArgs(clusterID string, metadataKey *string, metadata **ClusterMetadata) error {
	if clusterID == "" {
		return errors.InvalidArgument("invalid empty clusterID")
	}

	if metadataKey != nil && *metadataKey == "" {
		return errors.InvalidArgument("invalid empty metadataKey")
	}

	if metadata != nil && *metadata == nil {
		return errors.InvalidArgument("invalid nil payload")
	}

	return nil
}
