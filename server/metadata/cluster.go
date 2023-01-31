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
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// ClusterSubspace is used to store metadata about Tigris clusters.
type ClusterSubspace struct {
	metadataSubspace
}

var (
	clusterVersion     = []byte{0x01}
	clusterID          = uuid.Nil.String()
	clusterMetadataKey = "key"
)

// ClusterMetadata keeps cluster wide metadata.
type ClusterMetadata struct {
	WorkerKeepalive time.Time
}

func NewClusterStore(nameRegistry *NameRegistry) *ClusterSubspace {
	return &ClusterSubspace{
		metadataSubspace{
			SubspaceName: nameRegistry.ClusterSubspaceName(),
			Version:      clusterVersion,
		},
	}
}

func (u *ClusterSubspace) getKey(clusterID string, clusterMetadataKey string) keys.Key {
	return keys.NewKey(u.SubspaceName, clusterVersion,
		[]byte(clusterID), []byte(clusterMetadataKey))
}

func (u *ClusterSubspace) Insert(ctx context.Context, tx transaction.Tx, metadata *ClusterMetadata) error {
	if err := u.validateArgs(clusterID, &clusterMetadataKey, &metadata); err != nil {
		return err
	}

	payload, err := jsoniter.Marshal(metadata)
	if ulog.E(err) {
		return errors.Internal("failed to marshal cluster metadata")
	}

	return u.insertMetadata(ctx, tx, nil, u.getKey(clusterID, clusterMetadataKey), payload)
}

func (u *ClusterSubspace) Get(ctx context.Context, tx transaction.Tx) (*ClusterMetadata, error) {
	payload, err := u.getMetadata(ctx, tx,
		u.validateArgs(clusterID, &clusterMetadataKey, nil),
		u.getKey(clusterID, clusterMetadataKey),
	)
	if err != nil {
		return nil, err
	}

	if payload == nil {
		return nil, nil
	}

	var metadata ClusterMetadata
	if err = jsoniter.Unmarshal(payload, &metadata); ulog.E(err) {
		return nil, errors.Internal("failed to unmarshal cluster metadata")
	}

	return &metadata, nil
}

func (u *ClusterSubspace) Update(ctx context.Context, tx transaction.Tx, metadata *ClusterMetadata) error {
	if err := u.validateArgs(clusterID, &clusterMetadataKey, &metadata); err != nil {
		return err
	}

	payload, err := jsoniter.Marshal(metadata)
	if ulog.E(err) {
		return errors.Internal("failed to marshal cluster metadata")
	}

	return u.updateMetadata(ctx, tx, nil, u.getKey(clusterID, clusterMetadataKey), payload)
}

func (u *ClusterSubspace) Delete(ctx context.Context, tx transaction.Tx) error {
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
