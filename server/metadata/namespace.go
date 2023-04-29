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
	"github.com/tigrisdata/tigris/server/defaults"
	"github.com/tigrisdata/tigris/server/transaction"
)

// A Namespace is a logical grouping of databases.
type Namespace interface {
	// Id for the namespace is used by the cluster to append as the first element in the key.
	Id() uint32
	// StrId is the name used for the lookup.
	StrId() string
	// Metadata for the namespace
	Metadata() NamespaceMetadata
	// SetMetadata updates namespace metadata
	SetMetadata(update NamespaceMetadata)
}

// NamespaceMetadata - This structure is persisted as the namespace in DB.
type NamespaceMetadata struct {
	// unique namespace Id
	Id uint32
	// unique namespace name StrId
	StrId string
	// displayName for the namespace
	Name string
	// external accounts
	Accounts AccountIntegrations
}

// DefaultNamespace is for "default" namespace in the cluster. This is useful when there is no need to logically group
// databases. All databases will be created under a single namespace. It is totally fine for a deployment to choose this
// and just have one namespace. The default assigned value for this namespace is 1.
type DefaultNamespace struct {
	metadata NamespaceMetadata
}

type ProjectMetadata struct {
	ID             uint32
	Creator        string
	CreatedAt      int64
	CachesMetadata []CacheMetadata
	SearchMetadata []SearchMetadata
}

type CacheMetadata struct {
	Name      string
	Creator   string
	CreatedAt int64
}

type SearchMetadata struct {
	Name      string
	Creator   string
	CreatedAt int64
}

// StrId returns id assigned to the namespace.
func (n *DefaultNamespace) StrId() string {
	return defaults.DefaultNamespaceName
}

// Id returns id assigned to the namespace.
func (n *DefaultNamespace) Id() uint32 {
	return defaults.DefaultNamespaceId
}

// Metadata returns metadata assigned to the namespace.
func (n *DefaultNamespace) Metadata() NamespaceMetadata {
	return n.metadata
}

// SetMetadata updates namespace metadata.
func (n *DefaultNamespace) SetMetadata(update NamespaceMetadata) {
	n.metadata = update
}

func NewNamespaceMetadata(id uint32, name string, displayName string) NamespaceMetadata {
	return NamespaceMetadata{
		Id:    id,
		StrId: name,
		Name:  displayName,
	}
}

func NewDefaultNamespace() *DefaultNamespace {
	return &DefaultNamespace{
		metadata: NewNamespaceMetadata(defaults.DefaultNamespaceId, defaults.DefaultNamespaceName, defaults.DefaultNamespaceName),
	}
}

// AccountIntegrations represents the external accounts.
type AccountIntegrations struct {
	Metronome *Metronome
}

func (a *AccountIntegrations) AddMetronome(id string) {
	a.Metronome = &Metronome{
		Enabled: true,
		Id:      id,
	}
}

// DisableMetronome for this user.
func (a *AccountIntegrations) DisableMetronome() {
	if a.Metronome == nil {
		a.Metronome = &Metronome{}
	}
	a.Metronome.Enabled = false
}

// GetMetronomeId returns the linked Metronome account id and a boolean to indicate if integration could be enabled.
func (a *AccountIntegrations) GetMetronomeId() (string, bool) {
	if a.Metronome == nil {
		return "", true
	}
	return a.Metronome.Id, a.Metronome.Enabled
}

type Metronome struct {
	// true - Either customer is already registered or can be registered with Metronome
	// false - do not use Metronome for this user
	Enabled bool
	Id      string
}

// NamespaceSubspace is used to store metadata about Tigris namespaces.
type NamespaceSubspace struct {
	metadataSubspace
}

const (
	nsMetaValueVersion      = 1
	nsMetaKeyVersion   byte = 1
)

func NewNamespaceStore(mdNameRegistry *NameRegistry) *NamespaceSubspace {
	return &NamespaceSubspace{
		metadataSubspace{
			SubspaceName: mdNameRegistry.NamespaceSubspaceName(),
			KeyVersion:   []byte{nsMetaKeyVersion},
		},
	}
}

func (n *NamespaceSubspace) getProjKey(namespaceId uint32, projName string) keys.Key {
	return keys.NewKey(n.SubspaceName, n.KeyVersion, UInt32ToByte(namespaceId), dbKey, projName)
}

func (n *NamespaceSubspace) getNSKey(namespaceId uint32, metadataKey string) keys.Key {
	if metadataKey != "" {
		return keys.NewKey(n.SubspaceName, n.KeyVersion, UInt32ToByte(namespaceId), []byte(metadataKey))
	}

	return keys.NewKey(n.SubspaceName, n.KeyVersion, UInt32ToByte(namespaceId))
}

func (n *NamespaceSubspace) InsertProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	projName string, projMetadata *ProjectMetadata,
) error {
	return n.insertMetadata(ctx, tx,
		n.validateProjectArgs(namespaceId, projName, &projMetadata),
		n.getProjKey(namespaceId, projName), nsMetaValueVersion, projMetadata)
}

func (n *NamespaceSubspace) GetProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	projName string,
) (*ProjectMetadata, error) {
	var projMetadata ProjectMetadata

	if err := n.getMetadata(ctx, tx,
		n.validateProjectArgs(namespaceId, projName, nil),
		n.getProjKey(namespaceId, projName),
		&projMetadata,
	); err != nil {
		// TODO: This is not needed if the cluster created after writing of this comment
		// Project metadata inserted during project creation and should always exist.
		if err == errors.ErrNotFound {
			return &ProjectMetadata{}, nil
		}

		return nil, err
	}

	return &projMetadata, nil
}

func (n *NamespaceSubspace) UpdateProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	projName string, projMetadata *ProjectMetadata,
) error {
	return n.updateMetadata(ctx, tx,
		n.validateProjectArgs(namespaceId, projName, &projMetadata),
		n.getProjKey(namespaceId, projName),
		nsMetaValueVersion,
		projMetadata,
	)
}

func (n *NamespaceSubspace) DeleteProjectMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	projName string,
) error {
	return n.deleteMetadata(ctx, tx,
		n.validateProjectArgs(namespaceId, projName, nil),
		n.getProjKey(namespaceId, projName),
	)
}

func (n *NamespaceSubspace) validateProjectArgs(namespaceId uint32, projName string, metadata **ProjectMetadata) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}

	if projName == "" {
		return errors.InvalidArgument("invalid projName, projName must not be blank")
	}

	if metadata != nil && *metadata == nil {
		return errors.InvalidArgument("invalid projMetadata, projMetadata must not be nil")
	}

	return nil
}

func (n *NamespaceSubspace) InsertNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	metadataKey string, payload []byte,
) error {
	return n.insertPayload(ctx, tx,
		n.validateNSArgs(namespaceId, &metadataKey, &payload),
		n.getNSKey(namespaceId, metadataKey),
		nsMetaValueVersion,
		payload,
	)
}

func (n *NamespaceSubspace) GetNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	metadataKey string,
) ([]byte, error) {
	data, err := n.getPayload(ctx, tx,
		n.validateNSArgs(namespaceId, &metadataKey, nil),
		n.getNSKey(namespaceId, metadataKey),
	)
	if err != nil {
		return nil, err
	}

	return data.RawData, nil
}

func (n *NamespaceSubspace) UpdateNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	metadataKey string, payload []byte,
) error {
	return n.updatePayload(ctx, tx,
		n.validateNSArgs(namespaceId, &metadataKey, &payload),
		n.getNSKey(namespaceId, metadataKey),
		nsMetaValueVersion,
		payload,
	)
}

func (n *NamespaceSubspace) DeleteNamespaceMetadata(ctx context.Context, tx transaction.Tx, namespaceId uint32,
	metadataKey string,
) error {
	return n.deleteMetadata(ctx, tx,
		n.validateNSArgs(namespaceId, &metadataKey, nil),
		n.getNSKey(namespaceId, metadataKey),
	)
}

func (n *NamespaceSubspace) DeleteNamespace(ctx context.Context, tx transaction.Tx, namespaceId uint32) error {
	return n.deleteMetadata(ctx, tx,
		n.validateNSArgs(namespaceId, nil, nil),
		n.getNSKey(namespaceId, ""),
	)
}

func (n *NamespaceSubspace) validateNSArgs(namespaceId uint32, metadataKey *string, value *[]byte) error {
	if namespaceId < 1 {
		return errors.InvalidArgument("invalid namespace, id must be greater than 0")
	}

	if metadataKey != nil {
		if *metadataKey == "" {
			return errors.InvalidArgument("invalid empty metadataKey")
		}

		if *metadataKey == dbKey {
			return errors.InvalidArgument("invalid metadataKey. " + dbKey + " is reserved")
		}

		if *metadataKey == namespaceKey {
			return errors.InvalidArgument("invalid metadataKey. " + namespaceKey + " is reserved")
		}
	}

	if value != nil && *value == nil {
		return errors.InvalidArgument("invalid nil payload")
	}

	return nil
}
