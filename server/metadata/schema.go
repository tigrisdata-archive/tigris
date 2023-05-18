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
	"sort"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

var schVersion = []byte{0x01}

// SchemaSubspace is used to manage schemas in schema subspace.
type SchemaSubspace struct {
	SubspaceName []byte
	version      []byte
}

func NewSchemaStore(mdNameRegistry *NameRegistry) *SchemaSubspace {
	return &SchemaSubspace{
		SubspaceName: mdNameRegistry.SchemaSubspaceName(),
		version:      schVersion,
	}
}

func (s *SchemaSubspace) getKey(namespaceId uint32, dbId uint32, collId uint32, revision uint32) keys.Key {
	if revision > 0 {
		return keys.NewKey(s.SubspaceName, s.version, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), keyEnd, UInt32ToByte(revision))
	}

	return keys.NewKey(s.SubspaceName, s.version, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), keyEnd)
}

// Put is to persist schema for a given namespace, database and collection.
func (s *SchemaSubspace) Put(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32, schema []byte, revision uint32) error {
	return schemaPut(ctx, tx, s.getKey(namespaceId, dbId, collId, revision), schema, revision)
}

// GetLatest returns the latest version stored for a collection inside a given namespace and database.
func (s *SchemaSubspace) GetLatest(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32) (*schema.Version, error) {
	return schemaGetLatest(ctx, tx, s.getKey(namespaceId, dbId, collId, 0))
}

// Get returns all the version stored for a collection inside a given namespace and database.
func (s *SchemaSubspace) Get(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32) (schema.Versions, error) {
	return schemaGet(ctx, tx, s.getKey(namespaceId, dbId, collId, 0))
}

func (s *SchemaSubspace) GetNotGreater(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32, version uint32) (*schema.Version, error) {
	versions, err := schemaGet(ctx, tx, s.getKey(namespaceId, dbId, collId, 0))
	if err != nil {
		return nil, err
	}

	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].Version <= version {
			return &versions[i], nil
		}
	}

	return nil, errors.NotFound("schema version %d not found", version)
}

// GetVersion returns specific version stored for a collection inside a given namespace and database.
func (s *SchemaSubspace) GetVersion(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32, version uint32) (*schema.Version, error) {
	v, err := schemaGet(ctx, tx, s.getKey(namespaceId, dbId, collId, version))
	if err != nil {
		return nil, err
	}

	if len(v) == 0 {
		return nil, errors.NotFound("schema version %d not found", version)
	}

	return &v[0], nil
}

// Delete is to remove schema for a given namespace, database and collection.
func (s *SchemaSubspace) Delete(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32) error {
	return schemaDelete(ctx, tx, s.getKey(namespaceId, dbId, collId, 0))
}

// SearchSchemaSubspace is used to manage search schemas.
type SearchSchemaSubspace struct {
	SubspaceName []byte
	version      []byte
}

func NewSearchSchemaStore(mdNameRegistry *NameRegistry) *SearchSchemaSubspace {
	return &SearchSchemaSubspace{
		SubspaceName: mdNameRegistry.SearchSchemaSubspaceName(),
		version:      schVersion,
	}
}

func (s *SearchSchemaSubspace) getKey(namespaceId uint32, dbId uint32, search string, revision uint32) keys.Key {
	if revision > 0 {
		return keys.NewKey(s.SubspaceName, s.version, UInt32ToByte(namespaceId), UInt32ToByte(dbId), search, keyEnd, UInt32ToByte(revision))
	}

	return keys.NewKey(s.SubspaceName, s.version, UInt32ToByte(namespaceId), UInt32ToByte(dbId), search, keyEnd)
}

// Put is to persist schema for a given namespace, database and search index.
func (s *SearchSchemaSubspace) Put(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, search string, schema []byte, revision uint32) error {
	return schemaPut(ctx, tx, s.getKey(namespaceId, dbId, search, revision), schema, revision)
}

// GetLatest returns the latest version stored for a collection inside a given namespace and database.
func (s *SearchSchemaSubspace) GetLatest(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, index string) (*schema.Version, error) {
	return schemaGetLatest(ctx, tx, s.getKey(namespaceId, dbId, index, 0))
}

// Get returns all the version stored for a collection inside a given namespace and database.
func (s *SearchSchemaSubspace) Get(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, index string) (schema.Versions, error) {
	return schemaGet(ctx, tx, s.getKey(namespaceId, dbId, index, 0))
}

// Delete is to remove schema for a given namespace, database and collection.
func (s *SearchSchemaSubspace) Delete(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, index string) error {
	return schemaDelete(ctx, tx, s.getKey(namespaceId, dbId, index, 0))
}

func schemaPut(ctx context.Context, tx transaction.Tx, key keys.Key, schema []byte, revision uint32) error {
	if revision <= 0 {
		return errors.InvalidArgument("invalid schema version %d", revision)
	}

	if len(schema) == 0 {
		return errors.InvalidArgument("empty schema")
	}

	err := tx.Insert(ctx, key, internal.NewTableData(schema))

	log.Debug().Err(err).Str("key", key.String()).Str("value", string(schema)).Msg("storing schema")

	return err
}

// getLatest returns the latest version stored for a collection inside a given namespace and database.
func schemaGetLatest(ctx context.Context, tx transaction.Tx, key keys.Key) (*schema.Version, error) {
	schemas, err := schemaGet(ctx, tx, key)
	if err != nil {
		return nil, err
	}

	if len(schemas) == 0 {
		return nil, nil
	}

	return &schemas[len(schemas)-1], nil
}

// Get returns all the version stored for a collection inside a given namespace and database.
func schemaGet(ctx context.Context, tx transaction.Tx, key keys.Key) (schema.Versions, error) {
	it, err := tx.Read(ctx, key, false)
	if err != nil {
		return nil, err
	}

	var (
		versions schema.Versions
		row      kv.KeyValue
	)

	// Do not count for metadata operations
	metrics.SetMetadataOperationInContext(ctx)

	for it.Next(&row) {
		ver, ok := row.Key[len(row.Key)-1].([]byte)
		if !ok {
			return nil, errors.Internal("not able to extract revision from schema %v", row.Key)
		}

		versions = append(versions, schema.Version{Version: ByteToUInt32(ver), Schema: row.Data.RawData})
	}

	if it.Err() != nil {
		return nil, it.Err()
	}

	sort.Sort(versions)

	return versions, nil
}

// Delete is to remove schema for a given namespace, database and collection.
func schemaDelete(ctx context.Context, tx transaction.Tx, key keys.Key) error {
	err := tx.Delete(ctx, key)

	log.Debug().Err(err).Str("key", key.String()).Msg("deleting schema succeed")

	return err
}
