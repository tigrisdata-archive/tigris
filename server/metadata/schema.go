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
	"sort"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

var schVersion = []byte{0x01}

// SchemaSubspace is used to manage schemas in schema subspace.
type SchemaSubspace struct {
	MDNameRegistry
}

func NewSchemaStore(mdNameRegistry MDNameRegistry) *SchemaSubspace {
	return &SchemaSubspace{
		MDNameRegistry: mdNameRegistry,
	}
}

// Put is to persist schema for a given namespace, database and collection.
func (s *SchemaSubspace) Put(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32, schema []byte, revision int) error {
	if revision <= 0 {
		return errors.InvalidArgument("invalid schema version %d", revision)
	}
	if len(schema) == 0 {
		return errors.InvalidArgument("empty schema")
	}

	key := keys.NewKey(s.SchemaSubspaceName(), schVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), keyEnd, UInt32ToByte(uint32(revision)))
	if err := tx.Insert(ctx, key, internal.NewTableData(schema)); err != nil {
		log.Debug().Str("key", key.String()).Str("value", string(schema)).Err(err).Msg("storing schema failed")
		return err
	}

	log.Debug().Str("key", key.String()).Str("value", string(schema)).Msg("storing schema succeed")
	return nil
}

// GetLatest returns the latest version stored for a collection inside a given namespace and database.
func (s *SchemaSubspace) GetLatest(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32) ([]byte, int, error) {
	schemas, revisions, err := s.Get(ctx, tx, namespaceId, dbId, collId)
	if err != nil {
		return nil, 0, err
	}
	if len(schemas) == 0 {
		return nil, 0, nil
	}

	return schemas[len(schemas)-1], revisions[len(revisions)-1], nil
}

// Get returns all the version stored for a collection inside a given namespace and database.
func (s *SchemaSubspace) Get(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32) ([][]byte, []int, error) {
	key := keys.NewKey(s.SchemaSubspaceName(), schVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), keyEnd)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	revisionToSchemaMapping := make(map[uint32][]byte)
	var revisions []int
	var row kv.KeyValue
	for it.Next(&row) {
		revision, ok := row.Key[len(row.Key)-1].([]byte)
		if !ok {
			return nil, nil, errors.Internal("not able to extract revision from schema %v", row.Key)
		}
		revisionToSchemaMapping[ByteToUInt32(revision)] = row.Data.RawData
		revisions = append(revisions, int(ByteToUInt32(revision)))
	}
	if it.Err() != nil {
		return nil, nil, it.Err()
	}

	// sort revisions now
	sort.Ints(revisions)
	schemas := make([][]byte, 0, len(revisions))
	for _, r := range revisions {
		schema := revisionToSchemaMapping[uint32(r)]
		schemas = append(schemas, schema)
	}

	return schemas, revisions, nil
}

// Delete is to remove schema for a given namespace, database and collection.
func (s *SchemaSubspace) Delete(ctx context.Context, tx transaction.Tx, namespaceId uint32, dbId uint32, collId uint32) error {
	key := keys.NewKey(s.SchemaSubspaceName(), schVersion, UInt32ToByte(namespaceId), UInt32ToByte(dbId), UInt32ToByte(collId), keyEnd)
	if err := tx.Delete(ctx, key); err != nil {
		log.Debug().Str("key", key.String()).Err(err).Msg("deleting schema failed")
		return err
	}

	log.Debug().Str("key", key.String()).Msg("deleting schema succeed")
	return nil
}
