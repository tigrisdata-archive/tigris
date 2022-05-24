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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata/encoding"
)

func TestEncodeDecodeKey(t *testing.T) {
	coll := &schema.DefaultCollection{
		Id:   5,
		Name: "test_coll",
	}
	idx := &schema.Index{Id: 10}
	ns := NewTenantNamespace("test_ns", 1)
	db := &Database{
		id:   3,
		name: "test_db",
		idToCollectionMap: map[uint32]string{
			coll.Id: coll.Name,
		},
	}

	mgr := &TenantManager{
		idToTenantMap: map[uint32]string{
			ns.Id(): ns.Name(),
		},
		tenants: map[string]*Tenant{
			ns.Name(): {
				namespace: ns,
				databases: map[string]*Database{
					db.name: db,
				},
				idToDatabaseMap: map[uint32]string{
					db.id: db.Name(),
				},
			},
		},
	}

	k := NewEncoder(mgr)
	encodedTable, err := k.EncodeTableName(ns, db, coll)
	require.NoError(t, err)
	require.Equal(t, uint32(1), encoding.ByteToUInt32(encodedTable[0:4]))
	require.Equal(t, uint32(3), encoding.ByteToUInt32(encodedTable[4:8]))
	require.Equal(t, uint32(5), encoding.ByteToUInt32(encodedTable[8:12]))

	encodedIdx := k.EncodeIndexName(idx)
	require.Equal(t, uint32(10), encoding.ByteToUInt32(encodedIdx))

	tenantName, dbName, collName, ok := k.DecodeTableName(encodedTable)
	require.Equal(t, ns.Name(), tenantName)
	require.Equal(t, db.name, dbName)
	require.Equal(t, coll.Name, collName)
	require.True(t, ok)
}
