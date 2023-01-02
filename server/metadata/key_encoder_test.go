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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
)

func TestEncodeDecodeKey(t *testing.T) {
	coll := &schema.DefaultCollection{
		Id:   5,
		Name: "test_coll",
	}
	idx := &schema.Index{Id: 10}
	ns := NewTenantNamespace("test_ns", NewNamespaceMetadata(1, "test_ns", "test_ns-display_name"))
	db := &Database{
		id:   3,
		name: NewDatabaseName("test_db"),
		idToCollectionMap: map[uint32]string{
			coll.Id: coll.Name,
		},
	}

	mgr := &TenantManager{
		idToTenantMap: map[uint32]string{
			ns.Id(): ns.StrId(),
		},
		tenants: map[string]*Tenant{
			ns.StrId(): {
				namespace: ns,
				databases: map[string]*Database{
					db.Name(): db,
				},
				idToDatabaseMap: map[uint32]string{
					db.id: db.Name(),
				},
			},
		},
	}

	k := NewEncoder()
	encodedTable, err := k.EncodeTableName(ns, db, coll)
	require.NoError(t, err)
	require.Equal(t, internal.UserTableKeyPrefix, encodedTable[0:4])
	require.Equal(t, uint32(1), ByteToUInt32(encodedTable[4:8]))
	require.Equal(t, uint32(3), ByteToUInt32(encodedTable[8:12]))
	require.Equal(t, uint32(5), ByteToUInt32(encodedTable[12:16]))

	encodedIdx := k.EncodeIndexName(idx)
	require.Equal(t, uint32(10), ByteToUInt32(encodedIdx))

	tenantID, dbID, collID, ok := k.DecodeTableName(encodedTable)
	require.True(t, ok)

	tenantName, dbName, collName, ok := mgr.GetTableNameFromIds(tenantID, dbID, collID)
	require.True(t, ok)

	require.Equal(t, ns.StrId(), tenantName)
	require.Equal(t, db.Name(), dbName)
	require.Equal(t, coll.Name, collName)
	require.True(t, ok)
}
