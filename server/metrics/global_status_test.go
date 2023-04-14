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

package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/server/config"
)

func TestRequestStatus(t *testing.T) {
	t.Run("Test RequestStatus", func(t *testing.T) {
		dataSize8K := int64(8192)
		dataSize16K := int64(16384)
		ctx := context.Background()
		rs := NewRequestStatus("test_tenant")
		ctx = rs.SaveRequestStatusToContext(ctx)
		_, ok := RequestStatusFromContext(ctx)
		assert.True(t, ok)
		ctx = rs.ClearRequestStatusFromContext(ctx)
		_, ok = RequestStatusFromContext(ctx)
		assert.False(t, ok)

		rs.AddReadBytes(dataSize8K)
		rs.AddWriteBytes(dataSize8K)
		rs.AddDDLDropUnit()
		rs.AddDDLUpdateUnit()
		rs.AddDDLCreateUnit()
		config.DefaultConfig.GlobalStatus.Enabled = false
		rs.AddReadBytes(dataSize16K)
		rs.AddWriteBytes(dataSize16K)
		rs.AddDDLDropUnit()
		rs.AddDDLUpdateUnit()
		rs.AddDDLCreateUnit()
		config.DefaultConfig.GlobalStatus.Enabled = true
		assert.Equal(t, dataSize8K, rs.GetReadBytes())
		assert.Equal(t, dataSize8K, rs.GetWriteBytes())
		assert.Equal(t, int64(1), rs.GetDDLDropUnits())
		assert.Equal(t, int64(1), rs.GetDDLCreateUnits())
		assert.Equal(t, int64(1), rs.GetDDLUpdateUnits())

		rs.SetReadBytes(int64(0))
		rs.SetWriteBytes(int64(0))
		assert.Equal(t, int64(0), rs.GetReadBytes())
		assert.Equal(t, int64(0), rs.GetWriteBytes())

		rs.SetReadBytes(dataSize16K)
		rs.SetWriteBytes(dataSize16K)
		assert.Equal(t, dataSize16K, rs.GetReadBytes())
		assert.Equal(t, dataSize16K, rs.GetWriteBytes())

		rs.SetCollectionSearchType()
		assert.True(t, rs.IsCollectionSearch())
		assert.False(t, rs.IsApiSearch())
		rs.SetApiSearchType()
		assert.True(t, rs.IsApiSearch())
		assert.False(t, rs.IsCollectionSearch())
		rs.AddSearchUnit()
		assert.Equal(t, int64(1), rs.GetApiSearchUnits())
		rs.AddCollectionSearchUnit()
		assert.Equal(t, int64(1), rs.GetCollectionSearchUnits())
		rs.AddSearchCreateIndexUnit()
		assert.Equal(t, int64(1), rs.GetSearchCreateIndexUnits())
		rs.AddSearchDropIndexUnit()
		assert.Equal(t, int64(1), rs.GetSearchDropIndexUnits())
		rs.AddSearchDeleteDocumentUnit(2)
		assert.Equal(t, int64(2), rs.GetSearchDeleteDocumentUnits())

		config.DefaultConfig.GlobalStatus.Enabled = false
		rs.AddSearchCreateIndexUnit()
		assert.Equal(t, int64(1), rs.GetSearchCreateIndexUnits())
		rs.AddSearchDropIndexUnit()
		assert.Equal(t, int64(1), rs.GetSearchDropIndexUnits())
		rs.AddSearchDeleteDocumentUnit(3)
		assert.Equal(t, int64(2), rs.GetSearchDeleteDocumentUnits())
		rs.AddSearchUnit()
		assert.Equal(t, int64(1), rs.GetApiSearchUnits())
		rs.AddCollectionSearchUnit()
		assert.Equal(t, int64(1), rs.GetCollectionSearchUnits())
		config.DefaultConfig.GlobalStatus.Enabled = true
	})
}

func TestGlobalStatus(t *testing.T) {
	t.Run("Test Global status", func(t *testing.T) {
		dataSize16K := int64(16384)

		globalStatus := NewGlobalStatus()
		assert.NotNil(t, globalStatus)
		assert.NotNil(t, globalStatus.activeChunk)

		rs := NewRequestStatus("test_tenant")
		rs.AddReadBytes(dataSize16K)
		rs.AddWriteBytes(dataSize16K)
		rs.AddReadBytes(dataSize16K)
		rs.AddDDLDropUnit()
		rs.AddDDLUpdateUnit()
		rs.AddDDLCreateUnit()

		config.DefaultConfig.GlobalStatus.Enabled = false
		globalStatus.RecordRequestToActiveChunk(rs, "test_tenant")
		config.DefaultConfig.GlobalStatus.Enabled = true
		globalStatus.RecordRequestToActiveChunk(rs, "test_tenant")
		assert.Equal(t, globalStatus.activeChunk.Tenants["test_tenant"].readBytes, 2*dataSize16K)
		assert.Equal(t, globalStatus.activeChunk.Tenants["test_tenant"].writeBytes, dataSize16K)
		assert.Equal(t, globalStatus.activeChunk.Tenants["test_tenant"].ddlDropUnits, int64(1))
		assert.Equal(t, globalStatus.activeChunk.Tenants["test_tenant"].ddlCreateUnits, int64(1))
		assert.Equal(t, globalStatus.activeChunk.Tenants["test_tenant"].ddlUpdateUnits, int64(1))

		oldChunk := globalStatus.Flush()
		assert.Equal(t, oldChunk.Tenants["test_tenant"].readBytes, 2*dataSize16K)
		assert.Equal(t, oldChunk.Tenants["test_tenant"].writeBytes, dataSize16K)
		assert.Equal(t, oldChunk.Tenants["test_tenant"].ddlDropUnits, int64(1))
		assert.Equal(t, oldChunk.Tenants["test_tenant"].ddlUpdateUnits, int64(1))
		assert.Equal(t, oldChunk.Tenants["test_tenant"].ddlCreateUnits, int64(1))

		_, ok := globalStatus.activeChunk.Tenants["test_tenant"]
		assert.False(t, ok)
	})
}

func TestSecondaryIndexFields(t *testing.T) {
	t.Run("Test determining secondary indexes", func(t *testing.T) {
		rs := NewRequestStatus("test_tenant")
		notSecondaryIndex := []byte("foobar")
		secondaryIndex := []byte("skeysomething")
		ignoredSecondaryIndexCreatedAt := []byte("skey_foobar_tigris_created_at_something")
		ignoredSecondaryIndexUpdatedAt := []byte("skey_foobar_tigris_updated_at_something")
		assert.False(t, rs.IsKeySecondaryIndex(notSecondaryIndex))
		assert.True(t, rs.IsKeySecondaryIndex(secondaryIndex))
		assert.True(t, rs.IsKeySecondaryIndex(ignoredSecondaryIndexCreatedAt))
		assert.True(t, rs.IsKeySecondaryIndex(ignoredSecondaryIndexUpdatedAt))

		assert.False(t, rs.IsSecondaryIndexFieldIgnored(secondaryIndex))
		assert.True(t, rs.IsSecondaryIndexFieldIgnored(ignoredSecondaryIndexCreatedAt))
		assert.True(t, rs.IsSecondaryIndexFieldIgnored(ignoredSecondaryIndexUpdatedAt))
	})
}
