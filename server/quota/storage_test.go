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

package quota

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

func TestStorageQuota(t *testing.T) {
	tenants, ctx, cancel := metadata.NewTestTenantMgr(kvStore)
	defer cancel()

	txMgr := transaction.NewManager(kvStore)

	ns := fmt.Sprintf("ns-test-tenantQuota-1-%x", rand.Uint64()) //nolint:golint,gosec
	id := rand.Uint32()                                          //nolint:golint,gosec

	tenant, err := tenants.CreateOrGetTenant(ctx, metadata.NewTenantNamespace(ns, id))
	require.NoError(t, err)

	tx, err := txMgr.StartTx(context.TODO())
	require.NoError(t, err)

	_, err = tenant.CreateDatabase(ctx, tx, "tenant_db1")
	require.NoError(t, err)

	jsSchema := []byte(`{
        "title": "test_collection",
		"properties": {
			"K1": { "type": "string" },
			"K2": { "type": "integer" },
			"D1": { "type": "string", "maxLength": 128 }
          },
		  "primary_key": ["K1", "K2"]
	    }`)

	factory, err := schema.Build("test_collection", jsSchema)
	require.NoError(t, err)

	err = tenant.Reload(ctx, tx, []byte("aaa"))
	require.NoError(t, err)

	db1, err := tenant.GetDatabase(ctx, "tenant_db1")
	require.NoError(t, err)

	require.NoError(t, tenant.CreateCollection(ctx, tx, db1, factory))

	require.NoError(t, tx.Commit(context.TODO()))

	coll1 := db1.GetCollection("test_collection")
	table, err := metadata.NewEncoder().EncodeTableName(tenant.GetNamespace(), db1, coll1)
	require.NoError(t, err)

	m := initStorage(tenants, &config.QuotaConfig{
		Storage: config.StorageLimitsConfig{
			Enabled:         true,
			RefreshInterval: 50 * time.Millisecond,
			DataSizeLimit:   100,
		},
	})

	require.NoError(t, m.Allow(ctx, ns, 10, true))
	require.NoError(t, m.Allow(ctx, ns, 20, true))
	require.NoError(t, m.Allow(ctx, ns, 10, false))
	require.NoError(t, m.Allow(ctx, ns, 20, false))

	docSize := 10 * 1024
	for i := 0; i < 10; i++ {
		err = kvStore.Insert(ctx, table, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, ErrStorageSizeExceeded, m.Allow(ctx, ns, 0, true))
	require.NoError(t, m.Allow(ctx, ns, 0, false))
	require.Equal(t, ErrStorageSizeExceeded, m.Wait(ctx, ns, 0, true))
	require.NoError(t, m.Wait(ctx, ns, 0, false))

	m.Cleanup()
	require.NoError(t, kvStore.DropTable(ctx, table))
}
