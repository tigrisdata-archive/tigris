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

package quota

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

var kvStore kv.TxStore

func TestQuota(t *testing.T) {
	tenants, ctx, cancel := metadata.NewTestTenantMgr(t, kvStore)
	projName := "tenant_proj1"
	defer cancel()

	txMgr := transaction.NewManager(kvStore)

	rr := rand.New(rand.NewSource(time.Now().Unix())) //nolint:gosec

	ns := fmt.Sprintf("ns-test-tenantQuota-1-%x", rr.Uint64())
	id := rr.Uint32()

	tenant, err := tenants.CreateOrGetTenant(ctx, metadata.NewTenantNamespace(ns, metadata.NewNamespaceMetadata(id, ns, ns+"-display_name")))
	require.NoError(t, err)

	tx, err := txMgr.StartTx(context.TODO())
	require.NoError(t, err)

	err = tenant.CreateProject(ctx, tx, projName, nil)
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

	factory, err := schema.NewFactoryBuilder(true).Build("test_collection", jsSchema)
	require.NoError(t, err)

	err = tenant.Reload(ctx, tx, []byte("aaa"))
	require.NoError(t, err)

	proj1, err := tenant.GetProject(projName)
	require.NoError(t, err)

	require.NoError(t, tenant.CreateCollection(ctx, tx, proj1.GetMainDatabase(), factory))

	require.NoError(t, tx.Commit(context.TODO()))

	coll1 := proj1.GetMainDatabase().GetCollection("test_collection")
	table, err := metadata.NewEncoder().EncodeTableName(tenant.GetNamespace(), proj1.GetMainDatabase(), coll1)
	require.NoError(t, err)

	err = Init(tenants, &config.Config{
		Quota: config.QuotaConfig{
			Namespace: config.NamespaceLimitsConfig{
				Enabled: true,
				Default: config.LimitsConfig{
					ReadUnits:  100,
					WriteUnits: 50,
				},
				Node: config.LimitsConfig{
					ReadUnits:  10,
					WriteUnits: 5,
				},
				RefreshInterval: 1 * time.Second,
			},
			Node: config.LimitsConfig{
				Enabled:    true,
				ReadUnits:  10,
				WriteUnits: 10,
			},
			Storage: config.StorageLimitsConfig{
				Enabled:         true,
				RefreshInterval: 50 * time.Millisecond,
				DataSizeLimit:   100,
			},
		},
	})
	require.NoError(t, err)
	defer Cleanup()

	require.NoError(t, Allow(ctx, ns, 10, true))
	require.NoError(t, Allow(ctx, ns, 20, true))
	require.NoError(t, Allow(ctx, ns, 10, false))
	require.NoError(t, Allow(ctx, ns, 20, false))

	tx1, err := kvStore.BeginTx(ctx)
	require.NoError(t, err)
	docSize := 10 * 1024
	for i := 0; i < 10; i++ {
		err = tx1.Insert(ctx, table, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
		require.NoError(t, err)
	}
	_ = tx1.Commit(ctx)

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, ErrStorageSizeExceeded, Allow(ctx, ns, 1, true))
	require.NoError(t, Allow(ctx, ns, 0, false))
	require.Equal(t, ErrStorageSizeExceeded, Wait(ctx, ns, 1, true))
	require.NoError(t, Wait(ctx, ns, 0, false))

	i := 0
	for ; err != ErrReadUnitsExceeded && i < 15; i++ {
		err = Allow(ctx, ns, 2048, false)
	}

	if i != 10 && i != 11 {
		assert.Equal(t, 10, i)
	}

	i = 0
	for ; err != ErrWriteUnitsExceeded && err != ErrStorageSizeExceeded && i < 10; i++ {
		err = Allow(ctx, ns, 512, true) // < 1024 = 1 unit
	}
	assert.Equal(t, 1, i)

	require.NoError(t, kvStore.DropTable(ctx, table))
}

func TestMain(m *testing.M) {
	ulog.Configure(ulog.LogConfig{Level: "disabled", Format: "console"})

	metrics.InitializeMetrics()

	fdbCfg, err := config.GetTestFDBConfig("../..")
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB config: %v", err))
	}

	kvStore, err = kv.NewTxStore(fdbCfg)
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB KV %v", err))
	}

	os.Exit(m.Run())
}
