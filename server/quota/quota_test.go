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
	"os"
	"testing"
	"time"

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

var kvStore kv.KeyValueStore

func TestQuotaManager(t *testing.T) {
	txMgr := transaction.NewManager(kvStore)
	tenantMgr, ctx, cancel := metadata.NewTestTenantMgr(kvStore)
	defer cancel()

	m := newManager(tenantMgr, txMgr, &config.QuotaConfig{
		Enabled:              true,
		RateLimit:            6,
		WriteThroughputLimit: 100,
		DataSizeLimit:        10000,
	})

	ns := fmt.Sprintf("ns-test-tenantQuota-1-%x", rand.Uint64()) //nolint:golint,gosec
	id := rand.Uint32()                                          //nolint:golint,gosec

	tenant, err := tenantMgr.CreateOrGetTenant(ctx, txMgr, metadata.NewTenantNamespace(ns, id))
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
	docSize := 10 * 1024
	table, err := metadata.NewEncoder().EncodeTableName(tenant.GetNamespace(), db1, coll1)
	require.NoError(t, err)

	require.NoError(t, kvStore.DropTable(ctx, table))

	require.NoError(t, m.check(ctx, ns, 10))
	require.NoError(t, m.check(ctx, ns, 20))

	require.Equal(t, ErrThroughputExceeded, m.check(ctx, ns, 500))

	for i := 0; i < 10; i++ {
		err = kvStore.Insert(ctx, table, kv.BuildKey(fmt.Sprintf("aaa%d", i)), &internal.TableData{RawData: make([]byte, docSize)})
		require.NoError(t, err)
	}

	m.cfg.LimitUpdateInterval = 0
	require.Equal(t, ErrStorageSizeExceeded, m.check(ctx, ns, 0))
	m.cfg.LimitUpdateInterval = 1
	require.Equal(t, ErrStorageSizeExceeded, m.check(ctx, ns, 0))

	require.NoError(t, kvStore.DropTable(ctx, table))

	m.cfg.LimitUpdateInterval = 0

	require.NoError(t, m.check(ctx, ns, 0))

	for err != ErrRateExceeded {
		err = m.check(ctx, ns, 0)
	}

	Init(tenantMgr, txMgr, &config.QuotaConfig{
		Enabled:              false,
		RateLimit:            6,
		WriteThroughputLimit: 100,
		DataSizeLimit:        10000,
	})
	s := GetState(ns)
	require.Nil(t, Allow(ctx, ns, 0))
	mgr.cfg.Enabled = true
	require.Nil(t, Allow(ctx, ns, 0))
	require.NotNil(t, s)
}

func TestMain(m *testing.M) {
	metrics.InitializeMetrics()
	rand.Seed(time.Now().Unix())

	ulog.Configure(ulog.LogConfig{Level: "disabled", Format: "console"})

	fdbCfg, err := config.GetTestFDBConfig("../..")
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB config: %v", err))
	}

	kvStore, err = kv.NewKeyValueStore(fdbCfg)
	if err != nil {
		panic(fmt.Sprintf("failed to init FDB KV %v", err))
	}

	os.Exit(m.Run())
}
