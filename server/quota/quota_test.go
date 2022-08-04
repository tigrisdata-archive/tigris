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
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

var kvStore kv.KeyValueStore

func TestQuotaManager(t *testing.T) {
	txMgr := transaction.NewManager(kvStore)
	tenantMgr, ctx, cancel := metadata.NewTestTenantMgr(kvStore)
	defer cancel()

	m := newManager(tenantMgr, txMgr, &config.QuotaConfig{Enabled: true,
		RateLimit:            6,
		WriteThroughputLimit: 100,
		DataSizeLimit:        10000,
	})

	ns := fmt.Sprintf("ns-test-tenantQuota-1-%x", rand.Uint64())
	id := rand.Uint32()

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

	db1, err := tenant.GetDatabase(ctx, tx, "tenant_db1")
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

	sizeLimitUpdateInterval = 0
	require.Equal(t, ErrStorageSizeExceeded, m.check(ctx, ns, 0))
	sizeLimitUpdateInterval = 1
	require.Equal(t, ErrStorageSizeExceeded, m.check(ctx, ns, 0))

	require.NoError(t, kvStore.DropTable(ctx, table))

	sizeLimitUpdateInterval = 0

	require.NoError(t, m.check(ctx, ns, 0))

	for err != ErrRateExceeded {
		err = m.check(ctx, ns, 0)
	}
}

func TestMain(m *testing.M) {
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
