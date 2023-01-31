package database

import (
	"context"
	"time"

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
)

var rowsPerTransaction int = 16

func RepairRow(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, row *Row, version int32) error {
	var err error

	row.Data.RawData, err = coll.UpdateRowSchemaRaw(row.Data.RawData, version)
	if err != nil {
		return err
	}

	row.Data.Ver = version
	row.Data.UpdatedAt = internal.NewTimestamp()

	key, err := keys.FromBinary(coll.EncodedName, row.Key)
	if err != nil {
		return err
	}

	if err = tx.Replace(ctx, key, row.Data, false); err != nil {
		return err
	}

	return nil
}

func RepairSchema(version int32, coll *schema.DefaultCollection, from keys.Key, maxCount int) (keys.Key, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if from == nil {
		from = keys.NewKey(coll.EncodedName)
	}

	txMgr := transaction.NewManager()

	tx, err := txMgr.StartTx(ctx)
	if err != nil {
		return nil, err
	}

	reader := NewDatabaseReader(ctx, tx)

	it, err := reader.ScanIterator(from)
	if err != nil {
		return nil, err
	}

	var (
		row Row
		key keys.Key
	)

	for i := 0; (maxCount == 0 || i < maxCount) && it.Next(&row); i++ {
		tx1, err := txMgr.StartTx(ctx)
		if err != nil {
			return nil, err
		}

		for j := 0; j < rowsPerTransaction; j++ {
			if err = RepairRow(ctx, tx, coll, &row, version); err != nil {
				_ = tx.Rollback(ctx)
				return nil, err
			}
		}

		if err = tx1.Commit(ctx); err != nil {
			return nil, err
		}
	}

	if it.Interrupted() != nil {
		return nil, it.Interrupted()
	}

	return key, nil
}

type WorkerState struct {
	nsIdx int
	namespaces []metadata.Namespace
	curTenant *metadata.Tenant

	projectIdx int
	projects []string
	curProject *metadata.Project

	collId int
	collections []string
	curColl *schema.DefaultCollection
	curCollMeta *metadata.CollectionMetadata

	tenantMgr *metadata.TenantManager
}

func (w *WorkerState) nextNamespace(ctx context.Context) (*metadata.Tenant, error) {
	if w.nsIdx >= len(w.namespaces) {
		return nil, nil
	}

	t, err := w.tenantMgr.GetTenant(ctx, w.namespaces[w.nsIdx].Metadata().Name)
	if err != nil {
		return nil, err
	}

	w.nsIdx++

	return t, nil
}

func (w *WorkerState) nextProj(ctx context.Context) (*metadata.Project, error) {
	if w.projectIdx >= len(w.projects) {
		t, err := w.nextNamespace(ctx)
		if err != nil {
			return nil, err
		}

		if t == nil {
			return nil, nil
		}

		w.curTenant = t
		w.projects = t.ListProjects(ctx)
		w.projectIdx = 0

		if len(w.projects) == 0 {
			return nil, nil
		}
	}

	p, err := w.curTenant.GetProject(w.projects[w.projectIdx])
	if err != nil {
		return nil, err
	}

	w.curProject = p

	w.projectIdx++

	return p, nil
}

func (w *WorkerState) nextColl(ctx context.Context) (*schema.DefaultCollection, error) {
	if w.collId >= len(w.collections) {
		p, err := w.nextProj(ctx)
		if err != nil {
			return nil, err
		}

		if p == nil {
			return nil, nil
		}
	}

	c := w.collections[w.collId]

	w.collId++

	w.curProject.GetMainDatabase().ListCollections()

	return c, nil
}

func pickAJob(ctx context.Context, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) (*schema.DefaultCollection, error) {
	w := WorkerState{}

	c, err := w.nextColl(ctx)


	ctx := context.Background()

	collMetaStore := metadata.NewCollectionStore(metadata.DefaultNameRegistry)

	err := txMgr.Tx(ctx, func(ctx context.Context, tx transaction.Tx) {
		tenantMgr.ListNamespaces(ctx, tx)
	} )


	tenantMgr.ListNamespaces(ctx, tx,)
	tenantMgr.GetTenant(ctx, ns)

	for _, ns := range tenant.ListProjects( {

	}
	for _, p := range tenant.ListProjects(ctx) {
		proj, err := tenant.GetProject(p)

		for _, c := range proj.GetMainDatabase().ListCollections() {
			tx, err := txMgr.StartTx(ctx)
			if err != nil {
				return nil, err
			}

			meta, err := collMetaStore.Get(ctx, tx, tenant.GetNamespace().Id(), proj.Id(), c.Id)

			meta.OldestSchemaVersion

		}
	}
}

func RepairSchemaLoop(tenantMgr *metadata.TenantManager) {
	for {
		coll, err := pickAJob(tenantMgr)
		ulog.E(err)

	}
}
