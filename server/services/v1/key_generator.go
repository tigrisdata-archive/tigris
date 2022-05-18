package v1

import (
	"context"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata/encoding"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

const (
	// generatorSubspaceKey is used to store ids in storage so that we can guarantee uniqueness
	generatorSubspaceKey = "generator"
	// int32IdKey is the prefix after generator subspace to store int32 counters
	int32IdKey = "int32_id"
)

type generator struct {
	txMgr *transaction.Manager
}

func newGenerator(txMgr *transaction.Manager) *generator {
	return &generator{
		txMgr: txMgr,
	}
}

// get returns generated id for the supported primary key fields.
func (g *generator) get(ctx context.Context, table []byte, field *schema.Field) ([]byte, error) {
	switch field.Type() {
	case schema.StringType, schema.UUIDType:
		return []byte(fmt.Sprintf(`"%s"`, uuid.NewUUIDAsString())), nil
	case schema.ByteType:
		return jsoniter.Marshal([]byte(uuid.NewUUIDAsString()))
	case schema.DateTimeType:
		// use timestamp nano to reduce the contention if multiple workers end up generating same timestamp.
		return []byte(fmt.Sprintf(`"%s"`, time.Now().UTC().Format(time.RFC3339Nano))), nil
	case schema.Int64Type:
		// use timestamp nano to reduce the contention if multiple workers end up generating same timestamp.
		return []byte(fmt.Sprintf(`%d`, time.Now().UTC().UnixNano())), nil
	case schema.Int32Type:
		return g.generateInTx(ctx, table)
	}
	return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported type found in auto-generator")
}

// generateInTx is used to generate an id in a transaction for int32 field only. This is mainly used to guarantee
// uniqueness with auto-incremented ids, so what we are doing is reserving this id in storage before returning to the
// caller so that only one id is assigned to one caller.
func (g *generator) generateInTx(ctx context.Context, table []byte) ([]byte, error) {
	for {
		tx, err := g.txMgr.StartTx(ctx)
		if err != nil {
			return nil, err
		}

		var valueI32 int32
		if valueI32, err = g.generateInt(ctx, tx, table); err != nil {
			_ = tx.Rollback(ctx)
		}

		if err = tx.Commit(ctx); err == nil {
			return []byte(fmt.Sprintf(`%d`, valueI32)), nil
		}
		if err != kv.ErrConflictingTransaction {
			return nil, err
		}
	}
}

// generateInt as it is used to generate int32 value, we are simply maintaining a counter. There is a contention to
// generate a counter if it is concurrently getting executed but the generation should be fast then it is best to start
// with this approach.
func (g *generator) generateInt(ctx context.Context, tx transaction.Tx, table []byte) (int32, error) {
	key := keys.NewKey([]byte(generatorSubspaceKey), table, int32IdKey)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return 0, err
	}

	id := uint32(1)
	var row kv.KeyValue
	if it.Next(&row) {
		id = encoding.ByteToUInt32(row.Data.RawData) + uint32(1)
	}
	if err := it.Err(); err != nil {
		return 0, err
	}

	if err := tx.Replace(ctx, key, internal.NewTableData(encoding.UInt32ToByte(id))); err != nil {
		return 0, err
	}

	return int32(id), nil
}
