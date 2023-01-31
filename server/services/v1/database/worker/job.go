package worker

import (
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/transaction"
)

type DocumentJob interface {
	StartCollWork(coll *schema.DefaultCollection) bool
	FinishCollWork(coll *schema.DefaultCollection) error

	ProcessRow(tx *transaction.Tx, row map[string]any) error
}
