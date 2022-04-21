package kv

import "context"

type baseKeyValue struct {
	Key    Key
	FDBKey []byte
	Value  []byte
}

type baseKV interface {
	Insert(ctx context.Context, table []byte, key Key, data []byte) error
	Replace(ctx context.Context, table []byte, key Key, data []byte) error
	Delete(ctx context.Context, table []byte, key Key) error
	DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error
	Read(ctx context.Context, table []byte, key Key) (baseIterator, error)
	ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (baseIterator, error)
	Update(ctx context.Context, table []byte, key Key, apply func([]byte) ([]byte, error)) (int32, error)
	UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) (int32, error)
	SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
}

type baseIterator interface {
	Next(*baseKeyValue) bool
	Err() error
}

type baseTx interface {
	baseKV
	Commit(context.Context) error
	Rollback(context.Context) error
}

type baseKVStore interface {
	baseKV
	Tx(ctx context.Context) (baseTx, error)
	Batch() (baseTx, error)
	CreateTable(ctx context.Context, name []byte) error
	DropTable(ctx context.Context, name []byte) error
}
