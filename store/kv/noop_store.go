package kv

import (
	"context"
	"github.com/tigrisdata/tigris/internal"
)

type NoopIterator struct{}

func (n *NoopIterator) Next(value *KeyValue) bool { return false }
func (n *NoopIterator) Err() error                { return nil }

type NoopTx struct {
	*NoopKV
}

func (n *NoopTx) Commit(context.Context) error   { return nil }
func (n *NoopTx) Rollback(context.Context) error { return nil }
func (n *NoopTx) IsRetriable() bool              { return false }

// NoopKVStore is a noop store, useful if we need to profile/debug only compute and not with the storage. This can be
// initialized in main.go instead of using default kvStore.
type NoopKVStore struct {
	*NoopKV
}

func (n *NoopKVStore) BeginTx(ctx context.Context) (Tx, error)            { return &NoopTx{}, nil }
func (n *NoopKVStore) CreateTable(ctx context.Context, name []byte) error { return nil }
func (n *NoopKVStore) DropTable(ctx context.Context, name []byte) error   { return nil }
func (n *NoopKVStore) GetInternalDatabase() (interface{}, error)          { return nil, nil }

type NoopKV struct{}

func (n *NoopKV) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	return nil
}
func (n *NoopKV) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	return nil
}
func (n *NoopKV) Delete(ctx context.Context, table []byte, key Key) error                 { return nil }
func (n *NoopKV) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error { return nil }
func (n *NoopKV) Read(ctx context.Context, table []byte, key Key) (Iterator, error) {
	return &NoopIterator{}, nil
}
func (n *NoopKV) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (Iterator, error) {
	return &NoopIterator{}, nil
}
func (n *NoopKV) Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	return 0, nil
}
func (n *NoopKV) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	return 0, nil
}
func (n *NoopKV) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error {
	return nil
}
func (n *NoopKV) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error {
	return nil
}
func (n *NoopKV) Get(ctx context.Context, key []byte) ([]byte, error) { return nil, nil }
