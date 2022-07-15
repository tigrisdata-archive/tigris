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

package kv

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	ulog "github.com/tigrisdata/tigris/util/log"
)

const (
	maxTxSizeBytes = 10000000

	fdbAPIVersion = 710
)

// fdbkv is an implementation of kv on top of FoundationDB
type fdbkv struct {
	db fdb.Database
}

type fbatch struct {
	db  *fdbkv
	tx  baseTx
	rtx fdb.Transaction
}

type ftx struct {
	d   *fdbkv
	tx  *fdb.Transaction
	err error
}

type fdbIterator struct {
	it       *fdb.RangeIterator
	subspace subspace.Subspace
	err      error
}

type fdbIteratorTxCloser struct {
	baseIterator
	tx baseTx
}

// newFoundationDB initializes instance of FoundationDB KV interface implementation
func newFoundationDB(cfg *config.FoundationDBConfig) (*fdbkv, error) {
	d := &fdbkv{}
	if err := d.init(cfg); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *fdbkv) init(cfg *config.FoundationDBConfig) (err error) {
	log.Info().Int("api_version", fdbAPIVersion).Str("cluster_file", cfg.ClusterFile).Msg("initializing foundation db")
	fdb.MustAPIVersion(fdbAPIVersion)
	d.db, err = fdb.OpenDatabase(cfg.ClusterFile)
	log.Err(err).Msg("initialized foundation db")
	return
}

// Read returns all the keys which has prefix equal to "key" parameter
func (d *fdbkv) Read(ctx context.Context, table []byte, key Key) (baseIterator, error) {
	tx, err := d.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	it, err := tx.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}
	return &fdbIteratorTxCloser{it, tx}, nil
}

func (d *fdbkv) ReadRange(ctx context.Context, table []byte, lKey Key, rKey Key) (baseIterator, error) {
	tx, err := d.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	it, err := tx.ReadRange(ctx, table, lKey, rKey)
	if err != nil {
		return nil, err
	}
	return &fdbIteratorTxCloser{it, tx}, nil
}

func (d *fdbkv) txWithRetry(ctx context.Context, fn func(fdb.Transaction) (interface{}, error)) (interface{}, error) {
	for {
		retry, res, err := d.txWithRetryLow(ctx, fn)
		if !retry {
			return res, err
		}
	}
}

func (d *fdbkv) txWithRetryLow(ctx context.Context, fn func(fdb.Transaction) (interface{}, error)) (bool, interface{}, error) {
	tr, err := d.db.CreateTransaction()
	defer tr.Cancel()

	if err != nil {
		return false, nil, err
	}

	if err := setTxTimeout(&tr, getCtxTimeout(ctx)); err != nil {
		return false, nil, err
	}

	var res interface{}
	if res, err = fn(tr); err != nil {
		return false, nil, err
	}

	if err := tr.Commit().Get(); err == nil {
		return false, res, nil
	}

	var ep fdb.Error
	if errors.As(err, &ep) {
		// OnError returns nil if error is retryable
		err = tr.OnError(ep).Get()
	}

	if err != nil {
		return false, nil, err
	}

	return true, nil, nil
}

func (d *fdbkv) Insert(ctx context.Context, table []byte, key Key, data []byte) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).Insert(ctx, table, key, data)
	})
	return err
}

func (d *fdbkv) Replace(ctx context.Context, table []byte, key Key, data []byte) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).Replace(ctx, table, key, data)
	})
	return err
}

func (d *fdbkv) Delete(ctx context.Context, table []byte, key Key) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).Delete(ctx, table, key)
	})
	return err
}

func (d *fdbkv) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).DeleteRange(ctx, table, lKey, rKey)
	})
	return err
}

func (d *fdbkv) Update(ctx context.Context, table []byte, key Key, apply func([]byte) ([]byte, error)) (int32, error) {
	count, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return (&ftx{d: d, tx: &tr}).Update(ctx, table, key, apply)
	})
	return count.(int32), err
}

func (d *fdbkv) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) (int32, error) {
	count, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return (&ftx{d: d, tx: &tr}).UpdateRange(ctx, table, lKey, rKey, apply)
	})
	return count.(int32), err
}

func (d *fdbkv) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).SetVersionstampedValue(ctx, key, value)
	})
	return err
}

func (d *fdbkv) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).SetVersionstampedKey(ctx, key, value)
	})
	return err
}

func (d *fdbkv) Get(ctx context.Context, key []byte) ([]byte, error) {
	val, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return (&ftx{d: d, tx: &tr}).Get(ctx, key)
	})
	return val.([]byte), err
}

func (d *fdbkv) CreateTable(_ context.Context, name []byte) error {
	log.Debug().Str("name", string(name)).Msg("table created")
	return nil
}

func (d *fdbkv) DropTable(ctx context.Context, name []byte) error {
	s := subspace.FromBytes(name)

	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(s)
		return nil, nil
	})

	log.Err(err).Str("name", string(name)).Msg("table dropped")

	return nil
}

func (d *fdbkv) Batch() (baseTx, error) {
	tx, err := d.db.CreateTransaction()
	if ulog.E(err) {
		return nil, err
	}
	log.Debug().Msg("create batch")
	b := &fbatch{db: d, tx: &ftx{d: d, tx: &tx}, rtx: tx}
	return b, nil
}

func (b *fbatch) flushBatch(ctx context.Context, _ Key, _ Key, data []byte) error {
	fsz := b.rtx.GetApproximateSize()
	sz, err := fsz.Get()
	if ulog.E(err) {
		return err
	}

	//FIXME: Include lkey and rKey in size calculation

	if sz+int64(len(data)) > maxTxSizeBytes {
		log.Debug().Int64("size", sz).Msg("flush batch")
		err = b.tx.Commit(ctx)
		if ulog.E(err) {
			return err
		}
		tx, err := b.db.db.CreateTransaction()
		if ulog.E(err) {
			return err
		}
		b.rtx = tx
		b.tx = &ftx{d: b.db, tx: &tx}
	}

	return nil
}

func (b *fbatch) Insert(ctx context.Context, table []byte, key Key, data []byte) error {
	if err := b.flushBatch(ctx, key, nil, data); err != nil {
		return err
	}
	return b.tx.Insert(ctx, table, key, data)
}

func (b *fbatch) Replace(ctx context.Context, table []byte, key Key, data []byte) error {
	if err := b.flushBatch(ctx, key, nil, data); err != nil {
		return err
	}
	return b.tx.Replace(ctx, table, key, data)
}

func (b *fbatch) Delete(ctx context.Context, table []byte, key Key) error {
	if err := b.flushBatch(ctx, key, nil, nil); err != nil {
		return err
	}
	return b.tx.Delete(ctx, table, key)
}

func (b *fbatch) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error {
	if err := b.flushBatch(ctx, lKey, rKey, nil); err != nil {
		return err
	}
	return b.tx.DeleteRange(ctx, table, lKey, rKey)
}

func (b *fbatch) Update(ctx context.Context, table []byte, key Key, apply func([]byte) ([]byte, error)) (int32, error) {
	if err := b.flushBatch(ctx, key, nil, nil); err != nil {
		return -1, err
	}
	return b.tx.Update(ctx, table, key, apply)
}

func (b *fbatch) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) (int32, error) {
	if err := b.flushBatch(ctx, lKey, rKey, nil); err != nil {
		return -1, err
	}
	return b.tx.UpdateRange(ctx, table, lKey, rKey, apply)
}

func (b *fbatch) Read(ctx context.Context, table []byte, key Key) (baseIterator, error) {
	if err := b.flushBatch(ctx, key, nil, nil); err != nil {
		return nil, err
	}
	return b.tx.Read(ctx, table, key)
}

func (b *fbatch) ReadRange(ctx context.Context, table []byte, lKey Key, rKey Key) (baseIterator, error) {
	if err := b.flushBatch(ctx, lKey, rKey, nil); err != nil {
		return nil, err
	}
	return b.tx.ReadRange(ctx, table, lKey, rKey)
}

func (b *fbatch) SetVersionstampedValue(_ context.Context, _ []byte, _ []byte) error {
	return fmt.Errorf("batch doesn't support setting versionstamped value")
}

func (b *fbatch) SetVersionstampedKey(_ context.Context, _ []byte, _ []byte) error {
	return fmt.Errorf("batch doesn't support setting versionstamped key")
}

func (b *fbatch) Get(_ context.Context, _ []byte) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *fbatch) Commit(ctx context.Context) error {
	return b.tx.Commit(ctx)
}

func (b *fbatch) Rollback(ctx context.Context) error {
	return b.tx.Rollback(ctx)
}

func (b *fbatch) IsRetriable() bool {
	return false
}

func (d *fdbkv) BeginTx(ctx context.Context) (baseTx, error) {
	tx, err := d.db.CreateTransaction()
	if ulog.E(err) {
		return nil, err
	}

	if err := setTxTimeout(&tx, getCtxTimeout(ctx)); err != nil {
		return nil, err
	}

	log.Debug().Msg("create transaction")
	return &ftx{d: d, tx: &tx}, nil
}

func (t *ftx) Insert(ctx context.Context, table []byte, key Key, data []byte) error {
	listener := GetEventListener(ctx)
	k := getFDBKey(table, key)

	// Read the value and if exists reject the request.
	v := t.tx.Get(k)
	vv, err := v.Get()
	if err != nil {
		return err
	}
	if vv != nil {
		return ErrDuplicateKey
	}

	t.tx.Set(k, data)
	listener.OnSet(InsertEvent, table, k, data)

	log.Debug().Str("table", string(table)).Interface("key", key).Msg("Insert")

	return err
}

func (t *ftx) Replace(ctx context.Context, table []byte, key Key, data []byte) error {
	listener := GetEventListener(ctx)
	k := getFDBKey(table, key)

	t.tx.Set(k, data)
	listener.OnSet(ReplaceEvent, table, k, data)

	log.Debug().Str("table", string(table)).Interface("key", key).Msg("tx Replace")

	return nil
}

func (t *ftx) Delete(ctx context.Context, table []byte, key Key) error {
	listener := GetEventListener(ctx)
	kr, err := fdb.PrefixRange(getFDBKey(table, key))
	if ulog.E(err) {
		return err
	}

	t.tx.ClearRange(kr)
	listener.OnClearRange(DeleteEvent, table, kr.Begin.FDBKey(), kr.End.FDBKey())

	log.Debug().Str("table", string(table)).Interface("key", key).Msg("tx delete")

	return nil
}

func (t *ftx) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error {
	listener := GetEventListener(ctx)
	lk := getFDBKey(table, lKey)
	rk := getFDBKey(table, rKey)

	t.tx.ClearRange(fdb.KeyRange{Begin: lk, End: rk})
	listener.OnClearRange(DeleteRangeEvent, table, lk, rk)

	log.Debug().Str("table", string(table)).Interface("lKey", lKey).Interface("rKey", rKey).Msg("tx delete range")

	return nil
}

func (t *ftx) Update(ctx context.Context, table []byte, key Key, apply func([]byte) ([]byte, error)) (int32, error) {
	listener := GetEventListener(ctx)
	k, err := fdb.PrefixRange(getFDBKey(table, key))
	if ulog.E(err) {
		return -1, err
	}

	r := t.tx.GetRange(k, fdb.RangeOptions{})
	it := r.Iterator()

	modifiedCount := int32(0)
	for it.Advance() {
		kv, err := it.Get()
		if ulog.E(err) {
			return -1, err
		}
		v, err := apply(kv.Value)
		if ulog.E(err) {
			return -1, err
		}

		t.tx.Set(kv.Key, v)
		listener.OnSet(UpdateEvent, table, kv.Key, v)

		modifiedCount++
	}

	log.Debug().Str("table", string(table)).Interface("Key", key).Msg("tx update")

	return modifiedCount, nil
}

func (t *ftx) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) (int32, error) {
	listener := GetEventListener(ctx)
	lk := getFDBKey(table, lKey)
	rk := getFDBKey(table, rKey)

	r := t.tx.GetRange(fdb.KeyRange{Begin: lk, End: rk}, fdb.RangeOptions{})

	modifiedCount := int32(0)
	it := r.Iterator()
	for it.Advance() {
		kv, err := it.Get()
		if ulog.E(err) {
			return -1, err
		}
		v, err := apply(kv.Value)
		if ulog.E(err) {
			return -1, err
		}

		t.tx.Set(kv.Key, v)
		listener.OnSet(UpdateRangeEvent, table, kv.Key, v)

		modifiedCount++
	}

	log.Debug().Str("table", string(table)).Interface("lKey", lKey).Interface("rKey", rKey).Msg("tx update range")

	return modifiedCount, nil
}

func (t *ftx) Read(_ context.Context, table []byte, key Key) (baseIterator, error) {
	k, err := fdb.PrefixRange(getFDBKey(table, key))
	if ulog.E(err) {
		return nil, err
	}

	r := t.tx.GetRange(k, fdb.RangeOptions{})

	return &fdbIterator{it: r.Iterator(), subspace: subspace.FromBytes(table)}, nil
}

func (t *ftx) ReadRange(_ context.Context, table []byte, lKey Key, rKey Key) (baseIterator, error) {
	lk := getFDBKey(table, lKey)
	rk := getFDBKey(table, rKey)

	r := t.tx.GetRange(fdb.KeyRange{Begin: lk, End: rk}, fdb.RangeOptions{})

	log.Debug().Str("table", string(table)).Interface("lKey", lKey).Interface("rKey", rKey).Msg("tx read range")

	return &fdbIterator{it: r.Iterator(), subspace: subspace.FromBytes(table)}, nil
}

func (t *ftx) SetVersionstampedValue(_ context.Context, key []byte, value []byte) error {
	t.tx.SetVersionstampedValue(fdb.Key(key), value)

	return nil
}

func (t *ftx) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error {
	t.tx.SetVersionstampedKey(fdb.Key(key), value)

	return nil
}

func (t *ftx) Get(_ context.Context, key []byte) ([]byte, error) {
	return t.tx.Get(fdb.Key(key)).Get()
}

func (t *ftx) Commit(ctx context.Context) error {
	if t.err != nil {
		return t.err
	}

	if t.err = t.tx.Commit().Get(); t.err == nil {
		return nil
	}

	log.Err(t.err).Msg("tx Commit")

	var ep fdb.Error
	if errors.As(t.err, &ep) {
		if ep.Code == 1020 {
			t.err = ErrConflictingTransaction
		}
	}

	t.tx.Cancel()

	return t.err
}

func (t *ftx) Rollback(ctx context.Context) error {
	t.tx.Cancel()

	log.Debug().Msg("tx Rollback")

	return nil
}

// IsRetriable returns true if transaction can be retried after error
func (t *ftx) IsRetriable() bool {
	if t.err == nil {
		return false
	}

	var ep fdb.Error
	if errors.As(t.err, &ep) {
		err := t.tx.OnError(ep).Get()
		if err == nil {
			return true
		}
	}

	return false
}

func tupleToKey(t *tuple.Tuple) Key {
	p := unsafe.Pointer(t)
	return *(*Key)(p)
}

func (i *fdbIterator) Next(kv *baseKeyValue) bool {
	if i.err != nil {
		return false
	}

	if !i.it.Advance() {
		return false
	}

	tkv, err := i.it.Get()
	if ulog.E(err) {
		i.err = err
		return false
	}

	t, err := i.subspace.Unpack(tkv.Key)
	if ulog.E(err) {
		i.err = err
		return false
	}

	if kv != nil {
		kv.Key = tupleToKey(&t)
		kv.FDBKey = tkv.Key
		kv.Value = tkv.Value
	}

	//log.Debug().Interface("key", tupleToKey(&t)).Str("table", i.subspace.FDBKey().String()).Msg("fdbIterator.Next")

	return true
}

func (i *fdbIterator) Err() error {
	return i.err
}

func (i *fdbIteratorTxCloser) Next(kv *baseKeyValue) bool {
	if i.tx == nil {
		return false
	}
	if !i.baseIterator.Next(kv) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := i.tx.Rollback(ctx)
		ulog.E(err)
		i.tx = nil
		return false
	}
	return true
}

func getFDBKey(table []byte, key Key) fdb.Key {
	s := subspace.FromBytes(table)
	var k fdb.Key
	if len(key) == 0 {
		k = s.FDBKey()
	} else {
		p := unsafe.Pointer(&key)
		k = s.Pack(*(*tuple.Tuple)(p))
	}

	return k
}

// getCtxTimeout returns timeout in ms if it's set in the context
// returns 0 if timeout is not set
// returns negative number if timeout has expired
func getCtxTimeout(ctx context.Context) int64 {
	tm, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return time.Until(tm).Milliseconds()
}

// setTxTimeout sets transaction timeout
// Zero input sets unlimited timeout
func setTxTimeout(tx *fdb.Transaction, ms int64) error {
	if ms < 0 {
		return context.DeadlineExceeded
	}

	return tx.Options().SetTimeout(ms)
}
