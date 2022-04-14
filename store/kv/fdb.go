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
	"fmt"
	"time"
	"unsafe"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/server/config"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
)

const (
	maxTxSizeBytes = 10000000
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
	d  *fdbkv
	tx *fdb.Transaction
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
	log.Err(err).Int("api_version", 630).Str("cluster_file", cfg.ClusterFile).Msg("initializing foundation db")
	fdb.MustAPIVersion(630)
	d.db, err = fdb.OpenDatabase(cfg.ClusterFile)
	log.Err(err).Msg("initialized foundation db")
	return
}

// Read returns all the keys which has prefix equal to "key" parameter
func (d *fdbkv) Read(ctx context.Context, table []byte, key Key) (baseIterator, error) {
	tx, err := d.Tx(ctx)
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
	tx, err := d.Tx(ctx)
	if err != nil {
		return nil, err
	}
	it, err := tx.ReadRange(ctx, table, lKey, rKey)
	if err != nil {
		return nil, err
	}
	return &fdbIteratorTxCloser{it, tx}, nil
}

func (d *fdbkv) txWithTimeout(ctx context.Context, fn func(fdb.Transaction) (interface{}, error)) (interface{}, error) {
	return d.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := setTxTimeout(&tr, getCtxTimeout(ctx)); err != nil {
			return nil, err
		}
		return fn(tr)
	})
}

func (d *fdbkv) Insert(ctx context.Context, table []byte, key Key, data []byte) error {
	_, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d, &tr}).Insert(ctx, table, key, data)
	})
	return err
}

func (d *fdbkv) Replace(ctx context.Context, table []byte, key Key, data []byte) error {
	_, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d, &tr}).Replace(ctx, table, key, data)
	})
	return err
}

func (d *fdbkv) Delete(ctx context.Context, table []byte, key Key) error {
	_, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d, &tr}).Delete(ctx, table, key)
	})
	return err
}

func (d *fdbkv) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error {
	_, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d, &tr}).DeleteRange(ctx, table, lKey, rKey)
	})
	return err
}

func (d *fdbkv) Update(ctx context.Context, table []byte, key Key, apply func([]byte) ([]byte, error)) error {
	_, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d, &tr}).Update(ctx, table, key, apply)
	})
	return err
}

func (d *fdbkv) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) error {
	_, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d, &tr}).UpdateRange(ctx, table, lKey, rKey, apply)
	})
	return err
}

func (d *fdbkv) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error {
	_, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d, &tr}).SetVersionstampedValue(ctx, key, value)
	})
	return err
}

func (d *fdbkv) Get(ctx context.Context, key []byte) ([]byte, error) {
	val, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return (&ftx{d, &tr}).Get(ctx, key)
	})
	return val.([]byte), err
}

func (d *fdbkv) CreateTable(_ context.Context, name []byte) error {
	log.Debug().Str("name", string(name)).Msg("table created")
	return nil
}

func (d *fdbkv) DropTable(ctx context.Context, name []byte) error {
	s := subspace.FromBytes(name)

	_, err := d.txWithTimeout(ctx, func(tr fdb.Transaction) (interface{}, error) {
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

func (b *fbatch) Update(ctx context.Context, table []byte, key Key, apply func([]byte) ([]byte, error)) error {
	if err := b.flushBatch(ctx, key, nil, nil); err != nil {
		return err
	}
	return b.tx.Update(ctx, table, key, apply)
}

func (b *fbatch) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) error {
	if err := b.flushBatch(ctx, lKey, rKey, nil); err != nil {
		return err
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

func (b *fbatch) Get(_ context.Context, _ []byte) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *fbatch) Commit(ctx context.Context) error {
	return b.tx.Commit(ctx)
}

func (b *fbatch) Rollback(ctx context.Context) error {
	return b.tx.Rollback(ctx)
}

func (d *fdbkv) Tx(ctx context.Context) (baseTx, error) {
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

func (t *ftx) Insert(_ context.Context, table []byte, key Key, data []byte) error {
	k := getFDBKey(table, key)

	// Read the value and if exists reject the write
	v := t.tx.Get(k)
	vv, err := v.Get()
	if err != nil {
		return err
	}
	if vv != nil {
		return api.Errorf(codes.AlreadyExists, "duplicate key value, violates unique primary key constraint")
	}

	t.tx.Set(k, data)

	log.Err(err).Str("table", string(table)).Interface("key", key).Msg("Insert")

	return err
}

func (t *ftx) Replace(_ context.Context, table []byte, key Key, data []byte) error {
	k := getFDBKey(table, key)

	t.tx.Set(k, data)

	log.Debug().Str("table", string(table)).Interface("key", key).Msg("tx Replace")

	return nil
}

func (t *ftx) Delete(_ context.Context, table []byte, key Key) error {
	kr, err := fdb.PrefixRange(getFDBKey(table, key))
	if ulog.E(err) {
		return err
	}

	t.tx.ClearRange(kr)

	log.Debug().Str("table", string(table)).Interface("key", key).Msg("tx delete")

	return nil
}

func (t *ftx) DeleteRange(_ context.Context, table []byte, lKey Key, rKey Key) error {
	lk := getFDBKey(table, lKey)
	rk := getFDBKey(table, rKey)

	t.tx.ClearRange(fdb.KeyRange{Begin: lk, End: rk})

	log.Debug().Str("table", string(table)).Interface("lKey", lKey).Interface("rKey", rKey).Msg("tx delete range")

	return nil
}

func (t *ftx) Update(_ context.Context, table []byte, key Key, apply func([]byte) ([]byte, error)) error {
	k, err := fdb.PrefixRange(getFDBKey(table, key))
	if ulog.E(err) {
		return err
	}

	r := t.tx.GetRange(k, fdb.RangeOptions{})
	it := r.Iterator()

	for it.Advance() {
		kv, err := it.Get()
		if ulog.E(err) {
			return err
		}
		v, err := apply(kv.Value)
		if ulog.E(err) {
			return err
		}

		t.tx.Set(kv.Key, v)
	}

	log.Debug().Str("table", string(table)).Interface("Key", key).Msg("tx update")

	return nil
}

func (t *ftx) UpdateRange(_ context.Context, table []byte, lKey Key, rKey Key, apply func([]byte) ([]byte, error)) error {
	lk := getFDBKey(table, lKey)
	rk := getFDBKey(table, rKey)

	r := t.tx.GetRange(fdb.KeyRange{Begin: lk, End: rk}, fdb.RangeOptions{})

	it := r.Iterator()

	for it.Advance() {
		kv, err := it.Get()
		if ulog.E(err) {
			return err
		}
		v, err := apply(kv.Value)
		if ulog.E(err) {
			return err
		}

		t.tx.Set(kv.Key, v)
	}

	log.Debug().Str("table", string(table)).Interface("lKey", lKey).Interface("rKey", rKey).Msg("tx update range")

	return nil
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

	log.Debug().Str("key", string(key)).Msg("setting metadata version key")
	return nil
}

func (t *ftx) Get(_ context.Context, key []byte) ([]byte, error) {
	return t.tx.Get(fdb.Key(key)).Get()
}

func (t *ftx) Commit(_ context.Context) error {
	for {
		err := t.tx.Commit().Get()

		if err == nil {
			break
		}

		log.Err(err).Msg("tx Commit")

		var ep fdb.Error
		if xerrors.As(err, &ep) {
			err = t.tx.OnError(ep).Get()
		}

		if err != nil {
			return err
		}
	}

	log.Debug().Msg("tx Commit")

	return nil
}

func (t *ftx) Rollback(_ context.Context) error {
	t.tx.Cancel()

	log.Debug().Msg("tx Rollback")

	return nil
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

	log.Debug().Str("key", tkv.Key.String()).Msg("fdbIterator.Next")

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
	log.Debug().Str("key", k.String()).Msg("getFDBKey")
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
// Zero input sets unlimited timeout timeout
func setTxTimeout(tx *fdb.Transaction, ms int64) error {
	if ms < 0 {
		return context.DeadlineExceeded
	}

	return tx.Options().SetTimeout(ms)
}
