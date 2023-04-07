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

package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
	fdbAPIVersion = 710
)

// fdbkv is an implementation of kv on top of FoundationDB.
type fdbkv struct {
	db fdb.Database
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
	ctx context.Context
	baseIterator
	tx baseTx
}

// newFoundationDB initializes instance of FoundationDB KV interface implementation.
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

// Read returns all the keys which has prefix equal to "key" parameter.
func (d *fdbkv) Read(ctx context.Context, table []byte, key Key) (baseIterator, error) {
	tx, err := d.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	it, err := tx.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}
	return &fdbIteratorTxCloser{ctx, it, tx}, nil
}

func (d *fdbkv) ReadRange(ctx context.Context, table []byte, lKey Key, rKey Key, isSnapshot bool) (baseIterator, error) {
	tx, err := d.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	it, err := tx.ReadRange(ctx, table, lKey, rKey, isSnapshot)
	if err != nil {
		return nil, err
	}
	return &fdbIteratorTxCloser{ctx, it, tx}, nil
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

func (d *fdbkv) Replace(ctx context.Context, table []byte, key Key, data []byte, isUpdate bool) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).Replace(ctx, table, key, data, isUpdate)
	})
	return err
}

func (d *fdbkv) Delete(ctx context.Context, table []byte, key Key) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).Delete(ctx, table, key)
	})
	return err
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

func (d *fdbkv) AtomicAdd(ctx context.Context, table []byte, key Key, value int64) error {
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return nil, (&ftx{d: d, tx: &tr}).AtomicAdd(ctx, table, key, value)
	})
	return err
}

func (d *fdbkv) AtomicRead(ctx context.Context, table []byte, key Key) (int64, error) {
	val, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return (&ftx{d: d, tx: &tr}).AtomicRead(ctx, table, key)
	})
	return val.(int64), err
}

func (d *fdbkv) AtomicReadRange(ctx context.Context, table []byte, lKey Key, rKey Key, isSnapshot bool) (AtomicIterator, error) {
	tx, err := d.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	it, err := tx.ReadRange(ctx, table, lKey, rKey, isSnapshot)
	if err != nil {
		return nil, err
	}
	return &AtomicIteratorImpl{ctx, it, nil}, nil
}

func (d *fdbkv) Get(ctx context.Context, key []byte, isSnapshot bool) Future {
	val, _ := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		return (&ftx{d: d, tx: &tr}).Get(ctx, key, isSnapshot), nil
	})

	return val.(Future)
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

// TableSize calculates approximate table size in bytes
// It also works with the prefix of the table name,
// allowing to calculate sizes of multiple table with the same prefix.
func (d *fdbkv) TableSize(ctx context.Context, name []byte) (int64, error) {
	s := subspace.FromBytes(name)

	var sz int64
	_, err := d.txWithRetry(ctx, func(tr fdb.Transaction) (interface{}, error) {
		var err error
		sz, err = tr.GetEstimatedRangeSizeBytes(s).Get()
		return nil, err
	})
	if err != nil {
		log.Err(err).Str("name", string(name)).Int64("size", sz).Msg("table size")
	}

	return sz, err
}

func (d *fdbkv) BeginTx(ctx context.Context) (baseTx, error) {
	tx, err := d.db.CreateTransaction()
	if ulog.E(err) {
		return nil, err
	}

	if err = setTxTimeout(&tx, getCtxTimeout(ctx)); err != nil {
		return nil, err
	}

	log.Trace().Msg("create transaction")

	return &ftx{d: d, tx: &tx}, nil
}

func (t *ftx) Insert(_ context.Context, table []byte, key Key, data []byte) error {
	k := getFDBKey(table, key)

	// Read the value and if exists reject the request.
	v := t.tx.Get(k)
	vv, err := v.Get()
	if err != nil {
		return convertFDBToStoreErr(err)
	}
	if vv != nil {
		return ErrDuplicateKey
	}

	t.tx.Set(k, data)

	log.Debug().Str("table", string(table)).Interface("key", key).Msg("Insert")

	return nil
}

func (t *ftx) Replace(_ context.Context, table []byte, key Key, data []byte, _ bool) error {
	k := getFDBKey(table, key)

	t.tx.Set(k, data)

	log.Debug().Str("table", string(table)).Interface("key", key).Msg("tx Replace")

	return nil
}

func (t *ftx) Delete(_ context.Context, table []byte, key Key) error {
	kr, err := fdb.PrefixRange(getFDBKey(table, key))
	if ulog.E(err) {
		return convertFDBToStoreErr(err)
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

func (t *ftx) Read(_ context.Context, table []byte, key Key) (baseIterator, error) {
	k, err := fdb.PrefixRange(getFDBKey(table, key))
	if ulog.E(err) {
		return nil, err
	}

	// It is possible that caller may be chunking the payload. Therefore, the "iterator" returned by this API is only
	// applicable for ascending order. Once we add support to do reverse reads then we should return a different iterator
	// or some other signal to the caller.
	r := t.tx.GetRange(k, fdb.RangeOptions{})

	return &fdbIterator{it: r.Iterator(), subspace: subspace.FromBytes(table)}, nil
}

func (t *ftx) ReadRange(_ context.Context, table []byte, lKey Key, rKey Key, isSnapshot bool) (baseIterator, error) {
	lk := getFDBKey(table, lKey)
	var rk fdb.Key
	if rKey == nil {
		// add a table boundary
		rk1 := make([]byte, len(table)+1)
		copy(rk1, table)
		rk1[len(rk1)-1] = byte(0xFF)
		rk = rk1
	} else {
		rk = getFDBKey(table, rKey)
	}

	kr := fdb.KeyRange{Begin: lk, End: rk}
	ro := fdb.RangeOptions{}

	var r fdb.RangeResult
	if isSnapshot {
		r = t.tx.Snapshot().GetRange(kr, ro)
	} else {
		r = t.tx.GetRange(kr, ro)
	}

	log.Trace().Str("table", string(table)).Interface("lKey", lKey).Interface("rKey", rKey).Msg("tx read range")

	return &fdbIterator{it: r.Iterator(), subspace: subspace.FromBytes(table)}, nil
}

func (t *ftx) SetVersionstampedValue(_ context.Context, key []byte, value []byte) error {
	t.tx.SetVersionstampedValue(fdb.Key(key), value)

	return nil
}

func (t *ftx) SetVersionstampedKey(_ context.Context, key []byte, value []byte) error {
	t.tx.SetVersionstampedKey(fdb.Key(key), value)

	return nil
}

func (t *ftx) AtomicAdd(_ context.Context, table []byte, key Key, value int64) error {
	fdbKey := getFDBKey(table, key)

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, value)
	if err != nil {
		return err
	}
	encVal := buf.Bytes()

	t.tx.Add(fdbKey, encVal)

	return nil
}

func (t *ftx) AtomicRead(_ context.Context, table []byte, key Key) (int64, error) {
	fdbKey := getFDBKey(table, key)
	raw, err := t.tx.Get(fdbKey).Get()
	if err != nil {
		return 0, err
	}

	return fdbByteToInt64(raw)
}

func (t *ftx) AtomicReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool) (AtomicIterator, error) {
	iter, err := t.ReadRange(ctx, table, lkey, rkey, isSnapshot)
	if err != nil {
		return nil, err
	}

	return &AtomicIteratorImpl{ctx, iter, nil}, nil
}

type AtomicIteratorImpl struct {
	ctx context.Context
	baseIterator
	err error
}

func (i *AtomicIteratorImpl) Next(value *FdbBaseKeyValue[int64]) bool {
	var v baseKeyValue
	hasNext := i.baseIterator.Next(&v)
	if hasNext {
		value.Key = v.Key
		value.FDBKey = v.FDBKey
		num, err := fdbByteToInt64(v.Value)
		if err != nil {
			i.err = err
			return false
		}
		value.Data = num
	}
	return hasNext
}

func (i *AtomicIteratorImpl) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.baseIterator.Err()
}

func (t *ftx) Get(_ context.Context, key []byte, isSnapshot bool) Future {
	if isSnapshot {
		return t.tx.Snapshot().Get(fdb.Key(key))
	}
	return t.tx.Get(fdb.Key(key))
}

// RangeSize calculates approximate range table size in bytes - this is an estimate
// and a range smaller than 3mb will not be that accurate.
func (t *ftx) RangeSize(_ context.Context, table []byte, lKey Key, rKey Key) (int64, error) {
	lk := getFDBKey(table, lKey)
	var rk fdb.Key
	if rKey == nil {
		// add a table boundary
		rk1 := make([]byte, len(table)+1)
		copy(rk1, table)
		rk1[len(rk1)-1] = byte(0xFF)
		rk = rk1
	} else {
		rk = getFDBKey(table, rKey)
	}

	kr := fdb.KeyRange{Begin: lk, End: rk}
	sz, err := t.tx.GetEstimatedRangeSizeBytes(kr).Get()
	log.Trace().Str("table", string(table)).Interface("lKey", lKey).Interface("rKey", rKey).Int64("size", sz).Msg("tx range size")
	if err != nil {
		log.Err(err).Str("name", string(table)).Int64("size", sz).Msg("tx range size")
	}

	return sz, err
}

func (t *ftx) Commit(_ context.Context) error {
	if t.err != nil {
		return t.err
	}

	if t.err = t.tx.Commit().Get(); t.err == nil {
		return nil
	}

	log.Err(t.err).Msg("tx Commit")

	t.err = convertFDBToStoreErr(t.err)

	t.tx.Cancel()

	return t.err
}

func (t *ftx) Rollback(_ context.Context) error {
	t.tx.Cancel()

	log.Debug().Msg("tx Rollback")

	return nil
}

// IsRetriable returns true if transaction can be retried after error.
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
		i.err = convertFDBToStoreErr(err)
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
// returns negative number if timeout has expired.
func getCtxTimeout(ctx context.Context) int64 {
	tm, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return time.Until(tm).Milliseconds()
}

// setTxTimeout sets transaction timeout
// Zero input sets unlimited timeout.
func setTxTimeout(tx *fdb.Transaction, ms int64) error {
	if ms < 0 {
		return context.DeadlineExceeded
	}

	return tx.Options().SetTimeout(ms)
}

func fdbByteToInt64(value []byte) (int64, error) {
	var numVal int64
	err := binary.Read(bytes.NewReader(value), binary.LittleEndian, &numVal)
	return numVal, err
}
