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
	"context"

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/metrics"
)

type TxStoreWithMetrics struct {
	kv TxStore
}

func NewKeyValueStoreWithMetrics(txStore TxStore) (TxStore, error) {
	return &TxStoreWithMetrics{
		kv: txStore,
	}, nil
}

func measureLow(ctx context.Context, name string, f func() error) {
	// Low level measurement wrapper that is called by the measure functions on the appropriate receiver
	measurement := metrics.NewMeasurement(metrics.KvTracingServiceName, name, metrics.FdbSpanType, metrics.GetFdbBaseTags(name))
	ctx = measurement.StartTracing(ctx, true)
	err := f()
	if err == nil {
		// Request was ok
		measurement.CountOkForScope(metrics.FdbOkCount, measurement.GetFdbOkTags())
		_ = measurement.FinishTracing(ctx)
		measurement.RecordDuration(metrics.FdbRespTime, measurement.GetFdbOkTags())
		return
	}
	// Request had an error
	measurement.CountErrorForScope(metrics.FdbOkCount, measurement.GetFdbErrorTags(err))
	_ = measurement.FinishWithError(ctx, err)
	measurement.RecordDuration(metrics.FdbErrorRespTime, measurement.GetFdbErrorTags(err))
}

func (m *TxStoreWithMetrics) measure(ctx context.Context, name string, f func() error) {
	measureLow(ctx, name, f)
}

func (m *TxStoreWithMetrics) CreateTable(ctx context.Context, name []byte) (err error) {
	m.measure(ctx, "CreateTable", func() error {
		err = m.kv.CreateTable(ctx, name)
		return err
	})
	return
}

func (m *TxStoreWithMetrics) DropTable(ctx context.Context, name []byte) (err error) {
	m.measure(ctx, "DropTable", func() error {
		err = m.kv.DropTable(ctx, name)
		return err
	})
	return
}

func (m *TxStoreWithMetrics) TableSize(ctx context.Context, name []byte) (size int64, err error) {
	m.measure(ctx, "TableSize", func() error {
		size, err = m.kv.TableSize(ctx, name)
		return err
	})
	return
}

func (m *TxStoreWithMetrics) BeginTx(ctx context.Context) (Tx, error) {
	// This needs to be a special case in order to have the tx metrics as well
	var btx Tx
	var err error
	m.measure(ctx, "BeginTx", func() error {
		btx, err = m.kv.BeginTx(ctx)
		return err
	})
	return &TxImplWithMetrics{
		btx,
	}, err
}

func (m *TxStoreWithMetrics) GetInternalDatabase() (k interface{}, err error) {
	k, err = m.kv.GetInternalDatabase()
	return
}

type TxImplWithMetrics struct {
	tx Tx
}

func (m *TxImplWithMetrics) measure(ctx context.Context, name string, f func() error) {
	measureLow(ctx, name, f)
}

func (m *TxImplWithMetrics) Delete(ctx context.Context, table []byte, key Key) (err error) {
	m.measure(ctx, "Delete", func() error {
		err = m.tx.Delete(ctx, table, key)
		return err
	})
	return
}

func (m *TxImplWithMetrics) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) (err error) {
	m.measure(ctx, "SetVersionstampedValue", func() error {
		err = m.tx.SetVersionstampedValue(ctx, key, value)
		return err
	})
	return
}

func (m *TxImplWithMetrics) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) (err error) {
	m.measure(ctx, "SetVersionstampedKey", func() error {
		err = m.tx.SetVersionstampedKey(ctx, key, value)
		return err
	})
	return
}

func (m *TxImplWithMetrics) AtomicAdd(ctx context.Context, table []byte, key Key, value int64) (err error) {
	m.measure(ctx, "AtomicAdd", func() error {
		err = m.tx.AtomicAdd(ctx, table, key, value)
		return err
	})
	return
}

func (m *TxImplWithMetrics) AtomicRead(ctx context.Context, table []byte, key Key) (value int64, err error) {
	m.measure(ctx, "AtomicRead", func() error {
		value, err = m.tx.AtomicRead(ctx, table, key)
		return err
	})
	return
}

func (m *TxImplWithMetrics) AtomicReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool) (iter AtomicIterator, err error) {
	m.measure(ctx, "AtomicReadRange", func() error {
		iter, err = m.tx.AtomicReadRange(ctx, table, lkey, rkey, isSnapshot)
		return err
	})
	return
}

func (m *TxImplWithMetrics) Get(ctx context.Context, key []byte, isSnapshot bool) (val Future, err error) {
	m.measure(ctx, "Get", func() error {
		val, err = m.tx.Get(ctx, key, isSnapshot)
		return err
	})
	return
}

func (m *TxImplWithMetrics) RangeSize(ctx context.Context, table []byte, lkey Key, rkey Key) (size int64, err error) {
	m.measure(ctx, "RangeSize", func() error {
		size, err = m.tx.RangeSize(ctx, table, lkey, rkey)
		return err
	})
	return
}

func (m *TxImplWithMetrics) Commit(ctx context.Context) (err error) {
	m.measure(ctx, "Commit", func() error {
		err = m.tx.Commit(ctx)
		return err
	})
	return
}

func (m *TxImplWithMetrics) Rollback(ctx context.Context) (err error) {
	m.measure(ctx, "Rollback", func() error {
		err = m.tx.Rollback(ctx)
		return err
	})
	return
}

func (m *TxImplWithMetrics) IsRetriable() bool {
	return m.tx.IsRetriable()
}

func (m *TxImplWithMetrics) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) (err error) {
	m.measure(ctx, "Insert", func() error {
		err = m.tx.Insert(ctx, table, key, data)
		return err
	})
	return
}

func (m *TxImplWithMetrics) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) (err error) {
	m.measure(ctx, "Replace", func() error {
		err = m.tx.Replace(ctx, table, key, data, isUpdate)
		return err
	})
	return
}

func (m *TxImplWithMetrics) Read(ctx context.Context, table []byte, key Key) (it Iterator, err error) {
	m.measure(ctx, "Read", func() error {
		it, err = m.tx.Read(ctx, table, key)
		return err
	})
	return
}

func (m *TxImplWithMetrics) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key, isSnapshot bool) (it Iterator, err error) {
	m.measure(ctx, "ReadRange", func() error {
		it, err = m.tx.ReadRange(ctx, table, lkey, rkey, isSnapshot)
		return err
	})
	return
}
