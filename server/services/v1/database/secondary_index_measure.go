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

package database

import (
	"context"

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
)

type secondaryIndexerWithMetrics struct {
	q *SecondaryIndexerImpl
}

func NewSecondaryIndexer(coll *schema.DefaultCollection) SecondaryIndexer {
	if config.DefaultConfig.Metrics.SecondaryIndex.Enabled {
		return NewSecondaryIndexerWithMetrics(coll)
	}

	return newSecondaryIndexerImpl(coll)
}

func NewSecondaryIndexerWithMetrics(coll *schema.DefaultCollection) SecondaryIndexer {
	q := newSecondaryIndexerImpl(coll)
	return &secondaryIndexerWithMetrics{
		q,
	}
}

func (m *secondaryIndexerWithMetrics) measure(ctx context.Context, name string, f func(ctx context.Context) error) {
	// Low level measurement wrapper that is called by the measure functions on the appropriate receiver
	measurement := metrics.NewMeasurement(metrics.SecondaryIndexServiceName, name, metrics.SecondaryIndexSpanType, metrics.GetSecondaryIndexTags(name))
	ctx = measurement.StartTracing(ctx, true)
	err := f(ctx)
	if err != nil {
		// Request had error
		measurement.CountErrorForScope(metrics.SecondaryIndexErrorCount, measurement.GetSecondaryIndexErrorTags(err))
		_ = measurement.FinishWithError(ctx, err)
		measurement.RecordDuration(metrics.SecondaryIndexErrorRespTime, measurement.GetSecondaryIndexErrorTags(err))
	} else {
		// Request was ok
		measurement.CountOkForScope(metrics.SecondaryIndexOkCount, measurement.GetSecondaryIndexOkTags())
		_ = measurement.FinishTracing(ctx)
		measurement.RecordDuration(metrics.SecondaryIndexRespTime, measurement.GetSecondaryIndexOkTags())
	}
}

func (m *secondaryIndexerWithMetrics) BuildCollection(ctx context.Context, txMgr *transaction.Manager) (err error) {
	m.measure(ctx, "BuildCollection", func(ctx context.Context) error {
		err = m.q.BuildCollection(ctx, txMgr)
		return err
	})
	return
}

func (m *secondaryIndexerWithMetrics) ReadDocAndDelete(ctx context.Context, tx transaction.Tx, key keys.Key) (size int32, err error) {
	m.measure(ctx, "ReadDocAndDelete", func(ctx context.Context) error {
		size, err = m.q.ReadDocAndDelete(ctx, tx, key)
		return err
	})
	return
}

func (m *secondaryIndexerWithMetrics) Delete(ctx context.Context, tx transaction.Tx, td *internal.TableData, primaryKey []interface{}) (err error) {
	m.measure(ctx, "Delete", func(ctx context.Context) error {
		err = m.q.Delete(ctx, tx, td, primaryKey)
		return err
	})
	return
}

func (m *secondaryIndexerWithMetrics) Index(ctx context.Context, tx transaction.Tx, td *internal.TableData, primaryKey []interface{}) (err error) {
	m.measure(ctx, "Index", func(ctx context.Context) error {
		err = m.q.Index(ctx, tx, td, primaryKey)
		return err
	})
	return
}

func (m *secondaryIndexerWithMetrics) Update(ctx context.Context, tx transaction.Tx, newTd *internal.TableData, oldTd *internal.TableData, primaryKey []interface{}) (err error) {
	m.measure(ctx, "Update", func(ctx context.Context) error {
		err = m.q.Update(ctx, tx, newTd, oldTd, primaryKey)
		return err
	})
	return
}

type secondaryIndexReaderWithMetrics struct {
	reader *SecondaryIndexReaderImpl
}

func NewSecondaryIndexReader(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, filter *filter.WrappedFilter, queryPlan *filter.QueryPlan) (Iterator, error) {
	if config.DefaultConfig.Metrics.SecondaryIndex.Enabled {
		return newSecondaryIndexReaderWithMetrics(ctx, tx, coll, filter, queryPlan)
	}

	return newSecondaryIndexReaderImpl(ctx, tx, coll, filter, queryPlan)
}

func newSecondaryIndexReaderWithMetrics(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, filter *filter.WrappedFilter, queryPlan *filter.QueryPlan) (Iterator, error) {
	reader, err := newSecondaryIndexReaderImpl(ctx, tx, coll, filter, queryPlan)
	if err != nil {
		return nil, err
	}

	return &secondaryIndexReaderWithMetrics{
		reader: reader,
	}, nil
}

func (m *secondaryIndexReaderWithMetrics) measure(ctx context.Context, name string, f func(ctx context.Context) error) {
	// Low level measurement wrapper that is called by the measure functions on the appropriate receiver
	measurement := metrics.NewMeasurement(metrics.SecondaryIndexServiceName, name, metrics.SecondaryIndexSpanType, metrics.GetSecondaryIndexTags(name))
	ctx = measurement.StartTracing(ctx, true)
	err := f(ctx)
	if err != nil {
		// Request had error
		measurement.CountErrorForScope(metrics.SecondaryIndexErrorCount, measurement.GetSecondaryIndexErrorTags(err))
		_ = measurement.FinishWithError(ctx, err)
		measurement.RecordDuration(metrics.SecondaryIndexErrorRespTime, measurement.GetSecondaryIndexErrorTags(err))
	} else {
		// Request was ok
		measurement.CountOkForScope(metrics.SecondaryIndexOkCount, measurement.GetSecondaryIndexOkTags())
		_ = measurement.FinishTracing(ctx)
		measurement.RecordDuration(metrics.SecondaryIndexRespTime, measurement.GetSecondaryIndexOkTags())
	}
}

func (m *secondaryIndexReaderWithMetrics) Next(row *Row) (hasNext bool) {
	m.measure(m.reader.ctx, "Next", func(ctx context.Context) error {
		hasNext = m.reader.Next(row)
		return nil
	})
	return
}

func (m *secondaryIndexReaderWithMetrics) Interrupted() error {
	return m.reader.Interrupted()
}
