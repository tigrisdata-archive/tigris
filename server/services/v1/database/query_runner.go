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

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/query/update"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
)

// QueryRunner is responsible for executing the current query and return the response.
type QueryRunner interface {
	Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error)
}

// ReadOnlyQueryRunner is the QueryRunner which decides inside the ReadOnly method if the query needs to be run inside
// a transaction or can opt to just execute the query. This interface allows caller to control the state of the transaction
// or can choose to execute without starting any transaction.
type ReadOnlyQueryRunner interface {
	ReadOnly(ctx context.Context, tenant *metadata.Tenant) (Response, context.Context, error)
}

type InsertQueryRunner struct {
	*BaseQueryRunner

	req          *api.InsertRequest
	queryMetrics *metrics.WriteQueryMetrics
}

func (runner *InsertQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant,
		runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	if err = runner.mustBeDocumentsCollection(coll, "insert"); err != nil {
		return Response{}, ctx, err
	}

	ts, allKeys, err := runner.insertOrReplace(ctx, tx, tenant, coll, runner.req.GetDocuments(), true)
	if err != nil {
		if err == kv.ErrDuplicateKey {
			return Response{}, ctx, errors.AlreadyExists(err.(kv.StoreError).Msg())
		}

		return Response{}, ctx, err
	}

	runner.queryMetrics.SetWriteType("insert")
	metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	return Response{
		CreatedAt: ts,
		AllKeys:   allKeys,
		Status:    InsertedStatus,
	}, ctx, nil
}

type ReplaceQueryRunner struct {
	*BaseQueryRunner

	req          *api.ReplaceRequest
	queryMetrics *metrics.WriteQueryMetrics
}

func (runner *ReplaceQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant,
		runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	if err = runner.mustBeDocumentsCollection(coll, "replace"); err != nil {
		return Response{}, ctx, err
	}

	ts, allKeys, err := runner.insertOrReplace(ctx, tx, tenant, coll, runner.req.GetDocuments(), false)
	if err != nil {
		return Response{}, ctx, err
	}

	runner.queryMetrics.SetWriteType("replace")
	metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	return Response{
		CreatedAt: ts,
		AllKeys:   allKeys,
		Status:    ReplacedStatus,
	}, ctx, nil
}

type UpdateQueryRunner struct {
	*BaseQueryRunner

	req          *api.UpdateRequest
	queryMetrics *metrics.WriteQueryMetrics
}

func updateDefaultsAndSchema(db string, branch string, collection *schema.DefaultCollection, doc []byte, version int32, ts *internal.Timestamp) ([]byte, error) {
	var (
		err    error
		decDoc map[string]any
	)

	if len(collection.TaggedDefaultsForUpdate()) == 0 && collection.CompatibleSchemaSince(version) {
		return doc, nil
	}

	// TODO: revisit this path. We are deserializing here the merged payload (existing + incoming) and then
	// we are setting the updated value if any field is tagged with @updatedAt and then we are packing
	// it again.
	decDoc, err = util.JSONToMap(doc)
	if ulog.E(err) {
		return nil, err
	}

	if !collection.CompatibleSchemaSince(version) {
		collection.UpdateRowSchema(decDoc, version)
		metrics.SchemaUpdateRepaired(db, branch, collection.Name)
	}

	if len(collection.TaggedDefaultsForUpdate()) > 0 {
		mutator := newUpdatePayloadMutator(collection, ts.ToRFC3339())
		if err = mutator.setDefaultsInExistingPayload(decDoc); err != nil {
			return nil, err
		}
	}

	if doc, err = util.MapToJSON(decDoc); err != nil {
		return nil, err
	}

	return doc, nil
}

func (runner *UpdateQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant,
		runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	indexer := NewSecondaryIndexer(coll)

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	if filter.None(runner.req.Filter) {
		return Response{}, ctx, errors.InvalidArgument("updating all documents is not allowed")
	}

	if err = runner.mustBeDocumentsCollection(coll, "update"); err != nil {
		return Response{}, ctx, err
	}

	factory, err := update.BuildFieldOperators(runner.req.Fields)
	if err != nil {
		return Response{}, ctx, err
	}

	var (
		collation     *value.Collation
		limit         int32
		modifiedCount int32
		row           Row
		ts            = internal.NewTimestamp()
	)

	if fieldOperator, ok := factory.FieldOperators[string(update.Set)]; ok {
		// Set operation needs schema validation as well as mutation if we need to convert numeric fields from string to int64
		fieldOperator.Input, err = runner.mutateAndValidatePayload(ctx, coll, newUpdatePayloadMutator(coll, ts.ToRFC3339()), fieldOperator.Input)
		if err != nil {
			return Response{}, ctx, err
		}
	}

	if runner.req.Options != nil {
		collation = value.NewCollationFrom(runner.req.Options.Collation)
		limit = int32(runner.req.Options.Limit)
	} else {
		collation = value.NewCollation()
	}

	iterator, err := runner.getWriteIterator(ctx, tx, coll, runner.req.Filter, collation, runner.queryMetrics)
	if err != nil {
		return Response{}, ctx, err
	}

	writeSize := 0

	for ; (limit == 0 || modifiedCount < limit) && iterator.Next(&row); modifiedCount++ {
		key, err := keys.FromBinary(coll.EncodedName, row.Key)
		if err != nil {
			return Response{}, ctx, err
		}

		merged, err := updateDefaultsAndSchema(db.DbName(), db.BranchName(), coll, row.Data.RawData, row.Data.Ver, ts)
		if err != nil {
			return Response{}, ctx, err
		}

		// MergeAndGet merge the user input with existing doc and return the merged JSON document which we need to
		// persist back.
		merged, tentativeKeysToRemove, primaryKeyMutation, err := factory.MergeAndGet(merged, coll)
		if err != nil {
			return Response{}, ctx, err
		}
		if len(tentativeKeysToRemove) > 0 {
			// When an object is updated then we need to remove all the keys inside the object that are not part of the
			// update request. The reason is as we store data in flattened form we need to remove the stale keys.
			// The decision of what keys to be removed is pushed down to search indexer, the reason is performance.
			// The indexer is already deserializing the payload so it can eliminate the keys that are not needed
			// to be removed. Mainly filtering out keys that are part of incoming payload.
			ctx = context.WithValue(ctx, TentativeSearchKeysToRemove{}, tentativeKeysToRemove)
		}

		newData := internal.NewTableDataWithTS(row.Data.CreatedAt, ts, merged)
		newData.SetVersion(coll.GetVersion())

		writeSize += len(merged)

		isUpdate := true
		newKey := key
		if primaryKeyMutation {
			// we need to deleteReq old key and build new key from new data
			keyGen := newKeyGenerator(newData.RawData, tenant.TableKeyGenerator, coll.Indexes.PrimaryKey)
			if newKey, err = keyGen.generate(ctx, runner.txMgr, runner.encoder, coll.EncodedName); err != nil {
				return Response{}, nil, err
			}

			// deleteReq old key
			if err = tx.Delete(ctx, key); ulog.E(err) {
				return Response{}, ctx, err
			}
			isUpdate = false

			if config.DefaultConfig.SecondaryIndex.WriteEnabled {
				if err := indexer.Delete(ctx, tx, row.Data, key.IndexParts()); ulog.E(err) {
					return Response{}, nil, err
				}
				if err = indexer.Index(ctx, tx, newData, newKey.IndexParts()); ulog.E(err) {
					return Response{}, nil, err
				}
			}
		} else if config.DefaultConfig.SecondaryIndex.WriteEnabled {
			if err = indexer.Update(ctx, tx, newData, row.Data, key.IndexParts()); ulog.E(err) {
				return Response{}, ctx, err
			}
		}

		// as we have merged the data, it is safe to call replace
		if err = tx.Replace(ctx, newKey, newData, isUpdate); ulog.E(err) {
			return Response{}, ctx, err
		}
	}

	request.IncrementWrittenBytes(ctx, writeSize)
	request.IncrementReadBytes(ctx, iterator.ReadSize())

	ctx = metrics.UpdateSpanTags(ctx, runner.queryMetrics)
	return Response{
		Status:        UpdatedStatus,
		UpdatedAt:     ts,
		ModifiedCount: modifiedCount,
	}, ctx, err
}

type DeleteQueryRunner struct {
	*BaseQueryRunner

	req          *api.DeleteRequest
	queryMetrics *metrics.WriteQueryMetrics
}

func (runner *DeleteQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant,
		runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())
	indexer := NewSecondaryIndexer(coll)

	if err = runner.mustBeDocumentsCollection(coll, "deleteReq"); err != nil {
		return Response{}, ctx, err
	}

	ts := internal.NewTimestamp()

	var iterator Iterator
	if filter.None(runner.req.Filter) {
		iterator, err = NewDatabaseReader(ctx, tx).ScanTable(coll.EncodedName)
		runner.queryMetrics.SetWriteType("full_scan")
	} else {
		var collation *value.Collation
		if runner.req.Options != nil {
			collation = value.NewCollationFrom(runner.req.Options.Collation)
		} else {
			collation = value.NewCollation()
		}

		iterator, err = runner.getWriteIterator(ctx, tx, coll, runner.req.Filter, collation, runner.queryMetrics)
	}
	if err != nil {
		return Response{}, ctx, err
	}

	limit := int32(0)
	if runner.req.Options != nil {
		limit = int32(runner.req.Options.Limit)
	}

	writeSize := 0
	modifiedCount := int32(0)
	var row Row
	for iterator.Next(&row) {
		key, err := keys.FromBinary(coll.EncodedName, row.Key)
		if err != nil {
			return Response{}, ctx, err
		}

		if config.DefaultConfig.SecondaryIndex.WriteEnabled {
			err := indexer.Delete(ctx, tx, row.Data, key.IndexParts())
			if err != nil {
				return Response{}, ctx, err
			}
		}

		writeSize += len(row.Data.RawData)

		if err = tx.Delete(ctx, key); ulog.E(err) {
			return Response{}, ctx, err
		}

		modifiedCount++
		if limit > 0 && modifiedCount == limit {
			break
		}
	}

	request.IncrementWrittenBytes(ctx, writeSize)
	request.IncrementReadBytes(ctx, iterator.ReadSize())

	ctx = metrics.UpdateSpanTags(ctx, runner.queryMetrics)
	return Response{
		Status:        DeletedStatus,
		DeletedAt:     ts,
		ModifiedCount: modifiedCount,
	}, ctx, nil
}

// StreamingQueryRunner is a runner used for Queries that are reads and needs to return result in streaming fashion.
type StreamingQueryRunner struct {
	*BaseQueryRunner

	req          *api.ReadRequest
	streaming    Streaming
	queryMetrics *metrics.StreamingQueryMetrics
}

type readerOptions struct {
	from          keys.Key
	ikeys         []keys.Key
	plan          *filter.QueryPlan
	table         []byte
	noFilter      bool
	inMemoryStore bool
	// secondaryIndex bool
	sorting      *sort.Ordering
	filter       *filter.WrappedFilter
	fieldFactory *read.FieldFactory
}

func (runner *StreamingQueryRunner) buildReaderOptions(collection *schema.DefaultCollection) (readerOptions, error) {
	var err error
	options := readerOptions{}
	var collation *value.Collation
	if runner.req.Options != nil {
		collation = value.NewCollationFrom(runner.req.Options.Collation)
	}
	if options.sorting, err = runner.getSortOrdering(collection, runner.req.Sort); err != nil {
		return options, err
	}
	if options.filter, err = filter.NewFactory(collection.QueryableFields, collation).WrappedFilter(runner.req.Filter); err != nil {
		return options, err
	}

	options.table = collection.EncodedName
	if options.fieldFactory, err = read.BuildFields(runner.req.GetFields()); err != nil {
		return options, err
	}
	if runner.req.Options != nil && len(runner.req.Options.Offset) > 0 {
		if options.from, err = keys.FromBinary(options.table, runner.req.Options.Offset); err != nil {
			return options, err
		}
	}

	if config.DefaultConfig.SecondaryIndex.ReadEnabled {
		if queryPlan, err := runner.buildSecondaryIndexKeysUsingFilter(collection, runner.req.Filter, collation); err == nil {
			options.plan = queryPlan
			return options, nil
		}
	}

	if options.filter.None() || !options.filter.IsSearchIndexed() {
		// trigger full scan in case there is a field in the filter which is not indexed
		if options.sorting != nil {
			options.inMemoryStore = true
		} else {
			options.noFilter = true
		}
	} else if options.ikeys, err = runner.buildKeysUsingFilter(collection, runner.req.Filter, collation); err != nil {
		if !config.DefaultConfig.Search.IsReadEnabled() {
			if options.from == nil {
				// in this case, scan will happen from the beginning of the table.
				options.from = keys.NewKey(options.table)
			}
		} else {
			options.inMemoryStore = true
		}
	}

	return options, nil
}

func (runner *StreamingQueryRunner) instrumentRunner(ctx context.Context, options readerOptions) context.Context {
	// Set read type
	//nolint:gocritic
	if options.plan != nil {
		runner.queryMetrics.SetReadType("secondary")
	} else if options.ikeys == nil {
		runner.queryMetrics.SetReadType("non-pkey")
	} else {
		runner.queryMetrics.SetReadType("pkey")
	}

	if options.noFilter {
		runner.queryMetrics.SetReadType("full_scan")
	}

	// Sort is only supported for search
	runner.queryMetrics.SetSort(false)
	return metrics.UpdateSpanTags(ctx, runner.queryMetrics)
}

// ReadOnly is used by the read query runner to handle long-running reads. This method operates by starting a new
// transaction when needed which means a single user request may end up creating multiple read only transactions.
func (runner *StreamingQueryRunner) ReadOnly(ctx context.Context, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, nil, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return Response{}, ctx, err
	}

	options, err := runner.buildReaderOptions(collection)
	if err != nil {
		return Response{}, ctx, err
	}
	if options.inMemoryStore {
		if err = runner.iterateOnSearchStore(ctx, collection, options); err != nil {
			return Response{}, ctx, err
		}
		return Response{}, ctx, nil
	}

	for {
		// A for loop is needed to recreate the transaction after exhausting the duration of the previous transaction.
		// This is mainly needed for long-running transactions, otherwise reads should be small.
		tx, err := runner.txMgr.StartTx(ctx)
		if err != nil {
			return Response{}, ctx, err
		}

		var last []byte
		if options.plan != nil {
			last, err = runner.iterateOnSecondaryIndexStore(ctx, tx, collection, options)
		} else {
			last, err = runner.iterateOnKvStore(ctx, tx, collection, options)
		}

		_ = tx.Rollback(ctx)

		if err == kv.ErrTransactionMaxDurationReached {
			// We have received ErrTransactionMaxDurationReached i.e. 5 second transaction limit, so we need to retry the
			// transaction.
			options.from, _ = keys.FromBinary(options.table, last)
			continue
		}

		if err != nil {
			return Response{}, ctx, err
		}

		ctx = runner.instrumentRunner(ctx, options)

		return Response{}, ctx, nil
	}
}

// Run is responsible for running the read in the transaction started by the session manager. This doesn't do any retry
// if we see ErrTransactionMaxDurationReached which is expected because we do not expect caller to do long reads in an
// explicit transaction.
func (runner *StreamingQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant,
		runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	options, err := runner.buildReaderOptions(coll)
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.instrumentRunner(ctx, options)
	if options.inMemoryStore {
		if err = runner.iterateOnSearchStore(ctx, coll, options); err != nil {
			return Response{}, ctx, err
		}
		return Response{}, ctx, nil
	} else {
		if options.plan != nil {
			if _, err = runner.iterateOnSecondaryIndexStore(ctx, tx, coll, options); err != nil {
				return Response{}, ctx, err
			}
			return Response{}, ctx, nil
		}
		if _, err = runner.iterateOnKvStore(ctx, tx, coll, options); err != nil {
			return Response{}, ctx, err
		}
		return Response{}, ctx, nil
	}
}

func (runner *StreamingQueryRunner) iterateOnKvStore(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, options readerOptions) ([]byte, error) {
	var err error
	var iter Iterator
	reader := NewDatabaseReader(ctx, tx)
	if len(options.ikeys) != 0 {
		iter, err = reader.KeyIterator(options.ikeys)
	} else if options.from != nil {
		if iter, err = reader.ScanIterator(options.from, nil); err == nil {
			// pass it to filterable
			iter, err = reader.FilteredRead(iter, options.filter)
		}
	} else if iter, err = reader.ScanTable(options.table); err == nil {
		// pass it to filterable
		iter, err = reader.FilteredRead(iter, options.filter)
	}
	if err != nil {
		return nil, err
	}

	return runner.iterate(coll, iter, options.fieldFactory)
}

func (runner *StreamingQueryRunner) iterateOnSecondaryIndexStore(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, options readerOptions) ([]byte, error) {
	iter, err := NewSecondaryIndexReader(ctx, tx, coll, options.filter, options.plan)
	if err != nil {
		return nil, err
	}

	return runner.iterate(coll, NewFilterIterator(iter, options.filter), options.fieldFactory)
}

func (runner *StreamingQueryRunner) iterateOnSearchStore(ctx context.Context, coll *schema.DefaultCollection, options readerOptions) error {
	rowReader := NewSearchReader(ctx, runner.searchStore, coll, qsearch.NewBuilder().
		Filter(options.filter).
		SortOrder(options.sorting).
		PageSize(defaultPerPage).
		Build())

	if _, err := runner.iterate(coll, rowReader.Iterator(coll, options.filter), options.fieldFactory); err != nil {
		return err
	}

	return nil
}

func (runner *StreamingQueryRunner) iterate(coll *schema.DefaultCollection, iterator Iterator, fieldFactory *read.FieldFactory) ([]byte, error) {
	limit := int64(0)
	if runner.req.GetOptions() != nil {
		limit = runner.req.GetOptions().Limit
	}

	skip := int64(0)
	if runner.req.GetOptions() != nil {
		skip = runner.req.GetOptions().Skip
	}

	var row Row
	branch := "main"
	if runner.req.GetBranch() != "" {
		branch = runner.req.GetBranch()
	}

	limit += skip
	for i := int64(0); (limit == 0 || i < limit) && iterator.Next(&row); i++ {
		if skip > 0 {
			skip -= 1
			continue
		}

		rawData := row.Data.RawData
		var err error

		if !coll.CompatibleSchemaSince(row.Data.Ver) {
			rawData, err = coll.UpdateRowSchemaRaw(rawData, row.Data.Ver)
			if err != nil {
				return row.Key, err
			}

			metrics.SchemaReadOutdated(runner.req.GetProject(), branch, coll.Name)
		}

		newValue, err := fieldFactory.Apply(rawData)
		if ulog.E(err) {
			return row.Key, err
		}

		if err := runner.streaming.Send(&api.ReadResponse{
			Data: newValue,
			Metadata: &api.ResponseMetadata{
				CreatedAt: row.Data.CreateToProtoTS(),
				UpdatedAt: row.Data.UpdatedToProtoTS(),
			},
			ResumeToken: row.Key,
		}); ulog.E(err) {
			return row.Key, err
		}
	}

	return row.Key, createApiError(iterator.Interrupted())
}
