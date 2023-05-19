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
	"bytes"
	"context"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// defaultReadLimit is only applicable for non-streaming reads i.e. read when the content type is set
	// as application/json.
	defaultReadLimit = 256
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

	reqStatus, reqStatusFound := metrics.RequestStatusFromContext(ctx)
	if reqStatus != nil && reqStatusFound {
		reqStatus.AddResultDocs(int64(len(runner.req.Documents)))
	}

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

	reqStatus, reqStatusFound := metrics.RequestStatusFromContext(ctx)
	if reqStatus != nil && reqStatusFound {
		reqStatus.AddResultDocs(int64(len(runner.req.Documents)))
	}

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

func updateDefaultsAndSchema(db string, branch string, collection *schema.DefaultCollection, doc []byte, version uint32, ts *internal.Timestamp) ([]byte, error) {
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

func mutateAndValidatePushPayload(ctx context.Context, coll *schema.DefaultCollection, doc []byte) ([]byte, error) {
	ts := internal.NewTimestamp()
	deserializedDoc, err := util.JSONToMap(doc)
	if ulog.E(err) {
		return doc, err
	}

	// converting a single element to array for mutation.
	for key, value := range deserializedDoc {
		var newValue []any
		newValue = append(newValue, value)
		deserializedDoc[key] = newValue
	}

	mutator := newUpdatePayloadMutator(coll, ts.ToRFC3339())

	// this will mutate map, so we need to serialize this map again
	if err = mutator.stringToInt64(deserializedDoc); err != nil {
		return doc, err
	}

	if request.NeedSchemaValidation(ctx) {
		if err = coll.Validate(deserializedDoc); err != nil {
			// schema validation failed
			return doc, err
		}
	}

	if mutator.isMutated() {
		for key, v := range deserializedDoc {
			deserializedDoc[key] = v.([]any)[0]
		}
		return util.MapToJSON(deserializedDoc)
	}

	return doc, nil
}

func (runner *UpdateQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, coll, err := runner.getDBAndCollection(ctx, tx, tenant,
		runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	indexer := NewSecondaryIndexer(coll, false)

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

	if fieldOperator, ok := factory.FieldOperators[string(update.Push)]; ok {
		// mutate if it needs to convert numeric fields from string to int64
		fieldOperator.Input, err = mutateAndValidatePushPayload(ctx, coll, fieldOperator.Input)
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

	for ; (limit == 0 || modifiedCount < limit) && iterator.Next(&row); modifiedCount++ {
		key, err := keys.FromBinary(coll.EncodedName, row.Key)
		if err != nil {
			return Response{}, ctx, err
		}

		merged, err := updateDefaultsAndSchema(db.DbName(), db.BranchName(), coll, row.Data.RawData, uint32(row.Data.Ver), ts)
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
		newData.SetVersion(int32(coll.GetVersion()))
		// as we have merged the data, it is safe to call replace

		szCtx := kv.CtxWithSize(ctx, row.Data.Size())

		isUpdate := true
		newKey := key
		if primaryKeyMutation {
			// we need to deleteReq old key and build new key from new data
			keyGen := newKeyGenerator(newData.RawData, tenant.TableKeyGenerator, coll.GetPrimaryKey())
			if newKey, err = keyGen.generate(ctx, runner.txMgr, runner.encoder, coll.EncodedName); err != nil {
				return Response{}, nil, err
			}

			// deleteReq old key
			if err = tx.Delete(szCtx, key); ulog.E(err) {
				return Response{}, ctx, err
			}
			isUpdate = false

			// clear size from the context, so as we subtracted the size
			// in the delete above.
			// if new key exist its value size will be subtracted in the replace below
			szCtx = ctx

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

		if err = tx.Replace(szCtx, newKey, newData, isUpdate); ulog.E(err) {
			return Response{}, ctx, err
		}
	}

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
	indexer := NewSecondaryIndexer(coll, false)

	if err = runner.mustBeDocumentsCollection(coll, "deleteReq"); err != nil {
		return Response{}, ctx, err
	}

	ts := internal.NewTimestamp()

	reqStatus, reqStatusFound := metrics.RequestStatusFromContext(ctx)

	var iterator Iterator
	if filter.None(runner.req.Filter) {
		iterator, err = NewDatabaseReader(ctx, tx).ScanTable(coll.EncodedName, false)
		// For a delete without filter, do not count the read operations
		if reqStatusFound {
			reqStatus.AddDDLDropUnit()
			reqStatus.SetReadBytes(int64(0))
			reqStatus.SetWriteBytes(int64(0))
			ctx = reqStatus.SaveRequestStatusToContext(ctx)
		}
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

	modifiedCount := int32(0)
	var row Row
	for iterator.Next(&row) {
		key, err := keys.FromBinary(coll.EncodedName, row.Key)
		if err != nil {
			return Response{}, ctx, err
		}
		if reqStatusFound {
			reqStatus.AddWriteBytes(int64(row.Data.Size()))
		}

		if config.DefaultConfig.SecondaryIndex.WriteEnabled {
			err := indexer.Delete(ctx, tx, row.Data, key.IndexParts())
			if err != nil {
				return Response{}, ctx, err
			}
		}

		ctx = kv.CtxWithSize(ctx, row.Data.Size())

		if err = tx.Delete(ctx, key); ulog.E(err) {
			return Response{}, ctx, err
		}

		modifiedCount++
		if limit > 0 && modifiedCount == limit {
			break
		}
	}

	ctx = metrics.UpdateSpanTags(ctx, runner.queryMetrics)
	return Response{
		Status:        DeletedStatus,
		DeletedAt:     ts,
		ModifiedCount: modifiedCount,
	}, ctx, nil
}

type CountQueryRunner struct {
	*BaseQueryRunner

	req          *api.CountRequest
	queryMetrics *metrics.StreamingQueryMetrics
}

func (runner *CountQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	_, coll, err := runner.getDBAndCollection(ctx, tx, tenant, runner.req.GetProject(), runner.req.GetCollection(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	if err = runner.mustBeDocumentsCollection(coll, "countReq"); err != nil {
		return Response{}, ctx, err
	}

	reader := NewDatabaseReader(ctx, tx)
	var iterator Iterator
	if iterator, err = reader.ScanTable(coll.EncodedName, false); err != nil {
		return Response{}, ctx, err
	}
	if !filter.None(runner.req.Filter) {
		filterFactory := filter.NewFactory(coll.QueryableFields, nil)
		var filters []filter.Filter
		if filters, err = filterFactory.Factorize(runner.req.Filter); err != nil {
			return Response{}, ctx, err
		}

		if iterator, err = reader.FilteredRead(iterator, filter.NewWrappedFilter(filters)); err != nil {
			return Response{}, ctx, err
		}
	}

	var count int64
	var row Row
	for iterator.Next(&row) {
		count++
	}

	runner.queryMetrics.SetReadType("count")
	ctx = metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	return Response{
		Response: &api.CountResponse{
			Count: count,
		},
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
	plan          *filter.QueryPlan
	tablePlan     *filter.TableScanPlan
	inMemoryStore bool
	// secondaryIndex bool
	sorting        *sort.Ordering
	noSearchFilter *filter.WrappedFilter
	filter         *filter.WrappedFilter
	fieldFactory   *read.FieldFactory
}

func (runner *BaseQueryRunner) buildReaderOptions(req *api.ReadRequest, collection *schema.DefaultCollection) (readerOptions, error) {
	var err error
	options := readerOptions{}
	var collation *value.Collation
	if req.Options != nil {
		collation = value.NewCollationFrom(req.Options.Collation)
	}

	if options.filter, err = filter.NewFactory(collection.QueryableFields, collation).WrappedFilter(req.Filter); err != nil {
		return options, err
	}

	if options.fieldFactory, err = read.BuildFields(req.GetFields()); err != nil {
		return options, err
	}

	var from keys.Key
	if req.Options != nil && len(req.Options.Offset) > 0 {
		if from, err = keys.FromBinary(collection.EncodedName, req.Options.Offset); err != nil {
			return options, err
		}
	}

	if from == nil && config.DefaultConfig.SecondaryIndex.ReadEnabled {
		if secondarySorting, err := runner.getSortOrdering(collection, req.Sort); err == nil {
			if options.plan, err = runner.buildSecondaryIndexKeysUsingFilter(collection, req.Filter, collation, secondarySorting); err == nil {
				return options, nil
			}
		}
	}

	if searchSorting, err := runner.getSearchOrdering(collection, req.Sort); err == nil && searchSorting != nil {
		// only in case when sorting is explicitly tagged on the field we query search store. Also, we are not
		// passing filters, we are only using for sort and then applying filtering on server.
		options.noSearchFilter = filter.WrappedEmptyFilter
		options.sorting = searchSorting
		options.inMemoryStore = true
		return options, nil
	}

	planner, err := NewPrimaryIndexQueryPlanner(collection, runner.encoder, req.Filter, collation)
	if err != nil {
		return options, err
	}

	if options.sorting, err = runner.getSortOrdering(collection, req.Sort); err != nil {
		return options, err
	}

	var sortPlan *filter.QueryPlan
	if sortPlan, err = planner.SortPlan(options.sorting); err != nil {
		// at this point error means we can't handle sorting
		return options, err
	}
	if sortPlan != nil && planner.isPrefixSort(sortPlan) {
		// prefix sort OR no filter
		options.tablePlan, err = planner.GenerateTablePlan(sortPlan, from)
		return options, err
	}

	if planner.noFilter {
		if sortPlan != nil && !planner.isPrefixSort(sortPlan) {
			return options, errors.InvalidArgument("can't perform sort with empty filter and non-prefix primary key index")
		}

		// prefix sort OR no filter
		options.tablePlan, err = planner.GenerateTablePlan(sortPlan, from)
		return options, err
	}

	if options.plan, err = planner.GeneratePlan(sortPlan, from); err == nil {
		// plan can be handled by database index
		return options, nil
	} else if err == filter.ErrKeysEmpty {
		log.Err(err).
			Str("collection", collection.Name).
			Str("filter", string(req.Filter)).
			Msg("not able to build keys")
	}

	if planner.IsPrefixQueryWithSuffixSort(sortPlan) {
		// case when it is a prefix query with suffix sort
		options.tablePlan, err = planner.GenerateTablePlan(sortPlan, from)
		return options, err
	}
	if sortPlan != nil {
		return options, errors.InvalidArgument("can't perform sort on this field")
	}

	options.tablePlan, err = planner.GenerateTablePlan(sortPlan, from)
	return options, err
}

func (runner *StreamingQueryRunner) instrumentRunner(ctx context.Context, options readerOptions) context.Context {
	// Set read type
	//nolint:gocritic
	if options.tablePlan != nil {
		runner.queryMetrics.SetReadType("full_scan")
	} else if options.plan != nil && filter.IndexTypeSecondary(options.plan.IndexType) {
		runner.queryMetrics.SetReadType("secondary")
	} else if options.plan != nil && filter.IndexTypePrimary(options.plan.IndexType) {
		runner.queryMetrics.SetReadType("pkey")
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

	options, err := runner.buildReaderOptions(runner.req, collection)
	if err != nil {
		return Response{}, ctx, err
	}

	if options.inMemoryStore {
		if err = runner.iterateOnSearchStore(ctx, collection, options); err != nil {
			return Response{}, ctx, CreateApiError(err)
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
		if options.plan != nil && filter.IndexTypeSecondary(options.plan.IndexType) {
			last, err = runner.iterateOnSecondaryIndexStore(ctx, tx, collection, options)
		} else {
			last, err = runner.iterateOnKvStore(ctx, tx, collection, options)
		}

		_ = tx.Rollback(ctx)

		if err == kv.ErrTransactionMaxDurationReached {
			// We have received ErrTransactionMaxDurationReached i.e. 5 second transaction limit, so we need to retry the
			// transaction.
			// ToDo:
			//   - for secondary indexes the "from" need to be injected inside the plan
			//   - for primary index the full scan will handle it for now but we need to optimize it
			from, _ := keys.FromBinary(collection.EncodedName, last)
			if options.tablePlan == nil {
				// this means the Primary index plan or secondary index plan didn't finish under 5seconds
				options.tablePlan = &filter.TableScanPlan{
					Table:   collection.EncodedName,
					From:    from,
					Reverse: options.plan.Reverse(),
				}
			} else {
				options.tablePlan.From = from
			}
			continue
		}

		if err != nil {
			return Response{}, ctx, CreateApiError(err)
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

	options, err := runner.buildReaderOptions(runner.req, coll)
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.instrumentRunner(ctx, options)
	if options.inMemoryStore {
		if err = runner.iterateOnSearchStore(ctx, coll, options); err != nil {
			return Response{}, ctx, CreateApiError(err)
		}
		return Response{}, ctx, nil
	}

	if options.plan != nil && filter.IndexTypeSecondary(options.plan.IndexType) {
		if _, err = runner.iterateOnSecondaryIndexStore(ctx, tx, coll, options); err != nil {
			return Response{}, ctx, CreateApiError(err)
		}
		return Response{}, ctx, nil
	}
	if _, err = runner.iterateOnKvStore(ctx, tx, coll, options); err != nil {
		return Response{}, ctx, CreateApiError(err)
	}

	return Response{}, ctx, nil
}

func (runner *StreamingQueryRunner) iterateOnKvStore(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, options readerOptions) ([]byte, error) {
	var err error
	var iter Iterator
	reader := NewDatabaseReader(ctx, tx)
	//nolint:gocritic
	if options.tablePlan != nil {
		switch {
		case options.tablePlan.From != nil:
			if iter, err = reader.ScanIterator(options.tablePlan.From, nil, options.tablePlan.Reverse); err == nil {
				// pass it to filterable
				iter, err = reader.FilteredRead(iter, options.filter)
			}
		default:
			if iter, err = reader.ScanTable(options.tablePlan.Table, options.tablePlan.Reverse); err == nil {
				// pass it to filterable
				iter, err = reader.FilteredRead(iter, options.filter)
			}
		}
	} else if options.plan != nil {
		iter, err = reader.KeyIterator(options.plan.Keys)
	} else {
		return nil, errors.Internal("no plan to execute")
	}
	if err != nil {
		return nil, err
	}

	return runner.iterate(ctx, coll, iter, options.fieldFactory)
}

func (runner *StreamingQueryRunner) iterateOnSecondaryIndexStore(ctx context.Context, tx transaction.Tx, coll *schema.DefaultCollection, options readerOptions) ([]byte, error) {
	iter, err := NewSecondaryIndexReader(ctx, tx, coll, options.filter, options.plan)
	if err != nil {
		return nil, err
	}

	return runner.iterate(ctx, coll, NewFilterIterator(iter, options.filter), options.fieldFactory)
}

func (runner *StreamingQueryRunner) iterateOnSearchStore(ctx context.Context, coll *schema.DefaultCollection, options readerOptions) error {
	reqStatus, exists := metrics.RequestStatusFromContext(ctx)
	if reqStatus != nil && exists {
		reqStatus.SetCollectionRead()
	}
	rowReader := NewSearchReader(ctx, runner.searchStore, coll, qsearch.NewBuilder().
		Filter(options.filter).
		NoSearchFilter(options.noSearchFilter).
		SortOrder(options.sorting).
		PageSize(defaultPerPage).
		Build())

	// Note: Iterator expects the "options.filter" so that we use it to perform in-memory filtering.
	if _, err := runner.iterate(ctx, coll, rowReader.Iterator(ctx, coll, options.filter), options.fieldFactory); err != nil {
		return err
	}

	return nil
}

func (runner *StreamingQueryRunner) iterate(ctx context.Context, coll *schema.DefaultCollection, iterator Iterator, fieldFactory *read.FieldFactory) ([]byte, error) {
	var (
		row          Row
		branch       = metadata.MainBranch
		limit        int64
		skip         int64
		buffResponse []jsoniter.RawMessage
	)

	if runner.req.GetBranch() != "" {
		branch = runner.req.GetBranch()
	}
	if runner.req.GetOptions() != nil {
		limit = runner.req.GetOptions().Limit
	}
	if runner.req.GetOptions() != nil {
		skip = runner.req.GetOptions().Skip
	}

	isAcceptApplicationJSON := request.IsAcceptApplicationJSON(ctx)
	if isAcceptApplicationJSON && limit == 0 {
		limit = defaultReadLimit
	}

	limit += skip
	for i := int64(0); (limit == 0 || i < limit) && iterator.Next(&row); i++ {
		if skip > 0 {
			skip--
			continue
		}

		rawData := row.Data.RawData
		var err error

		if !coll.CompatibleSchemaSince(uint32(row.Data.Ver)) {
			rawData, err = coll.UpdateRowSchemaRaw(rawData, uint32(row.Data.Ver))
			if err != nil {
				return row.Key, err
			}

			metrics.SchemaReadOutdated(runner.req.GetProject(), branch, coll.Name)
		}

		newValue, err := fieldFactory.Apply(rawData)
		if ulog.E(err) {
			return row.Key, err
		}

		if isAcceptApplicationJSON {
			if newValue, err = runner.injectMDInsideBody(newValue, row.Data.CreateToProtoTS(), row.Data.UpdatedToProtoTS()); err != nil {
				return row.Key, err
			}

			// metadata will be injected inside the payload to simply unmarshaling for user
			buffResponse = append(buffResponse, newValue)
		} else {
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
	}

	if isAcceptApplicationJSON {
		marshaled, err := jsoniter.Marshal(buffResponse)
		if err != nil {
			return nil, err
		}

		if err := runner.streaming.Send(&api.ReadResponse{
			// no need to set resume token in this case.
			Data: marshaled,
		}); ulog.E(err) {
			return row.Key, err
		}
	}

	return row.Key, iterator.Interrupted()
}

func (runner *StreamingQueryRunner) injectMDInsideBody(raw []byte, createdAt *timestamppb.Timestamp, updatedAt *timestamppb.Timestamp) ([]byte, error) {
	if len(raw) == 0 || (createdAt == nil && updatedAt == nil) {
		return raw, nil
	}

	lastIndex := bytes.LastIndex(raw, []byte(`}`))
	if lastIndex <= 0 {
		return raw, nil
	}

	lastByte := raw[lastIndex]
	raw = raw[0:lastIndex]

	var buf bytes.Buffer
	_, _ = buf.Write(raw)

	if createdAt != nil {
		if err := runner.writeTimestamp(&buf, schema.ReservedFields[schema.CreatedAt], createdAt); err != nil {
			return nil, err
		}
	}

	if updatedAt != nil {
		if err := runner.writeTimestamp(&buf, schema.ReservedFields[schema.UpdatedAt], updatedAt); err != nil {
			return nil, err
		}
	}

	_ = buf.WriteByte(lastByte)

	return buf.Bytes(), nil
}

func (*StreamingQueryRunner) writeTimestamp(buf *bytes.Buffer, key string, timestamp *timestamppb.Timestamp) error {
	ts, err := jsoniter.Marshal(timestamp.AsTime())
	if err != nil {
		return err
	}

	// append comma first and write the pair
	_, _ = buf.Write([]byte(fmt.Sprintf(`, "%s":%s`, key, ts)))

	return nil
}

type ExplainQueryRunner struct {
	*BaseQueryRunner

	req *api.ReadRequest
}

func (runner *ExplainQueryRunner) Run(ctx context.Context, _ transaction.Tx, tenant *metadata.Tenant) (Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, nil, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return Response{}, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return Response{}, ctx, err
	}

	options, err := runner.buildReaderOptions(runner.req, collection)
	if err != nil {
		return Response{}, ctx, err
	}

	return Response{
		Response: buildExplainResp(options, collection, runner.req.Filter, runner.req.Sort),
	}, ctx, nil
}

const (
	PRIMARY   = "primary index"
	SECONDARY = "secondary index"
)

func buildExplainResp(options readerOptions, coll *schema.DefaultCollection, filter []byte, sortFields []byte) *api.ExplainResponse {
	explain := &api.ExplainResponse{
		Collection: coll.Name,
		Filter:     string(filter),
		Sorting:    string(sortFields),
	}

	if options.plan != nil {
		explain.ReadType = SECONDARY
		var keyRange []string
		for _, key := range options.plan.Keys {
			if len(key.IndexParts()) > 4 {
				var friendlyVal string
				val := key.IndexParts()[4]
				switch val {
				case nil:
					friendlyVal = "null"
				case 0xFF:
					friendlyVal = "$TIGRIS_MAX"
				default:
					if encodedString, ok := val.([]byte); ok {
						friendlyVal = fmt.Sprint(encodedString)
					} else {
						friendlyVal = fmt.Sprint(val)
					}
				}
				keyRange = append(keyRange, friendlyVal)
			}
		}

		explain.KeyRange = keyRange
		explain.Field = fmt.Sprint(options.plan.Keys[0].IndexParts()[2])
		return explain
	}
	explain.ReadType = PRIMARY
	return explain
}
