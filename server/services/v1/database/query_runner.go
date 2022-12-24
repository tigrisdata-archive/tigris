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

package database

import (
	"context"
	"encoding/json"
	"math"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	ljson "github.com/tigrisdata/tigris/lib/json"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/query/update"
	"github.com/tigrisdata/tigris/schema"
	cschema "github.com/tigrisdata/tigris/schema/lang"
	"github.com/tigrisdata/tigris/server/cdc"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/auth"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// QueryRunner is responsible for executing the current query and return the response.
type QueryRunner interface {
	Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error)
}

// ReadOnlyQueryRunner is the QueryRunner which decides inside the ReadOnly method if the query needs to be run inside
// a transaction or can opt to just execute the query. This interface allows caller to control the state of the transaction
// or can choose to execute without starting any transaction.
type ReadOnlyQueryRunner interface {
	ReadOnly(ctx context.Context, tenant *metadata.Tenant) (*Response, context.Context, error)
}

// QueryRunnerFactory is responsible for creating query runners for different queries.
type QueryRunnerFactory struct {
	txMgr       *transaction.Manager
	encoder     metadata.Encoder
	cdcMgr      *cdc.Manager
	searchStore search.Store
}

// NewQueryRunnerFactory returns QueryRunnerFactory object.
func NewQueryRunnerFactory(txMgr *transaction.Manager, cdcMgr *cdc.Manager, searchStore search.Store) *QueryRunnerFactory {
	return &QueryRunnerFactory{
		txMgr:       txMgr,
		encoder:     metadata.NewEncoder(),
		cdcMgr:      cdcMgr,
		searchStore: searchStore,
	}
}

func (f *QueryRunnerFactory) GetImportQueryRunner(r *api.ImportRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *ImportQueryRunner {
	return &ImportQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetInsertQueryRunner(r *api.InsertRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *InsertQueryRunner {
	return &InsertQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetReplaceQueryRunner(r *api.ReplaceRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *ReplaceQueryRunner {
	return &ReplaceQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetUpdateQueryRunner(r *api.UpdateRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *UpdateQueryRunner {
	return &UpdateQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetDeleteQueryRunner(r *api.DeleteRequest, qm *metrics.WriteQueryMetrics, accessToken *types.AccessToken) *DeleteQueryRunner {
	return &DeleteQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		queryMetrics:    qm,
	}
}

// GetStreamingQueryRunner returns StreamingQueryRunner.
func (f *QueryRunnerFactory) GetStreamingQueryRunner(r *api.ReadRequest, streaming Streaming, qm *metrics.StreamingQueryMetrics, accessToken *types.AccessToken) *StreamingQueryRunner {
	return &StreamingQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		streaming:       streaming,
		queryMetrics:    qm,
	}
}

// GetSearchQueryRunner for executing Search.
func (f *QueryRunnerFactory) GetSearchQueryRunner(r *api.SearchRequest, streaming SearchStreaming, qm *metrics.SearchQueryMetrics, accessToken *types.AccessToken) *SearchQueryRunner {
	return &SearchQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
		req:             r,
		streaming:       streaming,
		queryMetrics:    qm,
	}
}

func (f *QueryRunnerFactory) GetCollectionQueryRunner(accessToken *types.AccessToken) *CollectionQueryRunner {
	return &CollectionQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
	}
}

func (f *QueryRunnerFactory) GetDatabaseQueryRunner(accessToken *types.AccessToken) *DatabaseQueryRunner {
	return &DatabaseQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr, f.txMgr, f.searchStore, accessToken),
	}
}

type BaseQueryRunner struct {
	encoder     metadata.Encoder
	cdcMgr      *cdc.Manager
	searchStore search.Store
	txMgr       *transaction.Manager
	accessToken *types.AccessToken
}

func NewBaseQueryRunner(encoder metadata.Encoder, cdcMgr *cdc.Manager, txMgr *transaction.Manager, searchStore search.Store, accessToken *types.AccessToken) *BaseQueryRunner {
	return &BaseQueryRunner{
		encoder:     encoder,
		cdcMgr:      cdcMgr,
		searchStore: searchStore,
		txMgr:       txMgr,
		accessToken: accessToken,
	}
}

// getDatabaseFromTenant is a helper method to get database from the tenant object. Returns a user facing error if
// the database is not present.
func (runner *BaseQueryRunner) getDatabaseFromTenant(ctx context.Context, tenant *metadata.Tenant, dbName string, branch string) (*metadata.Database, error) {
	dbBranch := metadata.NewDatabaseNameWithBranch(dbName, branch)
	db, err := tenant.GetDatabase(ctx, dbBranch)
	if err != nil {
		return nil, createApiError(err)
	}

	return db, nil
}

// getDatabase is a helper method to return database either from the transactional context for explicit transactions or
// from the tenant object. Returns a user facing error if the database is not present.
func (runner *BaseQueryRunner) getDatabase(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant, dbName string, branch string) (*metadata.Database, error) {
	if tx.Context().GetStagedDatabase() != nil {
		// this means that some DDL operation has modified the database object, then we need to perform all the operations
		// on this staged database.
		return tx.Context().GetStagedDatabase().(*metadata.Database), nil
	}

	// otherwise, simply read from the in-memory cache/disk.
	dbBranch := metadata.NewDatabaseNameWithBranch(dbName, branch)
	db, err := tenant.GetDatabase(ctx, dbBranch)
	if err != nil {
		return nil, createApiError(err)
	}

	return db, nil
}

// getCollection is a wrapper around getCollection method on the database object to return a user facing error if the
// collection is not present.
func (runner *BaseQueryRunner) getCollection(db *metadata.Database, collName string) (*schema.DefaultCollection, error) {
	collection := db.GetCollection(collName)
	if collection == nil {
		return nil, errors.NotFound("collection doesn't exist '%s'", collName)
	}

	return collection, nil
}

func (runner *BaseQueryRunner) insertOrReplace(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant, db *metadata.Database,
	coll *schema.DefaultCollection, documents [][]byte, insert bool,
) (*internal.Timestamp, [][]byte, error) {
	var err error
	ts := internal.NewTimestamp()
	allKeys := make([][]byte, 0, len(documents))
	for _, doc := range documents {
		// reset it back to doc
		doc, err = runner.mutateAndValidatePayload(coll, newInsertPayloadMutator(coll, ts.ToRFC3339()), doc)
		if err != nil {
			return nil, nil, err
		}

		table, err := runner.encoder.EncodeTableName(tenant.GetNamespace(), db, coll)
		if err != nil {
			return nil, nil, err
		}

		keyGen := newKeyGenerator(doc, tenant.TableKeyGenerator, coll.Indexes.PrimaryKey)
		key, err := keyGen.generate(ctx, runner.txMgr, runner.encoder, table)
		if err != nil {
			return nil, nil, err
		}

		// we need to use keyGen updated document as it may be mutated by adding auto-generated keys.
		tableData := internal.NewTableDataWithTS(ts, nil, keyGen.document)
		tableData.SetVersion(coll.GetVersion())
		if insert || keyGen.forceInsert {
			// we use Insert API, in case user is using autogenerated primary key and has primary key field
			// as Int64 or timestamp to ensure uniqueness if multiple workers end up generating same timestamp.
			err = tx.Insert(ctx, key, tableData)
		} else {
			err = tx.Replace(ctx, key, tableData, false)
		}
		if err != nil {
			return nil, nil, err
		}
		allKeys = append(allKeys, keyGen.getKeysForResp())
	}
	return ts, allKeys, err
}

func (runner *BaseQueryRunner) mutateAndValidatePayload(coll *schema.DefaultCollection, mutator mutator, doc []byte) ([]byte, error) {
	deserializedDoc, err := ljson.Decode(doc)
	if ulog.E(err) {
		return doc, err
	}

	var nulls []string
	for k, v := range deserializedDoc {
		// for schema validation, if the field is set to null, remove it.
		if v == nil {
			// nulls will be added back if we need to serialize the payload again.
			nulls = append(nulls, k)
			delete(deserializedDoc, k)
		}
	}

	// this will mutate map, so we need to serialize this map again
	if err := mutator.stringToInt64(deserializedDoc); err != nil {
		return doc, err
	}
	if err := mutator.setDefaultsInIncomingPayload(deserializedDoc); err != nil {
		return doc, err
	}
	if err := coll.Validate(deserializedDoc); err != nil {
		// schema validation failed
		return doc, err
	}

	if mutator.isMutated() {
		for _, n := range nulls {
			deserializedDoc[n] = nil
		}

		return ljson.Encode(deserializedDoc)
	}

	return doc, nil
}

func (runner *BaseQueryRunner) buildKeysUsingFilter(tenant *metadata.Tenant, db *metadata.Database, coll *schema.DefaultCollection,
	reqFilter []byte, collation *api.Collation,
) ([]keys.Key, error) {
	filterFactory := filter.NewFactory(coll.QueryableFields, collation)
	filters, err := filterFactory.Factorize(reqFilter)
	if err != nil {
		return nil, err
	}

	encodedTable, err := runner.encoder.EncodeTableName(tenant.GetNamespace(), db, coll)
	if err != nil {
		return nil, err
	}

	primaryKeyIndex := coll.Indexes.PrimaryKey
	kb := filter.NewKeyBuilder(filter.NewStrictEqKeyComposer(func(indexParts ...interface{}) (keys.Key, error) {
		return runner.encoder.EncodeKey(encodedTable, primaryKeyIndex, indexParts)
	}))

	return kb.Build(filters, coll.Indexes.PrimaryKey.Fields)
}

func (runner *BaseQueryRunner) mustBeDocumentsCollection(collection *schema.DefaultCollection, method string) error {
	if collection.Type() != schema.DocumentsType {
		return errors.InvalidArgument("%s is only supported on collection type of 'documents'", method)
	}

	return nil
}

func (runner *BaseQueryRunner) getSortOrdering(coll *schema.DefaultCollection, sortReq jsoniter.RawMessage) (*sort.Ordering, error) {
	ordering, err := sort.UnmarshalSort(sortReq)
	if err != nil || ordering == nil {
		return nil, err
	}

	for i, sf := range *ordering {
		cf, err := coll.GetQueryableField(sf.Name)
		if err != nil {
			return nil, err
		}
		if cf.InMemoryName() != cf.Name() {
			(*ordering)[i].Name = cf.InMemoryName()
		}

		if !cf.Sortable {
			return nil, errors.InvalidArgument("Cannot sort on `%s` field", sf.Name)
		}
	}
	return ordering, nil
}

type ImportQueryRunner struct {
	*BaseQueryRunner

	req          *api.ImportRequest
	queryMetrics *metrics.WriteQueryMetrics
}

func (runner *ImportQueryRunner) evolveSchema(ctx context.Context, tenant *metadata.Tenant, rawSchema []byte) error {
	var sch cschema.Schema
	req := runner.req

	if rawSchema != nil {
		err := json.Unmarshal(rawSchema, &sch)
		if ulog.E(err) {
			return err
		}
	}

	err := schema.Infer(&sch, req.GetCollection(), req.GetDocuments(), req.GetPrimaryKey(), req.GetAutogenerated(), len(req.GetDocuments()))
	if err != nil {
		return err
	}

	b, err := json.Marshal(&sch)
	if ulog.E(err) {
		return err
	}

	log.Debug().Str("from", string(rawSchema)).Str("to", string(b)).Msg("evolving schema on import")

	schFactory, err := schema.Build(req.GetCollection(), b)
	if err != nil {
		return err
	}

	// Update collection schema in its own transaction
	tx, err := runner.txMgr.StartTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	db, err := runner.getDatabase(ctx, tx, tenant, req.GetProject(), "")
	if err != nil {
		return err
	}

	err = tenant.CreateCollection(ctx, tx, db, schFactory)
	if err == kv.ErrDuplicateKey {
		// this simply means, concurrently CreateCollection is called,
		return errors.Aborted("concurrent create collection request, aborting")
	}

	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (runner *ImportQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, tx, tenant, runner.req.GetProject(), "")
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	coll, err := runner.getCollection(db, runner.req.GetCollection())
	//FIXME: errors.As(err, &ep) doesn't work
	//nolint:golint,errorlint
	ep, ok := err.(*api.TigrisError)
	if err != nil && (!ok || ep.Code != api.Code_NOT_FOUND || !runner.req.CreateCollection) {
		return nil, ctx, err
	}
	if err != nil {
		// api.Code_NOT_FOUND && runner.req.CreateCollection
		// Infer schema and create collection from the first batch of documents
		if err := runner.evolveSchema(ctx, tenant, nil); err != nil {
			return nil, ctx, err
		}

		coll, err = runner.getCollection(db, runner.req.GetCollection())
		if err != nil {
			return nil, ctx, err
		}
	}

	if err = runner.mustBeDocumentsCollection(coll, "insert"); err != nil {
		return nil, ctx, err
	}

	ts, allKeys, err := runner.insertOrReplace(ctx, tx, tenant, db, coll, runner.req.GetDocuments(), true)
	if err != nil {
		if err == kv.ErrDuplicateKey {
			return nil, ctx, errors.AlreadyExists(err.Error())
		}

		ep, ok = err.(*api.TigrisError)
		if !ok || ep.Code != api.Code_INVALID_ARGUMENT {
			return nil, ctx, err
		}

		// Rollback original transaction, where partial batch insert might be succeeded.
		ulog.E(tx.Rollback(ctx))

		// Failed to insert due to schema change. Infer and update the schema.
		if err := runner.evolveSchema(ctx, tenant, coll.Schema); err != nil {
			return nil, ctx, err
		}

		// Retry insert after schema update in its own transaction
		tx, err := runner.txMgr.StartTx(ctx)
		if err != nil {
			return nil, ctx, err
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// Retry insert after updating the schema
		ts, allKeys, err = runner.insertOrReplace(ctx, tx, tenant, db, coll, runner.req.GetDocuments(), true)
		if err == kv.ErrDuplicateKey {
			return nil, ctx, errors.AlreadyExists(err.Error())
		}

		if err != nil {
			return nil, ctx, err
		}

		if err = tx.Commit(ctx); err != nil {
			return nil, ctx, err
		}
	}

	runner.queryMetrics.SetWriteType("import")
	metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	return &Response{
		CreatedAt: ts,
		AllKeys:   allKeys,
		Status:    InsertedStatus,
	}, ctx, nil
}

type InsertQueryRunner struct {
	*BaseQueryRunner

	req          *api.InsertRequest
	queryMetrics *metrics.WriteQueryMetrics
}

func (runner *InsertQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, tx, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	coll, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}
	if err = runner.mustBeDocumentsCollection(coll, "insert"); err != nil {
		return nil, ctx, err
	}

	ts, allKeys, err := runner.insertOrReplace(ctx, tx, tenant, db, coll, runner.req.GetDocuments(), true)
	if err != nil {
		if err == kv.ErrDuplicateKey {
			return nil, ctx, errors.AlreadyExists(err.Error())
		}

		return nil, ctx, err
	}

	runner.queryMetrics.SetWriteType("insert")
	metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	return &Response{
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

func (runner *ReplaceQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, tx, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	coll, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}
	if err = runner.mustBeDocumentsCollection(coll, "replace"); err != nil {
		return nil, ctx, err
	}

	ts, allKeys, err := runner.insertOrReplace(ctx, tx, tenant, db, coll, runner.req.GetDocuments(), false)
	if err != nil {
		return nil, ctx, err
	}

	runner.queryMetrics.SetWriteType("replace")
	metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	return &Response{
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

func (runner *UpdateQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	ts := internal.NewTimestamp()
	db, err := runner.getDatabase(ctx, tx, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}
	if err = runner.mustBeDocumentsCollection(collection, "update"); err != nil {
		return nil, ctx, err
	}

	var factory *update.FieldOperatorFactory
	factory, err = update.BuildFieldOperators(runner.req.Fields)
	if err != nil {
		return nil, ctx, err
	}

	if fieldOperator, ok := factory.FieldOperators[string(update.Set)]; ok {
		// Set operation needs schema validation as well as mutation if we need to convert numeric fields from string to int64
		fieldOperator.Input, err = runner.mutateAndValidatePayload(collection, newUpdatePayloadMutator(collection, ts.ToRFC3339()), fieldOperator.Input)
		if err != nil {
			return nil, ctx, err
		}
	}

	table, err := runner.encoder.EncodeTableName(tenant.GetNamespace(), db, collection)
	if err != nil {
		return nil, ctx, err
	}

	if filter.None(runner.req.Filter) {
		return nil, ctx, errors.InvalidArgument("updating all documents is not allowed")
	}

	var collation *api.Collation
	if runner.req.Options != nil {
		collation = runner.req.Options.Collation
	}

	var iterator Iterator
	reader := NewDatabaseReader(ctx, tx)
	iKeys, err := runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter, collation)
	if err == nil {
		iterator, err = reader.KeyIterator(iKeys)
	} else {
		if iterator, err = reader.ScanTable(table); err != nil {
			return nil, ctx, err
		}
		filterFactory := filter.NewFactory(collection.QueryableFields, collation)
		var filters []filter.Filter
		if filters, err = filterFactory.Factorize(runner.req.Filter); err != nil {
			return nil, ctx, err
		}

		iterator, err = reader.FilteredRead(iterator, filter.NewWrappedFilter(filters))
	}
	if err != nil {
		return nil, ctx, err
	}
	if len(iKeys) == 0 {
		runner.queryMetrics.SetWriteType("pkey")
	} else {
		runner.queryMetrics.SetWriteType("non-pkey")
	}

	limit := int32(0)
	if runner.req.Options != nil {
		limit = int32(runner.req.Options.Limit)
	}
	modifiedCount := int32(0)
	var row Row
	for iterator.Next(&row) {
		key, err := keys.FromBinary(table, row.Key)
		if err != nil {
			return nil, ctx, err
		}

		// MergeAndGet merge the user input with existing doc and return the merged JSON document which we need to
		// persist back.
		merged, err := factory.MergeAndGet(row.Data.RawData, collection)
		if err != nil {
			return nil, ctx, err
		}

		if len(collection.TaggedDefaultsForUpdate()) > 0 {
			// ToDo: revisit this path. We are deserializing here the merged payload (existing + incoming) and then
			// we are setting the updated value if any field is tagged with @updatedAt and then we are packing
			// it again.
			deserializedDoc, err := ljson.Decode(merged)
			if ulog.E(err) {
				return nil, ctx, err
			}

			mutator := newUpdatePayloadMutator(collection, ts.ToRFC3339())
			if err := mutator.setDefaultsInExistingPayload(deserializedDoc); err != nil {
				return nil, ctx, err
			}

			if mutator.isMutated() {
				if merged, err = ljson.Encode(deserializedDoc); err != nil {
					return nil, ctx, err
				}
			}
		}

		newData := internal.NewTableDataWithTS(row.Data.CreatedAt, ts, merged)
		newData.SetVersion(collection.GetVersion())
		// as we have merged the data, it is safe to call replace
		if err = tx.Replace(ctx, key, newData, true); ulog.E(err) {
			return nil, ctx, err
		}
		modifiedCount++
		if limit > 0 && modifiedCount == limit {
			break
		}
	}

	ctx = metrics.UpdateSpanTags(ctx, runner.queryMetrics)
	return &Response{
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

func (runner *DeleteQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	ts := internal.NewTimestamp()
	db, err := runner.getDatabase(ctx, tx, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}
	if err = runner.mustBeDocumentsCollection(collection, "delete"); err != nil {
		return nil, ctx, err
	}

	table, err := runner.encoder.EncodeTableName(tenant.GetNamespace(), db, collection)
	if err != nil {
		return nil, ctx, err
	}

	var iterator Iterator
	reader := NewDatabaseReader(ctx, tx)
	if filter.None(runner.req.Filter) {
		if iterator, err = reader.ScanTable(table); err != nil {
			return nil, ctx, err
		}
		runner.queryMetrics.SetWriteType("full_scan")
	} else {
		var collation *api.Collation
		if runner.req.Options != nil {
			collation = runner.req.Options.Collation
		}

		var iKeys []keys.Key
		if iKeys, err = runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter, collation); err == nil {
			iterator, err = reader.KeyIterator(iKeys)
		} else {
			if iterator, err = reader.ScanTable(table); err != nil {
				return nil, ctx, err
			}
			filterFactory := filter.NewFactory(collection.QueryableFields, collation)
			var filters []filter.Filter
			if filters, err = filterFactory.Factorize(runner.req.Filter); err != nil {
				return nil, ctx, err
			}

			iterator, err = reader.FilteredRead(iterator, filter.NewWrappedFilter(filters))
		}
		if len(iKeys) == 0 {
			runner.queryMetrics.SetWriteType("pkey")
		} else {
			runner.queryMetrics.SetWriteType("non-pkey")
		}
	}
	if err != nil {
		return nil, ctx, err
	}

	limit := int32(0)
	if runner.req.Options != nil {
		limit = int32(runner.req.Options.Limit)
	}
	modifiedCount := int32(0)
	var row Row
	for iterator.Next(&row) {
		key, err := keys.FromBinary(table, row.Key)
		if err != nil {
			return nil, ctx, err
		}

		if err = tx.Delete(ctx, key); ulog.E(err) {
			return nil, ctx, err
		}

		modifiedCount++
		if limit > 0 && modifiedCount == limit {
			break
		}
	}

	ctx = metrics.UpdateSpanTags(ctx, runner.queryMetrics)
	return &Response{
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
	table         []byte
	noFilter      bool
	inMemoryStore bool
	sorting       *sort.Ordering
	filter        *filter.WrappedFilter
	fieldFactory  *read.FieldFactory
}

func (runner *StreamingQueryRunner) buildReaderOptions(tenant *metadata.Tenant, db *metadata.Database, collection *schema.DefaultCollection) (readerOptions, error) {
	var err error
	options := readerOptions{}
	var collation *api.Collation
	if runner.req.Options != nil {
		collation = runner.req.Options.Collation
	}
	if options.sorting, err = runner.getSortOrdering(collection, runner.req.Sort); err != nil {
		return options, err
	}
	if options.filter, err = filter.NewFactory(collection.QueryableFields, collation).WrappedFilter(runner.req.Filter); err != nil {
		return options, err
	}
	if options.table, err = runner.encoder.EncodeTableName(tenant.GetNamespace(), db, collection); err != nil {
		return options, err
	}
	if options.fieldFactory, err = read.BuildFields(runner.req.GetFields()); err != nil {
		return options, err
	}
	if runner.req.Options != nil && len(runner.req.Options.Offset) > 0 {
		if options.from, err = keys.FromBinary(options.table, runner.req.Options.Offset); err != nil {
			return options, err
		}
	}

	if collection.Type() == schema.TopicType {
		// if it is event streaming then fallback to indexing store for all reads
		if !config.DefaultConfig.Search.IsReadEnabled() {
			if options.from == nil {
				// in this case, scan will happen from the beginning of the table.
				options.from = keys.NewKey(options.table)
			}
		} else {
			options.inMemoryStore = true
		}
	} else {
		var collation *api.Collation
		if runner.req.Options != nil {
			collation = runner.req.Options.Collation
		}

		if filter.None(runner.req.Filter) {
			if options.sorting != nil {
				options.inMemoryStore = true
			} else {
				options.noFilter = true
			}
		} else if options.ikeys, err = runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter, collation); err != nil {
			if !config.DefaultConfig.Search.IsReadEnabled() {
				if options.from == nil {
					// in this case, scan will happen from the beginning of the table.
					options.from = keys.NewKey(options.table)
				}
			} else {
				options.inMemoryStore = true
			}
		}
	}

	return options, nil
}

func (runner *StreamingQueryRunner) instrumentRunner(ctx context.Context, options readerOptions) context.Context {
	// Set read type
	if len(options.ikeys) == 0 {
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
func (runner *StreamingQueryRunner) ReadOnly(ctx context.Context, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.getDatabaseFromTenant(ctx, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}

	options, err := runner.buildReaderOptions(tenant, db, collection)
	if err != nil {
		return nil, ctx, err
	}

	if options.inMemoryStore {
		if err = runner.iterateOnIndexingStore(ctx, collection, options); err != nil {
			return nil, ctx, err
		}
		return &Response{}, ctx, nil
	}

	for {
		// A for loop is needed to recreate the transaction after exhausting the duration of the previous transaction.
		// This is mainly needed for long-running transactions, otherwise reads should be small.
		tx, err := runner.txMgr.StartTx(ctx)
		if err != nil {
			return nil, ctx, err
		}

		var last []byte
		last, err = runner.iterateOnKvStore(ctx, tx, options)
		_ = tx.Rollback(ctx)

		if err == kv.ErrTransactionMaxDurationReached {
			// We have received ErrTransactionMaxDurationReached i.e. 5 second transaction limit, so we need to retry the
			// transaction.
			options.from, _ = keys.FromBinary(options.table, last)
			continue
		}
		if err != nil {
			return nil, ctx, nil
		}

		ctx = runner.instrumentRunner(ctx, options)

		return &Response{}, ctx, nil
	}
}

// Run is responsible for running the read in the transaction started by the session manager. This doesn't do any retry
// if we see ErrTransactionMaxDurationReached which is expected because we do not expect caller to do long reads in an
// explicit transaction.
func (runner *StreamingQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.getDatabase(ctx, tx, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}

	options, err := runner.buildReaderOptions(tenant, db, collection)
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.instrumentRunner(ctx, options)

	if options.inMemoryStore {
		if err = runner.iterateOnIndexingStore(ctx, collection, options); err != nil {
			return nil, ctx, err
		}
		return &Response{}, ctx, nil
	} else {
		if _, err = runner.iterateOnKvStore(ctx, tx, options); err != nil {
			return nil, ctx, err
		}
		return &Response{}, ctx, nil
	}
}

func (runner *StreamingQueryRunner) iterateOnKvStore(ctx context.Context, tx transaction.Tx, options readerOptions) ([]byte, error) {
	var err error
	var iter Iterator
	reader := NewDatabaseReader(ctx, tx)
	if len(options.ikeys) > 0 {
		iter, err = reader.KeyIterator(options.ikeys)
	} else if options.from != nil {
		if iter, err = reader.ScanIterator(options.from); err == nil {
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

	return runner.iterate(iter, options.fieldFactory)
}

func (runner *StreamingQueryRunner) iterateOnIndexingStore(ctx context.Context, collection *schema.DefaultCollection, options readerOptions) error {
	rowReader := NewSearchReader(ctx, runner.searchStore, collection, qsearch.NewBuilder().
		Filter(options.filter).
		SortOrder(options.sorting).
		PageSize(defaultPerPage).
		Build())

	if _, err := runner.iterate(rowReader.Iterator(collection, options.filter), options.fieldFactory); err != nil {
		return err
	}

	return nil
}

func (runner *StreamingQueryRunner) iterate(iterator Iterator, fieldFactory *read.FieldFactory) ([]byte, error) {
	limit, totalResults := int64(0), int64(0)
	if runner.req.GetOptions() != nil {
		limit = runner.req.GetOptions().Limit
	}

	var lastRowKey []byte
	var row Row
	for iterator.Next(&row) {
		if limit > 0 && limit <= totalResults {
			return lastRowKey, nil
		}

		newValue, err := fieldFactory.Apply(row.Data.RawData)
		if ulog.E(err) {
			return lastRowKey, err
		}

		if err := runner.streaming.Send(&api.ReadResponse{
			Data: newValue,
			Metadata: &api.ResponseMetadata{
				CreatedAt: row.Data.CreateToProtoTS(),
				UpdatedAt: row.Data.UpdatedToProtoTS(),
			},
			ResumeToken: row.Key,
		}); ulog.E(err) {
			return lastRowKey, err
		}
		lastRowKey = row.Key
		totalResults++
	}

	return lastRowKey, iterator.Interrupted()
}

// SearchQueryRunner is a runner used for Queries that are reads and needs to return result in streaming fashion.
type SearchQueryRunner struct {
	*BaseQueryRunner

	req          *api.SearchRequest
	streaming    SearchStreaming
	queryMetrics *metrics.SearchQueryMetrics
}

// ReadOnly on search query runner is implemented as search queries do not need to be inside a transaction; in fact,
// there is no need to start any transaction for search queries as they are simply forwarded to the indexing store.
func (runner *SearchQueryRunner) ReadOnly(ctx context.Context, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.getDatabaseFromTenant(ctx, tenant, runner.req.GetProject(), runner.req.GetBranch())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.getCollection(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}

	wrappedF, err := filter.NewFactory(collection.QueryableFields, runner.req.Collation).WrappedFilter(runner.req.Filter)
	if err != nil {
		return nil, ctx, err
	}

	searchFields, err := runner.getSearchFields(collection)
	if err != nil {
		return nil, ctx, err
	}

	facets, err := runner.getFacetFields(collection)
	if err != nil {
		return nil, ctx, err
	}

	if len(facets.Fields) == 0 {
		runner.queryMetrics.SetSearchType("search_all")
	} else {
		runner.queryMetrics.SetSearchType("faceted")
	}

	fieldSelection, err := runner.getFieldSelection(collection)
	if err != nil {
		return nil, ctx, err
	}

	sortOrder, err := runner.getSortOrdering(collection, runner.req.Sort)
	if err != nil {
		return nil, ctx, err
	}

	if sortOrder != nil {
		runner.queryMetrics.SetSort(true)
	} else {
		runner.queryMetrics.SetSort(false)
	}

	ctx = metrics.UpdateSpanTags(ctx, runner.queryMetrics)

	pageSize := int(runner.req.PageSize)
	if pageSize == 0 {
		pageSize = defaultPerPage
	}
	var totalPages *int32

	searchQ := qsearch.NewBuilder().
		Query(runner.req.Q).
		SearchFields(searchFields).
		Facets(facets).
		PageSize(pageSize).
		Filter(wrappedF).
		ReadFields(fieldSelection).
		SortOrder(sortOrder).
		Build()

	searchReader := NewSearchReader(ctx, runner.searchStore, collection, searchQ)
	var iterator *FilterableSearchIterator
	if runner.req.Page != 0 {
		iterator = searchReader.SinglePageIterator(collection, wrappedF, runner.req.Page)
	} else {
		iterator = searchReader.Iterator(collection, wrappedF)
	}
	if err != nil {
		return nil, ctx, err
	}

	pageNo := int32(defaultPageNo)
	if runner.req.Page > 0 {
		pageNo = runner.req.Page
	}
	for {
		resp := &api.SearchResponse{}
		var row Row
		for iterator.Next(&row) {
			if searchQ.ReadFields != nil {
				// apply field selection
				newValue, err := searchQ.ReadFields.Apply(row.Data.RawData)
				if ulog.E(err) {
					return nil, ctx, err
				}
				row.Data.RawData = newValue
			}

			resp.Hits = append(resp.Hits, &api.SearchHit{
				Data: row.Data.RawData,
				Metadata: &api.SearchHitMeta{
					CreatedAt: row.Data.CreateToProtoTS(),
					UpdatedAt: row.Data.UpdatedToProtoTS(),
				},
			})

			if len(resp.Hits) == pageSize {
				break
			}
		}

		resp.Facets = iterator.getFacets()
		if totalPages == nil {
			tp := int32(math.Ceil(float64(iterator.getTotalFound()) / float64(pageSize)))
			totalPages = &tp
		}

		resp.Meta = &api.SearchMetadata{
			Found:      iterator.getTotalFound(),
			TotalPages: *totalPages,
			Page: &api.Page{
				Current: pageNo,
				Size:    int32(searchQ.PageSize),
			},
		}
		// if no hits, got error, send only error
		// if no hits, no error, at least one response and break
		// if some hits, got an error, send current hits and then error (will be zero hits next time)
		// if some hits, no error, continue to send response
		if len(resp.Hits) == 0 {
			if iterator.Interrupted() != nil {
				return nil, ctx, iterator.Interrupted()
			}
			if pageNo > defaultPageNo && pageNo > runner.req.Page {
				break
			}
		}

		if err := runner.streaming.Send(resp); err != nil {
			return nil, ctx, err
		}

		pageNo++
	}

	return &Response{}, ctx, nil
}

func (runner *SearchQueryRunner) getSearchFields(coll *schema.DefaultCollection) ([]string, error) {
	searchFields := runner.req.SearchFields
	if len(searchFields) == 0 {
		// this is to include all searchable fields if not present in the query
		for _, cf := range coll.GetQueryableFields() {
			if cf.DataType == schema.StringType {
				searchFields = append(searchFields, cf.InMemoryName())
			}
		}
	} else {
		for i, sf := range searchFields {
			cf, err := coll.GetQueryableField(sf)
			if err != nil {
				return nil, err
			}
			if cf.DataType != schema.StringType {
				return nil, errors.InvalidArgument("`%s` is not a searchable field. Only string fields can be queried", sf)
			}
			if cf.InMemoryName() != cf.Name() {
				searchFields[i] = cf.InMemoryName()
			}
		}
	}
	return searchFields, nil
}

func (runner *SearchQueryRunner) getFacetFields(coll *schema.DefaultCollection) (qsearch.Facets, error) {
	facets, err := qsearch.UnmarshalFacet(runner.req.Facet)
	if err != nil {
		return qsearch.Facets{}, err
	}

	for i, ff := range facets.Fields {
		cf, err := coll.GetQueryableField(ff.Name)
		if err != nil {
			return qsearch.Facets{}, err
		}
		if !cf.Faceted {
			return qsearch.Facets{}, errors.InvalidArgument(
				"Cannot generate facets for `%s`. Faceting is only supported for numeric and text fields", ff.Name)
		}
		if cf.InMemoryName() != cf.Name() {
			facets.Fields[i].Name = cf.InMemoryName()
		}
	}

	return facets, nil
}

func (runner *SearchQueryRunner) getFieldSelection(coll *schema.DefaultCollection) (*read.FieldFactory, error) {
	var selectionFields []string

	// Only one of include/exclude. Honor inclusion over exclusion
	//nolint:golint,gocritic
	if len(runner.req.IncludeFields) > 0 {
		selectionFields = runner.req.IncludeFields
	} else if len(runner.req.ExcludeFields) > 0 {
		selectionFields = runner.req.ExcludeFields
	} else {
		return nil, nil
	}

	factory := &read.FieldFactory{
		Include: map[string]read.Field{},
		Exclude: map[string]read.Field{},
	}

	for _, sf := range selectionFields {
		cf, err := coll.GetQueryableField(sf)
		if err != nil {
			return nil, err
		}

		factory.AddField(&read.SimpleField{
			Name: cf.Name(),
			Incl: len(runner.req.IncludeFields) > 0,
		})
	}

	return factory, nil
}

type CollectionQueryRunner struct {
	*BaseQueryRunner

	dropReq           *api.DropCollectionRequest
	listReq           *api.ListCollectionsRequest
	createOrUpdateReq *api.CreateOrUpdateCollectionRequest
	describeReq       *api.DescribeCollectionRequest
}

func (runner *CollectionQueryRunner) SetCreateOrUpdateCollectionReq(create *api.CreateOrUpdateCollectionRequest) {
	runner.createOrUpdateReq = create
}

func (runner *CollectionQueryRunner) SetDropCollectionReq(drop *api.DropCollectionRequest) {
	runner.dropReq = drop
}

func (runner *CollectionQueryRunner) SetListCollectionReq(list *api.ListCollectionsRequest) {
	runner.listReq = list
}

func (runner *CollectionQueryRunner) SetDescribeCollectionReq(describe *api.DescribeCollectionRequest) {
	runner.describeReq = describe
}

func (runner *CollectionQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	switch {
	case runner.dropReq != nil:
		db, err := runner.getDatabase(ctx, tx, tenant, runner.dropReq.GetProject(), runner.dropReq.GetBranch())
		if err != nil {
			return nil, ctx, err
		}

		if tx.Context().GetStagedDatabase() == nil {
			// do not modify the actual database object yet, just work on the clone
			db = db.Clone()
			tx.Context().StageDatabase(db)
		}

		if err = tenant.DropCollection(ctx, tx, db, runner.dropReq.GetCollection()); err != nil {
			return nil, ctx, err
		}

		return &Response{
			Status: DroppedStatus,
		}, ctx, nil
	case runner.createOrUpdateReq != nil:
		db, err := runner.getDatabase(ctx, tx, tenant, runner.createOrUpdateReq.GetProject(), runner.createOrUpdateReq.GetBranch())
		if err != nil {
			return nil, ctx, err
		}

		if db.GetCollection(runner.createOrUpdateReq.GetCollection()) != nil && runner.createOrUpdateReq.OnlyCreate {
			// check if onlyCreate is set and if set then return an error if collection already exist
			return nil, ctx, errors.AlreadyExists("collection already exist")
		}

		schFactory, err := schema.Build(runner.createOrUpdateReq.GetCollection(), runner.createOrUpdateReq.GetSchema())
		if err != nil {
			return nil, ctx, err
		}

		if tx.Context().GetStagedDatabase() == nil {
			// do not modify the actual database object yet, just work on the clone
			db = db.Clone()
			tx.Context().StageDatabase(db)
		}

		if err = tenant.CreateCollection(ctx, tx, db, schFactory); err != nil {
			if err == kv.ErrDuplicateKey {
				// this simply means, concurrently CreateCollection is called,
				return nil, ctx, errors.Aborted("concurrent create collection request, aborting")
			}
			return nil, ctx, err
		}

		return &Response{
			Status: CreatedStatus,
		}, ctx, nil
	case runner.listReq != nil:
		db, err := runner.getDatabase(ctx, tx, tenant, runner.listReq.GetProject(), runner.listReq.GetBranch())
		if err != nil {
			return nil, ctx, err
		}

		collectionList := db.ListCollection()
		collections := make([]*api.CollectionInfo, len(collectionList))
		for i, c := range collectionList {
			collections[i] = &api.CollectionInfo{
				Collection: c.GetName(),
			}
		}
		return &Response{
			Response: &api.ListCollectionsResponse{
				Collections: collections,
			},
		}, ctx, nil
	case runner.describeReq != nil:
		db, err := runner.getDatabase(ctx, tx, tenant, runner.describeReq.GetProject(), runner.describeReq.GetBranch())
		if err != nil {
			return nil, ctx, err
		}
		namespace, err := request.GetNamespace(ctx)
		if err != nil {
			namespace = "unknown"
		}

		coll, err := runner.getCollection(db, runner.describeReq.GetCollection())
		if err != nil {
			return nil, ctx, err
		}

		size, err := tenant.CollectionSize(ctx, db, coll)
		if err != nil {
			return nil, ctx, err
		}

		tenantName := tenant.GetNamespace().Metadata().Name

		metrics.UpdateCollectionSizeMetrics(namespace, tenantName, db.Name(), coll.GetName(), size)
		// remove indexing version from the schema before returning the response
		sch := schema.RemoveIndexingVersion(coll.Schema)

		// Generate schema in the requested language format
		if runner.describeReq.SchemaFormat != "" {
			sch, err = schema.Generate(sch, runner.describeReq.SchemaFormat)
			if err != nil {
				return nil, ctx, err
			}
		}

		return &Response{
			Response: &api.DescribeCollectionResponse{
				Collection: coll.Name,
				Metadata:   &api.CollectionMetadata{},
				Schema:     sch,
				Size:       size,
			},
		}, ctx, nil
	}

	return &Response{}, ctx, errors.Unknown("unknown request path")
}

type DatabaseQueryRunner struct {
	*BaseQueryRunner

	delete       *api.DeleteProjectRequest
	create       *api.CreateProjectRequest
	list         *api.ListProjectsRequest
	describe     *api.DescribeDatabaseRequest
	createBranch *api.CreateBranchRequest
	deleteBranch *api.DeleteBranchRequest
}

func (runner *DatabaseQueryRunner) SetCreateProjectReq(create *api.CreateProjectRequest) {
	runner.create = create
}

func (runner *DatabaseQueryRunner) SetDeleteProjectReq(d *api.DeleteProjectRequest) {
	runner.delete = d
}

func (runner *DatabaseQueryRunner) SetListProjectsReq(list *api.ListProjectsRequest) {
	runner.list = list
}

func (runner *DatabaseQueryRunner) SetDescribeDatabaseReq(describe *api.DescribeDatabaseRequest) {
	runner.describe = describe
}

func (runner *DatabaseQueryRunner) SetCreateBranchReq(create *api.CreateBranchRequest) {
	runner.createBranch = create
}

func (runner *DatabaseQueryRunner) SetDeleteBranchReq(deleteBranch *api.DeleteBranchRequest) {
	runner.deleteBranch = deleteBranch
}

func (runner *DatabaseQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	switch {
	case runner.delete != nil:
		exist, err := tenant.DropDatabase(ctx, tx, runner.delete.GetProject())
		if err != nil {
			return nil, ctx, err
		}
		if !exist {
			return nil, ctx, errors.NotFound("database doesn't exist '%s'", runner.delete.GetProject())
		}

		return &Response{
			Status: DroppedStatus,
		}, ctx, nil
	case runner.create != nil:
		dbMetadata, err := createDatabaseMetadata(ctx)
		if err != nil {
			return nil, ctx, err
		}
		exist, err := tenant.CreateDatabase(ctx, tx, runner.create.GetProject(), dbMetadata)
		if exist || err == kv.ErrDuplicateKey {
			return nil, ctx, errors.AlreadyExists("database already exist")
		}
		if err != nil {
			return nil, ctx, err
		}

		return &Response{
			Status: CreatedStatus,
		}, ctx, nil
	case runner.list != nil:
		// list projects need not include any branches
		databaseList := tenant.ListDatabasesOnly(ctx)
		databases := make([]*api.ProjectInfo, len(databaseList))
		for i, l := range databaseList {
			databases[i] = &api.ProjectInfo{
				Project: l,
			}
		}
		return &Response{
			Response: &api.ListProjectsResponse{
				Projects: databases,
			},
		}, ctx, nil
	case runner.describe != nil:
		db, err := runner.getDatabase(ctx, tx, tenant, runner.describe.GetProject(), runner.describe.GetBranch())
		if err != nil {
			return nil, ctx, err
		}

		namespace, err := request.GetNamespace(ctx)
		if err != nil {
			namespace = "unknown"
		}
		tenantName := tenant.GetNamespace().Metadata().Name

		collectionList := db.ListCollection()
		collections := make([]*api.CollectionDescription, len(collectionList))
		for i, c := range collectionList {
			size, err := tenant.CollectionSize(ctx, db, c)
			if err != nil {
				return nil, ctx, err
			}

			metrics.UpdateCollectionSizeMetrics(namespace, tenantName, db.Name(), c.GetName(), size)

			// remove indexing version from the schema before returning the response
			sch := schema.RemoveIndexingVersion(c.Schema)

			// Generate schema in the requested language format
			if runner.describe.SchemaFormat != "" {
				sch, err = schema.Generate(sch, runner.describe.SchemaFormat)
				if err != nil {
					return nil, ctx, err
				}
			}

			collections[i] = &api.CollectionDescription{
				Collection: c.GetName(),
				Metadata:   &api.CollectionMetadata{},
				Schema:     sch,
				Size:       size,
			}
		}

		size, err := tenant.DatabaseSize(ctx, db)
		if err != nil {
			return nil, ctx, err
		}

		metrics.UpdateDbSizeMetrics(namespace, tenantName, db.Name(), size)

		return &Response{
			Response: &api.DescribeDatabaseResponse{
				Metadata:    &api.DatabaseMetadata{},
				Collections: collections,
				Size:        size,
				Branches:    tenant.ListBranches(ctx, db),
			},
		}, ctx, nil

	case runner.createBranch != nil:
		dbBranch := metadata.NewDatabaseNameWithBranch(runner.createBranch.GetProject(), runner.createBranch.GetBranch())
		err := tenant.CreateBranch(ctx, tx, dbBranch)
		if err != nil {
			return nil, ctx, createApiError(err)
		}
		return &Response{
			Response: &api.CreateBranchResponse{
				Status: CreatedStatus,
			},
		}, ctx, nil
	case runner.deleteBranch != nil:
		dbBranch := metadata.NewDatabaseNameWithBranch(runner.deleteBranch.GetProject(), runner.deleteBranch.GetBranch())
		err := tenant.DeleteBranch(ctx, tx, dbBranch)
		if err != nil {
			return nil, ctx, createApiError(err)
		}
		return &Response{
			Response: &api.DeleteBranchResponse{
				Status: DeletedStatus,
			},
		}, ctx, nil
	}

	return &Response{}, ctx, errors.Unknown("unknown request path")
}

func createDatabaseMetadata(ctx context.Context) (*metadata.DatabaseMetadata, error) {
	currentSub, err := auth.GetCurrentSub(ctx)
	if err != nil && !config.DefaultConfig.Auth.EnableNamespaceIsolation {
		return nil, nil
	} else if err != nil {
		return nil, errors.Internal("Failed to create database metadata")
	}
	return &metadata.DatabaseMetadata{
		Id:        0, // it will be set to right value later on
		Creator:   currentSub,
		CreatedAt: time.Now().Unix(),
	}, nil
}
