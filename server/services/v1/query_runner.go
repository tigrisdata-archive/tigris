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

package v1

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/keys"
	"github.com/tigrisdata/tigrisdb/query/filter"
	"github.com/tigrisdata/tigrisdb/query/read"
	"github.com/tigrisdata/tigrisdb/query/update"
	"github.com/tigrisdata/tigrisdb/schema"
	"github.com/tigrisdata/tigrisdb/server/metadata"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"github.com/tigrisdata/tigrisdb/value"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

// QueryRunner is responsible for executing the current query and return the response
type QueryRunner interface {
	Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, error)
}

// QueryRunnerFactory is responsible for creating query runners for different queries
type QueryRunnerFactory struct {
	txMgr   *transaction.Manager
	encoder metadata.Encoder
}

// NewQueryRunnerFactory returns QueryRunnerFactory object
func NewQueryRunnerFactory(txMgr *transaction.Manager, encoder metadata.Encoder) *QueryRunnerFactory {
	return &QueryRunnerFactory{
		txMgr:   txMgr,
		encoder: encoder,
	}
}

func (f *QueryRunnerFactory) GetInsertQueryRunner(r *api.InsertRequest) *InsertQueryRunner {
	return &InsertQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetReplaceQueryRunner(r *api.ReplaceRequest) *ReplaceQueryRunner {
	return &ReplaceQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetUpdateQueryRunner(r *api.UpdateRequest) *UpdateQueryRunner {
	return &UpdateQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetDeleteQueryRunner(r *api.DeleteRequest) *DeleteQueryRunner {
	return &DeleteQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder),
		req:             r,
	}
}

// GetStreamingQueryRunner returns StreamingQueryRunner
func (f *QueryRunnerFactory) GetStreamingQueryRunner(r *api.ReadRequest, streaming Streaming) *StreamingQueryRunner {
	return &StreamingQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder),
		req:             r,
		streaming:       streaming,
	}
}

func (f *QueryRunnerFactory) GetCollectionQueryRunner() *CollectionQueryRunner {
	return &CollectionQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder),
	}
}

func (f *QueryRunnerFactory) GetDatabaseQueryRunner() *DatabaseQueryRunner {
	return &DatabaseQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder),
	}
}

type BaseQueryRunner struct {
	encoder metadata.Encoder
}

func NewBaseQueryRunner(encoder metadata.Encoder) *BaseQueryRunner {
	return &BaseQueryRunner{
		encoder: encoder,
	}
}

func (runner *BaseQueryRunner) GetDatabase(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant, dbName string) (*metadata.Database, error) {
	db, err := tenant.GetDatabase(ctx, tx, dbName)
	if err != nil {
		return nil, err
	}
	if db == nil {
		// database not found
		return nil, api.Errorf(codes.InvalidArgument, "database doesn't exists '%s'", dbName)
	}

	return db, nil
}

func (runner *BaseQueryRunner) GetCollections(db *metadata.Database, collName string) (*schema.DefaultCollection, error) {
	collection := db.GetCollection(collName)
	if collection == nil {
		return nil, api.Errorf(codes.InvalidArgument, "collection doesn't exists '%s'", collName)
	}

	return collection, nil
}

func (runner *BaseQueryRunner) extractIndexParts(userDefinedKeys []*schema.Field, doc map[string]*structpb.Value) ([]interface{}, error) {
	var appendTo []interface{}
	for _, v := range userDefinedKeys {
		k, ok := doc[v.Name()]
		if !ok {
			return nil, ulog.CE("missing index key column(s) %v", v)
		}

		val, err := value.NewValueUsingSchema(v, k)
		if err != nil {
			return nil, err
		}
		appendTo = append(appendTo, val.AsInterface())
	}
	if len(appendTo) == 0 {
		return nil, ulog.CE("missing index key column(s)")
	}
	return appendTo, nil
}

func (runner *BaseQueryRunner) insertOrReplace(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant, db *metadata.Database, coll *schema.DefaultCollection, documents [][]byte, insert bool) error {
	var err error
	for _, d := range documents {
		// ToDo: need to implement our own decoding to only extract custom keys
		var s = &structpb.Struct{}
		if err = json.Unmarshal(d, s); ulog.E(err) {
			return err
		}

		indexParts, err := runner.extractIndexParts(coll.Indexes.PrimaryKey.Fields, s.GetFields())
		if ulog.E(err) {
			return err
		}

		var key keys.Key
		if key, err = runner.encoder.EncodeKey(tenant.GetNamespace(), db, coll, coll.Indexes.PrimaryKey, indexParts); ulog.E(err) {
			return err
		}

		if insert {
			err = tx.Insert(ctx, key, d)
		} else {
			err = tx.Replace(ctx, key, d)
		}
		if err != nil {
			return err
		}
	}
	return err
}

func (runner *BaseQueryRunner) buildKeysUsingFilter(tenant *metadata.Tenant, db *metadata.Database, coll *schema.DefaultCollection, reqFilter []byte) ([]keys.Key, error) {
	filters, err := filter.Build(reqFilter)
	if err != nil {
		return nil, err
	}

	primaryKeyIndex := coll.Indexes.PrimaryKey
	kb := filter.NewKeyBuilder(filter.NewStrictEqKeyComposer(func(indexParts ...interface{}) (keys.Key, error) {
		return runner.encoder.EncodeKey(tenant.GetNamespace(), db, coll, primaryKeyIndex, indexParts)
	}))
	iKeys, err := kb.Build(filters, coll.Indexes.PrimaryKey.Fields)
	if err != nil {
		return nil, err
	}

	return iKeys, nil
}

type InsertQueryRunner struct {
	*BaseQueryRunner

	req *api.InsertRequest
}

func (runner *InsertQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, error) {
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, err
	}

	coll, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	if err := runner.insertOrReplace(ctx, tx, tenant, db, coll, runner.req.GetDocuments(), true); err != nil {
		return nil, err
	}
	return &Response{}, nil
}

type ReplaceQueryRunner struct {
	*BaseQueryRunner

	req *api.ReplaceRequest
}

func (runner *ReplaceQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, error) {
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, err
	}

	coll, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	if err := runner.insertOrReplace(ctx, tx, tenant, db, coll, runner.req.GetDocuments(), false); err != nil {
		return nil, err
	}
	return &Response{}, nil
}

type UpdateQueryRunner struct {
	*BaseQueryRunner

	req *api.UpdateRequest
}

func (runner *UpdateQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, error) {
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, err
	}

	collection, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	iKeys, err := runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter)
	if err != nil {
		return nil, err
	}

	var factory *update.FieldOperatorFactory
	factory, err = update.BuildFieldOperators(runner.req.Fields)
	if err != nil {
		return nil, err
	}

	for _, key := range iKeys {
		// decode the fields now
		if err = tx.Update(ctx, key, func(existingDoc []byte) ([]byte, error) {
			merged, er := factory.MergeAndGet(existingDoc)
			if er != nil {
				return nil, er
			}
			return merged, nil
		}); ulog.E(err) {
			return nil, err
		}
	}

	return &Response{}, err
}

type DeleteQueryRunner struct {
	*BaseQueryRunner

	req *api.DeleteRequest
}

func (runner *DeleteQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, error) {
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, err
	}

	collection, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	iKeys, err := runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter)
	if err != nil {
		return nil, err
	}

	for _, key := range iKeys {
		if err = tx.Delete(ctx, key); ulog.E(err) {
			return nil, err
		}
	}

	return &Response{}, nil
}

// StreamingQueryRunner is a runner used for Queries that are reads and needs to return result in streaming fashion
type StreamingQueryRunner struct {
	*BaseQueryRunner

	req       *api.ReadRequest
	streaming Streaming
}

// Run is responsible for running/executing the query
func (runner *StreamingQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, error) {
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, err
	}

	collection, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	fieldFactory, err := read.BuildFields(runner.req.GetFields())
	if ulog.E(err) {
		return nil, err
	}

	if filter.IsFullCollectionScan(runner.req.GetFilter()) {
		table := runner.encoder.EncodeTableName(tenant.GetNamespace(), db, collection)
		resp, err := runner.iterateCollection(ctx, tx, table, fieldFactory)
		if err != nil {
			log.Debug().Str("db", db.Name()).Str("coll", collection.Name).Bytes("encoding", table).Err(err).Msg("full scan")
		}

		return resp, err
	}

	// or this is a read request that needs to be streamed after filtering the keys.
	iKeys, err := runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter)
	if err != nil {
		return nil, err
	}

	resp, err := runner.iterateKeys(ctx, tx, iKeys, fieldFactory)
	if err != nil {
		log.Debug().Str("db", db.Name()).Str("coll", collection.Name).Err(err).Msg("iterate keys")
	}
	return resp, err
}

// iterateCollection is used to scan the entire collection.
func (runner *StreamingQueryRunner) iterateCollection(ctx context.Context, tx transaction.Tx, table []byte, fieldFactory *read.FieldFactory) (*Response, error) {
	var totalResults int64 = 0
	if err := runner.iterate(ctx, tx, keys.NewKey(table), fieldFactory, &totalResults); err != nil {
		return nil, err
	}

	return &Response{}, nil
}

// iterateKeys is responsible for building keys from the filter and then executing the query. A key could be a primary
// key or an index key.
func (runner *StreamingQueryRunner) iterateKeys(ctx context.Context, tx transaction.Tx, iKeys []keys.Key, fieldFactory *read.FieldFactory) (*Response, error) {
	var totalResults int64 = 0
	for _, key := range iKeys {
		if err := runner.iterate(ctx, tx, key, fieldFactory, &totalResults); err != nil {
			return nil, err
		}
	}

	return &Response{}, nil
}

func (runner *StreamingQueryRunner) iterate(ctx context.Context, tx transaction.Tx, key keys.Key, fieldFactory *read.FieldFactory, totalResults *int64) error {
	it, err := tx.Read(ctx, key)
	if ulog.E(err) {
		return err
	}

	var limit int64 = 0
	if runner.req.GetOptions() != nil {
		limit = runner.req.GetOptions().Limit
	}

	var v kv.KeyValue
	for it.Next(&v) {
		if limit > 0 && limit <= *totalResults {
			return nil
		}

		newValue, err := fieldFactory.Apply(v.Value)
		if ulog.E(err) {
			return err
		}

		if err := runner.streaming.Send(&api.ReadResponse{
			Doc: newValue,
			Key: v.FDBKey,
		}); ulog.E(err) {
			return err
		}

		*totalResults++
	}

	return it.Err()
}

type CollectionQueryRunner struct {
	*BaseQueryRunner

	drop           *api.DropCollectionRequest
	list           *api.ListCollectionsRequest
	createOrUpdate *api.CreateOrUpdateCollectionRequest
}

func (runner *CollectionQueryRunner) SetCreateOrUpdateCollectionReq(create *api.CreateOrUpdateCollectionRequest) {
	runner.createOrUpdate = create
}

func (runner *CollectionQueryRunner) SetDropCollectionReq(drop *api.DropCollectionRequest) {
	runner.drop = drop
}

func (runner *CollectionQueryRunner) SetListCollectionReq(list *api.ListCollectionsRequest) {
	runner.list = list
}

func (runner *CollectionQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, error) {
	if runner.drop != nil {
		db, err := runner.GetDatabase(ctx, tx, tenant, runner.drop.GetDb())
		if err != nil {
			return nil, err
		}

		if err = tenant.DropCollection(ctx, tx, db, runner.drop.GetCollection()); err != nil {
			return nil, err
		}
		return &Response{}, nil
	} else if runner.createOrUpdate != nil {
		db, err := runner.GetDatabase(ctx, tx, tenant, runner.createOrUpdate.GetDb())
		if err != nil {
			return nil, err
		}

		if db.GetCollection(runner.createOrUpdate.GetCollection()) != nil && runner.createOrUpdate.OnlyCreate {
			// check if onlyCreate is set and if yes then return an error if collection already exist
			return nil, api.Errorf(codes.AlreadyExists, "collection already exists")
		}

		schFactory, err := schema.Build(runner.createOrUpdate.GetCollection(), runner.createOrUpdate.GetSchema())
		if err != nil {
			return nil, err
		}

		if err = tenant.CreateCollection(ctx, tx, db, schFactory); err != nil {
			return nil, err
		}

		return &Response{}, nil
	} else if runner.list != nil {
		db, err := runner.GetDatabase(ctx, tx, tenant, runner.list.GetDb())
		if err != nil {
			return nil, err
		}

		collectionList := db.ListCollection()
		var collections = make([]*api.CollectionInfo, len(collectionList))
		for i, c := range collectionList {
			collections[i] = &api.CollectionInfo{
				Name: c.GetName(),
			}
		}
		return &Response{
			Response: &api.ListCollectionsResponse{
				Collections: collections,
			},
		}, nil
	}

	return &Response{}, api.Errorf(codes.Unknown, "unknown request path")
}

type DatabaseQueryRunner struct {
	*BaseQueryRunner

	drop   *api.DropDatabaseRequest
	create *api.CreateDatabaseRequest
	list   *api.ListDatabasesRequest
}

func (runner *DatabaseQueryRunner) SetCreateDatabaseReq(create *api.CreateDatabaseRequest) {
	runner.create = create
}

func (runner *DatabaseQueryRunner) SetDropDatabaseReq(drop *api.DropDatabaseRequest) {
	runner.drop = drop
}

func (runner *DatabaseQueryRunner) SetListDatabaseReq(list *api.ListDatabasesRequest) {
	runner.list = list
}

func (runner *DatabaseQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, error) {
	if runner.drop != nil {
		db, err := tenant.GetDatabase(ctx, tx, runner.drop.GetDb())
		if err != nil {
			return nil, err
		}
		if db == nil {
			return nil, api.Errorf(codes.InvalidArgument, "database doesn't exists '%s'", runner.drop.GetDb())
		}
		if err := tenant.DropDatabase(ctx, tx, runner.drop.GetDb()); err != nil {
			return nil, err
		}

		return &Response{}, nil
	} else if runner.create != nil {
		db, err := tenant.GetDatabase(ctx, tx, runner.create.GetDb())
		if err != nil {
			return nil, err
		}
		if db != nil {
			return nil, api.Errorf(codes.AlreadyExists, "database already exists")
		}

		if err := tenant.CreateDatabase(ctx, tx, runner.create.GetDb()); err != nil {
			return nil, err
		}
		return &Response{}, nil
	} else if runner.list != nil {
		databaseList := tenant.ListDatabases(ctx, tx)

		var databases = make([]*api.DatabaseInfo, len(databaseList))
		for i, l := range databaseList {
			databases[i] = &api.DatabaseInfo{
				Name: l,
			}
		}
		return &Response{
			Response: &api.ListDatabasesResponse{
				Databases: databases,
			},
		}, nil
	}

	return &Response{}, api.Errorf(codes.Unknown, "unknown request path")
}
