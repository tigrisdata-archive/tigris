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

	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/encoding"
	"github.com/tigrisdata/tigrisdb/keys"
	"github.com/tigrisdata/tigrisdb/query/filter"
	"github.com/tigrisdata/tigrisdb/query/read"
	"github.com/tigrisdata/tigrisdb/query/update"
	"github.com/tigrisdata/tigrisdb/schema"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

// QueryRunner is responsible for executing the current query and return the response
type QueryRunner interface {
	Run(ctx context.Context, tx transaction.Tx) (*Response, error)
}

// QueryRunnerFactory is responsible for creating query runners for different queries
type QueryRunnerFactory struct {
	kv          kv.KV
	txMgr       *transaction.Manager
	encoder     encoding.Encoder
	schemaCache *schema.Cache
}

// NewQueryRunnerFactory returns QueryRunnerFactory object
func NewQueryRunnerFactory(kv kv.KV, txMgr *transaction.Manager, encoder encoding.Encoder, cache *schema.Cache) *QueryRunnerFactory {
	return &QueryRunnerFactory{
		kv:          kv,
		txMgr:       txMgr,
		encoder:     encoder,
		schemaCache: cache,
	}
}

func (f *QueryRunnerFactory) GetInsertQueryRunner(r *api.InsertRequest) *InsertQueryRunner {
	return &InsertQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.schemaCache),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetReplaceQueryRunner(r *api.ReplaceRequest) *ReplaceQueryRunner {
	return &ReplaceQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.schemaCache),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetUpdateQueryRunner(r *api.UpdateRequest) *UpdateQueryRunner {
	return &UpdateQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.schemaCache),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetDeleteQueryRunner(r *api.DeleteRequest) *DeleteQueryRunner {
	return &DeleteQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.schemaCache),
		req:             r,
	}
}

// GetStreamingQueryRunner returns StreamingQueryRunner
func (f *QueryRunnerFactory) GetStreamingQueryRunner(r *api.ReadRequest, streaming Streaming) *StreamingQueryRunner {
	return &StreamingQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.schemaCache),
		req:             r,
		streaming:       streaming,
	}
}

func (f *QueryRunnerFactory) GetCollectionQueryRunner(create *api.CreateCollectionRequest, drop *api.DropCollectionRequest) *CollectionQueryRunner {
	return &CollectionQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.schemaCache),
		kv:              f.kv,
		create:          create,
		drop:            drop,
	}
}

type BaseQueryRunner struct {
	encoder     encoding.Encoder
	schemaCache *schema.Cache
}

func NewBaseQueryRunner(encoder encoding.Encoder, cache *schema.Cache) *BaseQueryRunner {
	return &BaseQueryRunner{
		encoder:     encoder,
		schemaCache: cache,
	}
}

func (runner *BaseQueryRunner) GetCollections(dbName string, collectionName string) (schema.Collection, error) {
	collection, err := runner.schemaCache.Get(dbName, collectionName)
	if err != nil {
		return nil, err
	}
	return collection, nil
}

func (runner *BaseQueryRunner) insertOrReplace(ctx context.Context, tx transaction.Tx, collection schema.Collection, documents [][]byte, insert bool) error {
	var err error
	for _, d := range documents {
		// ToDo: need to implement our own decoding to only extract custom keys
		var s = &structpb.Struct{}
		if err = json.Unmarshal(d, s); err != nil {
			return err
		}

		var key keys.Key
		if key, err = runner.encoder.BuildKey(s.GetFields(), collection); err != nil {
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

func (runner *BaseQueryRunner) buildKeysUsingFilter(collection schema.Collection, reqFilter []byte) ([]keys.Key, error) {
	filters, err := filter.Build(reqFilter)
	if err != nil {
		return nil, err
	}

	kb := filter.NewKeyBuilder(filter.NewStrictEqKeyComposer([]byte(collection.StorageName())))
	iKeys, err := kb.Build(filters, collection.PrimaryKeys())
	if err != nil {
		return nil, err
	}

	return iKeys, nil
}

type InsertQueryRunner struct {
	*BaseQueryRunner

	req *api.InsertRequest
}

func (runner *InsertQueryRunner) Run(ctx context.Context, tx transaction.Tx) (*Response, error) {
	collection, err := runner.GetCollections(runner.req.GetDb(), runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	if err := runner.insertOrReplace(ctx, tx, collection, runner.req.GetDocuments(), true); err != nil {
		return nil, err
	}
	return &Response{}, nil
}

type ReplaceQueryRunner struct {
	*BaseQueryRunner

	req *api.ReplaceRequest
}

func (runner *ReplaceQueryRunner) Run(ctx context.Context, tx transaction.Tx) (*Response, error) {
	collection, err := runner.GetCollections(runner.req.GetDb(), runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	if err := runner.insertOrReplace(ctx, tx, collection, runner.req.GetDocuments(), false); err != nil {
		return nil, err
	}
	return &Response{}, nil
}

type UpdateQueryRunner struct {
	*BaseQueryRunner

	req *api.UpdateRequest
}

func (runner *UpdateQueryRunner) Run(ctx context.Context, tx transaction.Tx) (*Response, error) {
	collection, err := runner.GetCollections(runner.req.GetDb(), runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	iKeys, err := runner.buildKeysUsingFilter(collection, runner.req.Filter)
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

func (runner *DeleteQueryRunner) Run(ctx context.Context, tx transaction.Tx) (*Response, error) {
	collection, err := runner.GetCollections(runner.req.GetDb(), runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	iKeys, err := runner.buildKeysUsingFilter(collection, runner.req.Filter)
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
func (runner *StreamingQueryRunner) Run(ctx context.Context, tx transaction.Tx) (*Response, error) {
	collection, err := runner.GetCollections(runner.req.GetDb(), runner.req.GetCollection())
	if err != nil {
		return nil, err
	}

	fieldFactory, err := read.BuildFields(runner.req.GetFields())
	if ulog.E(err) {
		return nil, err
	}

	if filter.IsFullCollectionScan(runner.req.GetFilter()) {
		return runner.iterateCollection(ctx, tx, collection, fieldFactory)
	}
	return runner.iterateKeys(ctx, tx, collection, fieldFactory)
}

// iterateCollection is used to scan the entire collection.
func (runner *StreamingQueryRunner) iterateCollection(ctx context.Context, tx transaction.Tx, collection schema.Collection, fieldFactory *read.FieldFactory) (*Response, error) {
	var totalResults int64 = 0
	if err := runner.iterate(ctx, tx, keys.NewKey([]byte(collection.StorageName())), fieldFactory, &totalResults); err != nil {
		return nil, err
	}

	return &Response{}, nil
}

// iterateKeys is responsible for building keys from the filter and then executing the query. A key could be a primary
// key or an index key.
func (runner *StreamingQueryRunner) iterateKeys(ctx context.Context, tx transaction.Tx, collection schema.Collection, fieldFactory *read.FieldFactory) (*Response, error) {
	iKeys, err := runner.buildKeysUsingFilter(collection, runner.req.Filter)
	if err != nil {
		return nil, err
	}

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
	if err != nil {
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
		if err != nil {
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

	kv     kv.KV
	drop   *api.DropCollectionRequest
	create *api.CreateCollectionRequest
}

func (runner *CollectionQueryRunner) Run(ctx context.Context, tx transaction.Tx) (*Response, error) {
	if runner.drop != nil {
		if err := runner.kv.DropTable(ctx, []byte(schema.StorageName(runner.drop.GetDb(), runner.drop.GetCollection()))); ulog.E(err) {
			return nil, api.Errorf(codes.Internal, "error: %v", err)
		}

		runner.schemaCache.Remove(runner.drop.GetDb(), runner.drop.GetCollection())
		return &Response{}, nil
	} else if runner.create != nil {
		if c, _ := runner.schemaCache.Get(runner.create.GetDb(), runner.create.GetCollection()); c != nil {
			return nil, api.Errorf(codes.AlreadyExists, "collection already exists")
		}

		collection, err := schema.CreateCollection(runner.create.GetDb(), runner.create.GetCollection(), runner.create.GetSchema())
		if err != nil {
			return nil, err
		}

		runner.schemaCache.Put(collection)
		return &Response{}, nil
	}

	return &Response{}, api.Errorf(codes.Unknown, "unknown path")
}
