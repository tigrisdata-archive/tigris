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

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/cdc"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
)

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

// getDatabase is a helper method to return database either from the transactional context for explicit transactions or
// from the tenant object. Returns a user facing error if the database is not present.
func (runner *BaseQueryRunner) getDatabase(_ context.Context, tx transaction.Tx, tenant *metadata.Tenant, projName string, branch string) (*metadata.Database, error) {
	dbBranch := metadata.NewDatabaseNameWithBranch(projName, branch)

	if tx != nil && tx.Context().GetStagedDatabase() != nil {
		// this means that some DDL operation has modified the database object, then we need to perform all the operations
		// on this staged database.

		db, ok := tx.Context().GetStagedDatabase().(*metadata.Database)
		if !ok {
			return nil, errors.Internal("invalid transaction staged database")
		}

		if db.Name() != dbBranch.Name() {
			return nil, errors.InvalidArgument("collections should belong to the same database branch in the transaction")
		}

		return db, nil
	}

	project, err := tenant.GetProject(projName)
	if err != nil {
		return nil, createApiError(err)
	}

	// otherwise, simply read from the in-memory cache/disk.
	db, err := project.GetDatabase(dbBranch)
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

func (runner *BaseQueryRunner) getDBAndCollection(ctx context.Context, tx transaction.Tx,
	tenant *metadata.Tenant, dbName string, collName string, branch string,
) (*metadata.Database, *schema.DefaultCollection, error) {
	db, err := runner.getDatabase(ctx, tx, tenant, dbName, branch)
	if err != nil {
		return nil, nil, err
	}

	collection, err := runner.getCollection(db, collName)
	if err != nil {
		return nil, nil, err
	}

	return db, collection, nil
}

func (runner *BaseQueryRunner) insertOrReplace(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant,
	coll *schema.DefaultCollection, documents [][]byte, insert bool,
) (*internal.Timestamp, [][]byte, error) {
	var err error
	ts := internal.NewTimestamp()
	allKeys := make([][]byte, 0, len(documents))
	secondaryIndexer := NewSecondaryIndexer(coll)
	for _, doc := range documents {
		// reset it back to doc
		doc, err = runner.mutateAndValidatePayload(coll, newInsertPayloadMutator(coll, ts.ToRFC3339()), doc)
		if err != nil {
			return nil, nil, err
		}

		keyGen := newKeyGenerator(doc, tenant.TableKeyGenerator, coll.Indexes.PrimaryKey)
		key, err := keyGen.generate(ctx, runner.txMgr, runner.encoder, coll.EncodedName)
		if err != nil {
			return nil, nil, err
		}

		// we need to use keyGen updated document as it may be mutated by adding auto-generated keys.
		tableData := internal.NewTableDataWithTS(ts, nil, keyGen.document)
		tableData.SetVersion(coll.GetVersion())

		if config.DefaultConfig.SecondaryIndex.WriteEnabled {
			err := secondaryIndexer.Index(ctx, tx, tableData, key.IndexParts())
			if err != nil {
				return nil, nil, err
			}
		}

		if insert || keyGen.forceInsert {
			// we use Insert API, in case user is using autogenerated primary key and has primary key field
			// as Int64 or timestamp to ensure uniqueness if multiple workers end up generating same timestamp.
			err = tx.Insert(ctx, key, tableData)
		} else {
			if config.DefaultConfig.SecondaryIndex.WriteEnabled {
				err := secondaryIndexer.ReadDocAndDelete(ctx, tx, key)
				if err != nil {
					return nil, nil, err
				}
			}
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
	deserializedDoc, err := util.JSONToMap(doc)
	if ulog.E(err) {
		return doc, err
	}

	// this will mutate map, so we need to serialize this map again
	if err = mutator.stringToInt64(deserializedDoc); err != nil {
		return doc, err
	}

	if err = mutator.setDefaultsInIncomingPayload(deserializedDoc); err != nil {
		return doc, err
	}

	if err = coll.Validate(deserializedDoc); err != nil {
		// schema validation failed
		return doc, err
	}

	if mutator.isMutated() {
		return util.MapToJSON(deserializedDoc)
	}

	return doc, nil
}

func (runner *BaseQueryRunner) buildKeysUsingFilter(coll *schema.DefaultCollection,
	reqFilter []byte, collation *value.Collation,
) ([]keys.Key, error) {
	filterFactory := filter.NewFactory(coll.QueryableFields, collation)
	filters, err := filterFactory.Factorize(reqFilter)
	if err != nil {
		return nil, err
	}

	primaryKeyIndex := coll.Indexes.PrimaryKey
	kb := filter.NewKeyBuilder(filter.NewStrictEqKeyComposer(func(indexParts ...interface{}) (keys.Key, error) {
		return runner.encoder.EncodeKey(coll.EncodedName, primaryKeyIndex, indexParts)
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

func (runner *BaseQueryRunner) getWriteIterator(ctx context.Context, tx transaction.Tx,
	collection *schema.DefaultCollection, reqFilter []byte, collation *value.Collation,
	metrics *metrics.WriteQueryMetrics,
) (Iterator, error) {
	var (
		err      error
		iKeys    []keys.Key
		iterator Iterator
	)

	reader := NewDatabaseReader(ctx, tx)

	if iKeys, err = runner.buildKeysUsingFilter(collection, reqFilter, collation); err == nil {
		iterator, err = reader.KeyIterator(iKeys)
	} else {
		if iterator, err = reader.ScanTable(collection.EncodedName); err != nil {
			return nil, err
		}
		filterFactory := filter.NewFactory(collection.QueryableFields, collation)
		var filters []filter.Filter
		if filters, err = filterFactory.Factorize(reqFilter); err != nil {
			return nil, err
		}

		iterator, err = reader.FilteredRead(iterator, filter.NewWrappedFilter(filters))
	}
	if err != nil {
		return nil, err
	}

	if len(iKeys) == 0 {
		metrics.SetWriteType("pkey")
	} else {
		metrics.SetWriteType("non-pkey")
	}

	return iterator, nil
}
