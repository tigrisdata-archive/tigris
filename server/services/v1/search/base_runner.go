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

package search

import (
	"bytes"
	"context"
	"github.com/tigrisdata/tigris/server/metrics"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type baseRunner struct {
	store       search.Store
	encoder     metadata.Encoder
	accessToken *types.AccessToken
	txMgr       *transaction.Manager
}

func newBaseRunner(store search.Store, encoder metadata.Encoder, txMgr *transaction.Manager, accessToken *types.AccessToken) *baseRunner {
	return &baseRunner{
		store:       store,
		encoder:     encoder,
		accessToken: accessToken,
		txMgr:       txMgr,
	}
}

func (*baseRunner) getIndex(tenant *metadata.Tenant, projName string, indexName string) (*schema.SearchIndex, error) {
	project, err := tenant.GetProject(projName)
	if err != nil {
		return nil, err
	}

	index, found := project.GetSearch().GetIndex(indexName)
	if !found {
		// this allows to trigger version check to reload if index already exists
		return nil, metadata.NewSearchIndexNotFoundErr(indexName)
	}

	return index, nil
}

func (*baseRunner) encodeDocuments(ctx context.Context, index *schema.SearchIndex, documents [][]byte, buffer *bytes.Buffer, isUpdate bool) ([]string, [][]byte, []error, int) {
	var (
		validDocs      = 0
		ts             = internal.NewTimestamp()
		transformer    = newWriteTransformer(index, ts, isUpdate)
		docIds         = make([]string, len(documents))
		docErrors      = make([]error, len(documents))
		serializedDocs = make([][]byte, len(documents))
	)

	for i, raw := range documents {
		metrics.AddSearchBytesInContext(ctx, int64(len(raw)))
		doc, err := util.JSONToMap(raw)
		if err != nil {
			docErrors[i] = err
			continue
		}

		// get or generate id
		if docIds[i], docErrors[i] = transformer.getOrGenerateId(raw, doc); docErrors[i] != nil {
			continue
		}

		// perform any validation on this document
		if docErrors[i] = index.Validate(doc); docErrors[i] != nil {
			continue
		}

		// transform to search
		if doc, docErrors[i] = transformer.toSearch(docIds[i], doc); docErrors[i] != nil {
			continue
		}

		// serialize it
		if serializedDocs[i], docErrors[i] = util.MapToJSON(doc); docErrors[i] != nil {
			continue
		}

		_, _ = buffer.Write(serializedDocs[i])
		_ = buffer.WriteByte('\n')
		validDocs++
	}

	return docIds, serializedDocs, docErrors, validDocs
}

func (runner *baseRunner) execInStorage(ctx context.Context, tableName string, ids []string, fn func(index int, tx transaction.Tx, key keys.Key) error) error {
	if !config.DefaultConfig.Search.StorageEnabled {
		return nil
	}

	tx, err := runner.txMgr.StartTx(ctx)
	if err != nil {
		return err
	}

	for i, id := range ids {
		if len(id) == 0 {
			// this is only possible if user payload has issues
			continue
		}
		var key keys.Key
		if key, err = runner.encoder.EncodeFDBSearchKey(tableName, id); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}

		if err = fn(i, tx, key); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}

	return tx.Commit(ctx)
}

func (*baseRunner) readRow(ctx context.Context, tx transaction.Tx, key keys.Key) (*kv.KeyValue, bool, error) {
	it, err := tx.Read(ctx, key, false)
	if ulog.E(err) {
		return nil, false, err
	}

	// Do not count for metadata operations
	metrics.SetMetadataOperationInContext(ctx)

	var value kv.KeyValue
	if it.Next(&value) {
		return &value, true, nil
	}

	return nil, false, it.Err()
}

// buildDocStatusResp is responsible for building doc status response that is returned by batch index APIs. This API
// expects a batch of "ids", a batch of "errors" and then the "storeResp". All three have the same order as the input
// documents. The batchErrors is set before sending requests to store which means first we check if that is set if yes
// then we fill the response with that error otherwise that document was successfully sent to store so we need to check
// whether store is processed it without any error.
func (*baseRunner) buildDocStatusResp(ids []string, batchErrors []error, storeResp []search.IndexResp) []*api.DocStatus {
	var (
		respIdx int
		status  = make([]*api.DocStatus, len(ids))
	)
	for i, id := range ids {
		var apiErr *api.Error
		if batchErrors[i] != nil {
			apiErr = convertStoreErrToApiErr(id, 400, batchErrors[i].Error())
		} else {
			// otherwise we need to extract from the store response
			apiErr = convertStoreErrToApiErr(id, storeResp[respIdx].Code, storeResp[respIdx].Error)
			respIdx++
		}

		status[i] = &api.DocStatus{
			Id:    id,
			Error: apiErr,
		}
	}

	return status
}
