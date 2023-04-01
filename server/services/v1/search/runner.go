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
	"fmt"
	"math"
	"strings"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/database"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
)

type Runner interface {
	Run(ctx context.Context, tenant *metadata.Tenant) (Response, error)
}

type TxRunner interface {
	Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, error)
}

type RunnerFactory struct {
	store   search.Store
	encoder metadata.Encoder
	txMgr   *transaction.Manager
}

// NewRunnerFactory returns RunnerFactory object.
func NewRunnerFactory(store search.Store, encoder metadata.Encoder, txMgr *transaction.Manager) *RunnerFactory {
	return &RunnerFactory{
		store:   store,
		encoder: encoder,
		txMgr:   txMgr,
	}
}

func (f *RunnerFactory) GetIndexRunner(accessToken *types.AccessToken) *IndexRunner {
	return &IndexRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, f.txMgr, accessToken),
	}
}

func (f *RunnerFactory) GetReadRunner(r *api.GetDocumentRequest, accessToken *types.AccessToken) *ReadRunner {
	return &ReadRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, f.txMgr, accessToken),
		req:        r,
	}
}

func (f *RunnerFactory) GetSearchRunner(r *api.SearchIndexRequest, streaming Streaming, accessToken *types.AccessToken) *SearchRunner {
	return &SearchRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, f.txMgr, accessToken),
		req:        r,
		streaming:  streaming,
	}
}

func (f *RunnerFactory) GetCreateRunner(accessToken *types.AccessToken) *CreateRunner {
	return &CreateRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, f.txMgr, accessToken),
	}
}

func (f *RunnerFactory) GetCreateOrReplaceRunner(r *api.CreateOrReplaceDocumentRequest, accessToken *types.AccessToken) *CreateOrReplaceRunner {
	return &CreateOrReplaceRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, f.txMgr, accessToken),
		req:        r,
	}
}

func (f *RunnerFactory) GetUpdateQueryRunner(r *api.UpdateDocumentRequest, accessToken *types.AccessToken) *UpdateRunner {
	return &UpdateRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, f.txMgr, accessToken),
		req:        r,
	}
}

func (f *RunnerFactory) GetDeleteQueryRunner(accessToken *types.AccessToken) *DeleteRunner {
	return &DeleteRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, f.txMgr, accessToken),
	}
}

type ReadRunner struct {
	*baseRunner

	req *api.GetDocumentRequest
}

func (runner *ReadRunner) Run(ctx context.Context, tenant *metadata.Tenant) (Response, error) {
	index, err := runner.getIndex(tenant, runner.req.Project, runner.req.Index)
	if err != nil {
		return Response{}, err
	}
	if request.ReadSearchDataFromStorage(ctx) {
		return runner.fromStorage(ctx, index)
	}

	result, err := runner.store.GetDocuments(ctx, index.StoreIndexName(), runner.req.Ids)
	if err != nil {
		return Response{}, err
	}

	idToHits := make(map[string]*map[string]any)
	for _, hit := range *result.Hits {
		// at this point we can safely rely on accessing "schema.SearchId" because we always inject it as top level key.
		idToHits[(*hit.Document)[schema.SearchId].(string)] = hit.Document
	}

	transformer := newReadTransformer(index)
	documents := make([]*api.SearchHit, len(runner.req.Ids))
	for i, id := range runner.req.Ids {
		// Order of returning the id should be same in the order it is asked in the request.
		outDoc, found := idToHits[id]
		if !found {
			documents[i] = nil
			continue
		}

		doc, created, updated, err := transformer.fromSearch(*outDoc)
		if err != nil {
			return Response{}, err
		}

		enc, err := util.MapToJSON(doc)
		if err != nil {
			return Response{}, err
		}

		meta := &api.SearchHitMeta{}
		if created != nil {
			meta.CreatedAt = created.GetProtoTS()
		}
		if updated != nil {
			meta.UpdatedAt = updated.GetProtoTS()
		}

		documents[i] = &api.SearchHit{
			Data:     enc,
			Metadata: meta,
		}
	}

	return Response{
		Response: &api.GetDocumentResponse{
			Documents: documents,
		},
	}, nil
}

func (runner *ReadRunner) fromStorage(ctx context.Context, index *schema.SearchIndex) (Response, error) {
	tx, err := runner.txMgr.StartTx(ctx)
	if err != nil {
		return Response{}, err
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	transformer := newReadTransformer(index)
	documents := make([]*api.SearchHit, len(runner.req.Ids))
	for i, id := range runner.req.Ids {
		var key keys.Key
		if key, err = runner.encoder.EncodeFDBSearchKey(index.StoreIndexName(), id); err != nil {
			return Response{}, err
		}

		keyValue, found, err := runner.readRow(ctx, tx, key)
		if err != nil {
			return Response{}, err
		}
		if !found {
			documents[i] = nil
			continue
		}

		// ToDo: Allow consuming "raw" data as-is as well so that we can just that to backfill indexes.
		decode, err := util.JSONToMap(keyValue.Data.RawData)
		if err != nil {
			return Response{}, err
		}

		reversed, _, _, err := transformer.fromSearch(decode)
		if err != nil {
			return Response{}, err
		}

		raw, err := util.MapToJSON(reversed)
		if err != nil {
			return Response{}, err
		}
		meta := &api.SearchHitMeta{}
		if keyValue.Data.CreatedAt != nil {
			meta.CreatedAt = keyValue.Data.CreatedAt.GetProtoTS()
		}
		if keyValue.Data.UpdatedAt != nil {
			meta.UpdatedAt = keyValue.Data.UpdatedAt.GetProtoTS()
		}

		documents[i] = &api.SearchHit{
			Data:     raw,
			Metadata: meta,
		}
	}

	return Response{
		Response: &api.GetDocumentResponse{
			Documents: documents,
		},
	}, nil
}

type CreateRunner struct {
	*baseRunner

	req     *api.CreateDocumentRequest
	reqById *api.CreateByIdRequest
}

func (runner *CreateRunner) SetCreateDocumentsReq(req *api.CreateDocumentRequest) {
	runner.req = req
}

func (runner *CreateRunner) SetCreateByIdReq(req *api.CreateByIdRequest) {
	runner.reqById = req
}

func (runner *CreateRunner) Run(ctx context.Context, tenant *metadata.Tenant) (Response, error) {
	if runner.reqById != nil {
		return runner.createDocumentById(ctx, tenant, runner.reqById)
	}

	return runner.createDocuments(ctx, tenant, runner.req)
}

func (runner *CreateRunner) createDocumentById(ctx context.Context, tenant *metadata.Tenant, req *api.CreateByIdRequest) (Response, error) {
	if len(req.Id) == 0 {
		return Response{}, errors.InvalidArgument("'id' is required when creating by id")
	}

	index, err := runner.getIndex(tenant, req.GetProject(), req.GetIndex())
	if err != nil {
		return Response{}, err
	}

	decDoc, err := util.JSONToMap(req.Document)
	if err != nil {
		return Response{}, err
	}

	transformer := newWriteTransformer(index, internal.NewTimestamp(), false)
	id, err := transformer.getOrGenerateId(req.Document, decDoc)
	if err != nil {
		return Response{}, err
	}
	if id != req.Id {
		return Response{}, errors.InvalidArgument("id passed in request '%s', is not matching id '%s' in body", req.Id, id)
	}
	if decDoc, err = transformer.toSearch(id, decDoc); err != nil {
		return Response{}, err
	}

	if err := runner.execInStorage(ctx, index.StoreIndexName(), []string{id}, func(_ int, tx transaction.Tx, key keys.Key) error {
		serialized, err := util.MapToJSON(decDoc)
		if err != nil {
			return err
		}

		return tx.Insert(ctx, key, internal.NewTableData(serialized))
	}); err != nil && err != kv.ErrDuplicateKey {
		return Response{}, createApiError(err)
	}

	if err = runner.store.CreateDocument(ctx, index.StoreIndexName(), decDoc); err != nil {
		return Response{}, createApiError(err)
	}

	return Response{
		Response: &api.CreateByIdResponse{
			Id: id,
		},
	}, nil
}

func (runner *CreateRunner) createDocuments(ctx context.Context, tenant *metadata.Tenant, req *api.CreateDocumentRequest) (Response, error) {
	index, err := runner.getIndex(tenant, req.GetProject(), req.GetIndex())
	if err != nil {
		return Response{}, err
	}

	var buffer bytes.Buffer
	ids, serialized, batchErrors, validDocs := runner.encodeDocuments(index, req.Documents, &buffer, false)

	var storeResponses []search.IndexResp
	if validDocs > 0 {
		if storeResponses, err = runner.store.IndexDocuments(ctx, index.StoreIndexName(), &buffer, search.IndexDocumentsOptions{
			Action:    search.Create,
			BatchSize: validDocs,
		}); err != nil {
			return Response{}, createApiError(err)
		}
	}

	resp := &api.CreateDocumentResponse{
		Status: runner.buildDocStatusResp(ids, batchErrors, storeResponses),
	}

	if err := runner.execInStorage(ctx, index.StoreIndexName(), ids, func(index int, tx transaction.Tx, key keys.Key) error {
		if resp.Status[index].Error == nil {
			return tx.Insert(ctx, key, internal.NewTableData(serialized[index]))
		}
		return nil
	}); err != nil && err != kv.ErrDuplicateKey {
		return Response{}, createApiError(err)
	}

	return Response{
		Response: resp,
	}, nil
}

type CreateOrReplaceRunner struct {
	*baseRunner

	req *api.CreateOrReplaceDocumentRequest
}

func (runner *CreateOrReplaceRunner) Run(ctx context.Context, tenant *metadata.Tenant) (Response, error) {
	index, err := runner.getIndex(tenant, runner.req.GetProject(), runner.req.GetIndex())
	if err != nil {
		return Response{}, err
	}

	var buffer bytes.Buffer
	ids, serialized, batchErrors, validDocs := runner.encodeDocuments(index, runner.req.Documents, &buffer, false)

	var storeResponses []search.IndexResp

	if validDocs > 0 {
		if storeResponses, err = runner.store.IndexDocuments(ctx, index.StoreIndexName(), &buffer, search.IndexDocumentsOptions{
			Action:    search.Replace,
			BatchSize: validDocs,
		}); err != nil {
			return Response{}, createApiError(err)
		}
	}

	resp := &api.CreateOrReplaceDocumentResponse{
		Status: runner.buildDocStatusResp(ids, batchErrors, storeResponses),
	}

	if err := runner.execInStorage(ctx, index.StoreIndexName(), ids, func(index int, tx transaction.Tx, key keys.Key) error {
		if resp.Status[index].Error == nil {
			return tx.Replace(ctx, key, internal.NewTableData(serialized[index]), false)
		}
		return nil
	}); err != nil {
		return Response{}, createApiError(err)
	}

	return Response{
		Response: resp,
	}, nil
}

type UpdateRunner struct {
	*baseRunner

	req *api.UpdateDocumentRequest
}

func (runner *UpdateRunner) Run(ctx context.Context, tenant *metadata.Tenant) (Response, error) {
	index, err := runner.getIndex(tenant, runner.req.GetProject(), runner.req.GetIndex())
	if err != nil {
		return Response{}, err
	}

	var buffer bytes.Buffer
	ids, serialized, batchErrors, validDocs := runner.encodeDocuments(index, runner.req.Documents, &buffer, true)

	var storeResponses []search.IndexResp
	if validDocs > 0 {
		if storeResponses, err = runner.store.IndexDocuments(ctx, index.StoreIndexName(), &buffer, search.IndexDocumentsOptions{
			Action:    search.Update,
			BatchSize: validDocs,
		}); err != nil {
			return Response{}, createApiError(err)
		}
	}

	resp := &api.UpdateDocumentResponse{
		Status: runner.buildDocStatusResp(ids, batchErrors, storeResponses),
	}

	if err := runner.execInStorage(ctx, index.StoreIndexName(), ids, func(index int, tx transaction.Tx, key keys.Key) error {
		if resp.Status[index].Error != nil {
			return nil
		}

		value, found, err := runner.readRow(ctx, tx, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}

		// merging with flattened form that should be present in storage.
		newData, err := runner.getMergedData(serialized[index], value.Data.RawData)
		if err != nil {
			return err
		}

		return tx.Replace(ctx, key, internal.NewTableDataWithTS(value.Data.CreatedAt, internal.NewTimestamp(), newData), false)
	}); err != nil {
		// if execution in storage fails then return a full error so that user can retry
		return Response{}, createApiError(err)
	}

	return Response{
		Response: resp,
	}, nil
}

func (*UpdateRunner) getMergedData(input jsoniter.RawMessage, existing jsoniter.RawMessage) (jsoniter.RawMessage, error) {
	var err error
	output := existing
	err = jsonparser.ObjectEach(input, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}
		if dataType == jsonparser.String {
			value = []byte(fmt.Sprintf(`"%s"`, value))
		}

		if dataType == jsonparser.Object {
			output, err = jsonparser.Set(output, value, strings.Split(string(key), ".")...)
		} else {
			output, err = jsonparser.Set(output, value, string(key))
		}
		if err != nil {
			return err
		}
		return nil
	})

	return output, err
}

type DeleteRunner struct {
	*baseRunner

	req        *api.DeleteDocumentRequest
	reqByQuery *api.DeleteByQueryRequest
}

func (runner *DeleteRunner) SetDeleteDocumentReq(req *api.DeleteDocumentRequest) {
	runner.req = req
}

func (runner *DeleteRunner) SetDeleteByQueryReq(req *api.DeleteByQueryRequest) {
	runner.reqByQuery = req
}

func (runner *DeleteRunner) Run(ctx context.Context, tenant *metadata.Tenant) (Response, error) {
	if runner.reqByQuery != nil {
		return runner.deleteDocumentsByQuery(ctx, tenant, runner.reqByQuery)
	}
	return runner.deleteDocumentsById(ctx, tenant, runner.req)
}

func (runner *DeleteRunner) deleteDocumentsById(ctx context.Context, tenant *metadata.Tenant, req *api.DeleteDocumentRequest) (Response, error) {
	index, err := runner.getIndex(tenant, req.GetProject(), req.GetIndex())
	if err != nil {
		return Response{}, err
	}

	resp := &api.DeleteDocumentResponse{}
	for _, id := range req.Ids {
		wr := &api.DocStatus{
			Id: id,
		}

		if err := runner.store.DeleteDocument(ctx, index.StoreIndexName(), id); err != nil {
			wr.Error = &api.Error{
				Message: err.Error(),
			}
		}

		resp.Status = append(resp.Status, wr)
	}

	if err := runner.execInStorage(ctx, index.StoreIndexName(), req.Ids, func(index int, tx transaction.Tx, key keys.Key) error {
		if resp.Status[index].Error == nil {
			return tx.Delete(ctx, key)
		}
		return nil
	}); err != nil {
		return Response{}, err
	}

	return Response{
		Response: resp,
	}, nil
}

func (runner *DeleteRunner) deleteDocumentsByQuery(ctx context.Context, tenant *metadata.Tenant, req *api.DeleteByQueryRequest) (Response, error) {
	index, err := runner.getIndex(tenant, req.GetProject(), req.GetIndex())
	if err != nil {
		return Response{}, err
	}

	factory := filter.NewFactory(index.QueryableFields, nil)
	filters, err := factory.Factorize(req.Filter)
	if err != nil {
		return Response{}, err
	}

	var count int
	if count, err = runner.store.DeleteDocuments(ctx, index.StoreIndexName(), filter.NewWrappedFilter(filters)); err != nil {
		return Response{}, err
	}
	return Response{
		Response: &api.DeleteByQueryResponse{
			Count: int32(count),
		},
	}, nil
}

type SearchRunner struct {
	*baseRunner

	req       *api.SearchIndexRequest
	streaming Streaming
}

func (runner *SearchRunner) Run(ctx context.Context, tenant *metadata.Tenant) (Response, error) {
	reqStatus, reqStatusExists := metrics.RequestStatusFromContext(ctx)
	if reqStatus != nil && reqStatusExists {
		reqStatus.SetApiSearchType()
	}

	index, err := runner.getIndex(tenant, runner.req.GetProject(), runner.req.GetIndex())
	if err != nil {
		return Response{}, err
	}

	wrappedF, err := filter.NewFactory(index.QueryableFields, value.NewCollationFrom(runner.req.Collation)).WrappedFilter(runner.req.Filter)
	if err != nil {
		return Response{}, err
	}

	facets, err := runner.getFacetFields(index)
	if err != nil {
		return Response{}, err
	}

	fieldSelection, err := runner.getFieldSelection(index)
	if err != nil {
		return Response{}, err
	}

	searchFields, err := runner.getSearchFields(index)
	if err != nil {
		return Response{}, err
	}

	sortOrder, err := runner.getSortOrdering(index)
	if err != nil {
		return Response{}, err
	}

	groupBy, err := runner.getGroupBy(index)
	if err != nil {
		return Response{}, err
	}

	vecSearch, err := runner.getVectorSearch(index)
	if err != nil {
		return Response{}, err
	}

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
		GroupBy(groupBy).
		VectorSearch(vecSearch).
		Build()
	if searchQ.IsQAndVectorBoth() {
		return Response{}, errors.InvalidArgument("Currently either full text or vector search is supported")
	}

	searchReader := NewReader(ctx, runner.store, index, searchQ)
	var iterator *FilterableSearchIterator
	if runner.req.Page != 0 {
		iterator = searchReader.SinglePageIterator(index, wrappedF, runner.req.Page)
	} else {
		iterator = searchReader.Iterator(index, wrappedF)
	}
	if err != nil {
		return Response{}, err
	}

	pageNo := int32(defaultPageNo)
	if runner.req.Page > 0 {
		pageNo = runner.req.Page
	}

	matchedFields := container.NewHashSet()
	for {
		resp := &api.SearchIndexResponse{}
		var rows ResultRow
		for iterator.Next(&rows) {
			var indexedDocs []*api.SearchHit
			for _, row := range rows.Rows {
				if searchQ.ReadFields != nil {
					// apply field selection
					newValue, err := searchQ.ReadFields.Apply(row.Document)
					if ulog.E(err) {
						return Response{}, err
					}
					row.Document = newValue
				}

				metadata := &api.SearchHitMeta{}
				if row.CreatedAt != nil {
					metadata.CreatedAt = row.CreatedAt.GetProtoTS()
				}
				if row.UpdatedAt != nil {
					metadata.UpdatedAt = row.UpdatedAt.GetProtoTS()
				}
				metadata.Match = row.Match
				if metadata.Match != nil {
					for _, f := range metadata.Match.Fields {
						if f != nil {
							matchedFields.Insert(f.Name)
						}
					}
				}

				indexedDocs = append(indexedDocs, &api.SearchHit{
					Data:     row.Document,
					Metadata: metadata,
				})
			}

			if len(rows.Group) > 0 {
				resp.Group = append(resp.Group, &api.GroupedSearchHits{
					GroupKeys: rows.Group,
					Hits:      indexedDocs,
				})
			} else {
				resp.Hits = append(resp.Hits, indexedDocs...)
			}

			if len(resp.Hits) == pageSize || len(resp.Group) == pageSize {
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
			MatchedFields: matchedFields.ToList(),
		}
		// if no hits, got error, send only error
		// if no hits, no error, at least one response and break
		// if some hits, got an error, send current hits and then error (will be zero hits next time)
		// if some hits, no error, continue to send response
		if len(resp.Hits) == 0 && len(resp.Group) == 0 {
			if iterator.Interrupted() != nil {
				return Response{}, iterator.Interrupted()
			}
			if pageNo > defaultPageNo && pageNo > runner.req.Page {
				break
			}
		}

		if err := runner.streaming.Send(resp); err != nil {
			return Response{}, err
		}

		pageNo++
	}

	return Response{}, nil
}

func (runner *SearchRunner) getSearchFields(index *schema.SearchIndex) ([]string, error) {
	searchFields := runner.req.SearchFields
	if len(searchFields) == 0 {
		// this is to include all searchable fields if not present in the query
		for _, cf := range index.QueryableFields {
			if cf.DataType == schema.StringType && cf.SearchIndexed {
				searchFields = append(searchFields, cf.InMemoryName())
			}
		}
	} else {
		for i, sf := range searchFields {
			cf, err := index.GetQueryableField(sf)
			if err != nil {
				return nil, err
			}
			if !cf.SearchIndexed {
				return nil, errors.InvalidArgument("`%s` is not a searchable field. Only indexed fields can be queried", sf)
			}
			if cf.SearchIndexed && (cf.DataType == schema.Int32Type || cf.DataType == schema.Int64Type || cf.DataType == schema.DoubleType) {
				return nil, errors.InvalidArgument("`%s` is not a searchable field. Only indexed fields can be queried", sf)
			}
			if cf.InMemoryName() != cf.Name() {
				searchFields[i] = cf.InMemoryName()
			}
		}
	}
	return searchFields, nil
}

func (runner *SearchRunner) getFacetFields(index *schema.SearchIndex) (qsearch.Facets, error) {
	facets, err := qsearch.UnmarshalFacet(runner.req.Facet)
	if err != nil {
		return qsearch.Facets{}, err
	}

	for i, ff := range facets.Fields {
		cf, err := index.GetQueryableField(ff.Name)
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

func (runner *SearchRunner) getFieldSelection(index *schema.SearchIndex) (*read.FieldFactory, error) {
	var selectionFields []string

	// Only one of include/exclude. Honor inclusion over exclusion
	//nolint:gocritic
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
		cf, err := index.GetQueryableField(sf)
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

func (runner *SearchRunner) getSortOrdering(index *schema.SearchIndex) (*sort.Ordering, error) {
	ordering, err := sort.UnmarshalSort(runner.req.Sort)
	if err != nil || ordering == nil {
		return nil, err
	}

	for i, sf := range *ordering {
		cf, err := index.GetQueryableField(sf.Name)
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

func (runner *SearchRunner) getGroupBy(index *schema.SearchIndex) (qsearch.GroupBy, error) {
	groupBy, err := qsearch.UnmarshalGroupBy(runner.req.GroupBy)
	if err != nil {
		return groupBy, err
	}

	for i, f := range groupBy.Fields {
		cf, err := index.GetQueryableField(f)
		if err != nil {
			return qsearch.GroupBy{}, err
		}
		if cf.InMemoryName() != cf.Name() {
			groupBy.Fields[i] = cf.InMemoryName()
		}

		if !cf.Faceted {
			return qsearch.GroupBy{}, errors.InvalidArgument("Cannot group by on `%s` field as facet is not enabled", f)
		}
		if cf.DataType != schema.StringType {
			return qsearch.GroupBy{}, errors.InvalidArgument("Group by is only allowed on string field")
		}
	}
	return groupBy, nil
}

func (runner *SearchRunner) getVectorSearch(index *schema.SearchIndex) (qsearch.VectorSearch, error) {
	vectorSearch, err := qsearch.UnmarshalVectorSearch(runner.req.Vector)
	if err != nil {
		return vectorSearch, err
	}
	if len(vectorSearch.VectorF) == 0 {
		return vectorSearch, nil
	}

	f, err := index.GetQueryableField(vectorSearch.VectorF)
	if err != nil {
		return qsearch.VectorSearch{}, err
	}
	if f.DataType != schema.VectorType {
		return qsearch.VectorSearch{}, errors.InvalidArgument("Cannot perform vector search on non-vector type, field `%s` is not a vector", f.FieldName)
	}
	if f.Dimensions != nil && *f.Dimensions != len(vectorSearch.VectorV) {
		return qsearch.VectorSearch{}, errors.InvalidArgument("query vector is not same size as dimensions, expected size: %d", *f.Dimensions)
	}

	return vectorSearch, nil
}

type IndexRunner struct {
	*baseRunner

	create *api.CreateOrUpdateIndexRequest
	get    *api.GetIndexRequest
	delete *api.DeleteIndexRequest
	list   *api.ListIndexesRequest
}

func (runner *IndexRunner) SetCreateIndexReq(create *api.CreateOrUpdateIndexRequest) {
	runner.create = create
}

func (runner *IndexRunner) SetGetIndexReq(get *api.GetIndexRequest) {
	runner.get = get
}

func (runner *IndexRunner) SetDeleteIndexReq(drop *api.DeleteIndexRequest) {
	runner.delete = drop
}

func (runner *IndexRunner) SetListIndexesReq(list *api.ListIndexesRequest) {
	runner.list = list
}

func (runner *IndexRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (Response, error) {
	currentSub, err := request.GetCurrentSub(ctx)
	if err != nil && config.DefaultConfig.Auth.Enabled {
		return Response{}, errors.Internal("Failed to get current sub for the request")
	}

	switch {
	case runner.create != nil:
		fb := schema.NewFactoryBuilder(true)
		factory, err := fb.BuildSearch(runner.create.GetName(), runner.create.GetSchema())
		if err != nil {
			return Response{}, err
		}

		project, err := tenant.GetProject(runner.create.GetProject())
		if err != nil {
			return Response{}, createApiError(err)
		}
		factory.Sub = currentSub
		if err = tenant.CreateSearchIndex(ctx, tx, project, factory); err != nil {
			return Response{}, createApiError(err)
		}

		return Response{
			Status: database.CreatedStatus,
		}, nil
	case runner.get != nil:
		project, err := tenant.GetProject(runner.get.GetProject())
		if err != nil {
			return Response{}, createApiError(err)
		}

		index, err := tenant.GetSearchIndex(ctx, tx, project, runner.get.Name)
		if err != nil {
			return Response{}, createApiError(err)
		}

		return Response{
			Response: &api.GetIndexResponse{
				Index: &api.IndexInfo{
					Name:   index.Name,
					Schema: index.Schema,
				},
			},
		}, nil
	case runner.delete != nil:
		project, err := tenant.GetProject(runner.delete.GetProject())
		if err != nil {
			return Response{}, createApiError(err)
		}
		if err = tenant.DeleteSearchIndex(ctx, tx, project, runner.delete.GetName()); err != nil {
			return Response{}, createApiError(err)
		}

		return Response{
			Status: database.DeletedStatus,
		}, nil
	case runner.list != nil:
		project, err := tenant.GetProject(runner.list.GetProject())
		if err != nil {
			return Response{}, createApiError(err)
		}

		var indexes []*schema.SearchIndex
		if indexes, err = tenant.ListSearchIndexes(ctx, tx, project); err != nil {
			return Response{}, createApiError(err)
		}

		var indexesResp []*api.IndexInfo
		for _, index := range indexes {
			if runner.list.Filter != nil {
				if string(index.Source.Type) != runner.list.Filter.Type {
					continue
				}

				if len(runner.list.Filter.Collection) > 0 && runner.list.Filter.Collection != index.Source.CollectionName {
					continue
				}

				if len(index.Source.DatabaseBranch) == 0 {
					if len(runner.list.Filter.Branch) > 0 && runner.list.Filter.Branch != metadata.MainBranch {
						continue
					}
				} else if len(runner.list.Filter.Branch) > 0 && runner.list.Filter.Branch != index.Source.DatabaseBranch {
					continue
				}
			}

			indexesResp = append(indexesResp, &api.IndexInfo{
				Name:   index.Name,
				Schema: index.Schema,
			})
		}

		return Response{
			Response: &api.ListIndexesResponse{
				Indexes: indexesResp,
			},
		}, nil
	}

	return Response{}, nil
}
