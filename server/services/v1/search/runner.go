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

	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/server/services/v1/database"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
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
}

// NewRunnerFactory returns RunnerFactory object.
func NewRunnerFactory(store search.Store, encoder metadata.Encoder) *RunnerFactory {
	return &RunnerFactory{
		store:   store,
		encoder: encoder,
	}
}

func (f *RunnerFactory) GetIndexRunner(accessToken *types.AccessToken) *IndexRunner {
	return &IndexRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, accessToken),
	}
}

func (f *RunnerFactory) GetReadRunner(r *api.GetDocumentRequest, accessToken *types.AccessToken) *ReadRunner {
	return &ReadRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, accessToken),
		req:        r,
	}
}

func (f *RunnerFactory) GetCreateRunner(accessToken *types.AccessToken) *CreateRunner {
	return &CreateRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, accessToken),
	}
}

func (f *RunnerFactory) GetCreateOrReplaceRunner(r *api.CreateOrReplaceDocumentRequest, accessToken *types.AccessToken) *CreateOrReplaceRunner {
	return &CreateOrReplaceRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, accessToken),
		req:        r,
	}
}

func (f *RunnerFactory) GetUpdateQueryRunner(r *api.UpdateDocumentRequest, accessToken *types.AccessToken) *UpdateRunner {
	return &UpdateRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, accessToken),

		req: r,
	}
}

func (f *RunnerFactory) GetDeleteQueryRunner(accessToken *types.AccessToken) *DeleteRunner {
	return &DeleteRunner{
		baseRunner: newBaseRunner(f.store, f.encoder, accessToken),
	}
}

type baseRunner struct {
	store       search.Store
	encoder     metadata.Encoder
	accessToken *types.AccessToken
}

func newBaseRunner(store search.Store, encoder metadata.Encoder, accessToken *types.AccessToken) *baseRunner {
	return &baseRunner{
		store:       store,
		encoder:     encoder,
		accessToken: accessToken,
	}
}

func (runner *baseRunner) getIndex(tenant *metadata.Tenant, projName string, indexName string) (*schema.SearchIndex, error) {
	project, err := tenant.GetProject(projName)
	if err != nil {
		return nil, err
	}

	index, found := project.GetSearch().GetIndex(indexName)
	if !found {
		return nil, errors.NotFound("index '%s' is missing", indexName)
	}

	return index, nil
}

func (runner *baseRunner) encodeDocuments(index *schema.SearchIndex, documents [][]byte, buffer *bytes.Buffer, addIdIfMissing bool) ([]string, error) {
	ids := make([]string, len(documents))
	encoder := jsoniter.NewEncoder(buffer)
	for i, doc := range documents {
		decDoc, err := util.JSONToMap(doc)
		if err != nil {
			return nil, err
		}

		_, found := decDoc[schema.SearchId]
		if !found {
			if !addIdIfMissing {
				return nil, errors.InvalidArgument("doc missing 'id' field")
			}

			decDoc[schema.SearchId] = uuid.New().String()
		}
		ids[i] = decDoc[schema.SearchId].(string)

		packed, err := MutateSearchDocument(index, decDoc)
		if err != nil {
			return nil, err
		}

		if err := encoder.Encode(packed); err != nil {
			return nil, err
		}
		buffer.WriteByte('\n')
	}

	return ids, nil
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

	result, err := runner.store.GetDocuments(ctx, index.StoreIndexName(), runner.req.Ids)
	if err != nil {
		return Response{}, err
	}

	idToHits := make(map[string]*map[string]interface{})
	for _, hit := range *result.Hits {
		idToHits[(*hit.Document)[schema.SearchId].(string)] = hit.Document
	}

	documents := make([][]byte, len(runner.req.Ids))
	for i, id := range runner.req.Ids {
		// Order of returning the id should be same in the order it is asked in the request.
		outDoc, found := idToHits[id]
		if !found {
			documents[i] = nil
			continue
		}

		doc, err := UnpackSearchFields(index, *outDoc)
		if err != nil {
			return Response{}, err
		}

		enc, err := util.MapToJSON(doc)
		if err != nil {
			return Response{}, err
		}
		documents[i] = enc
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

// Run ...
// ToDo: Test batch documents failure on duplicates.
func (runner *CreateRunner) Run(ctx context.Context, tenant *metadata.Tenant) (Response, error) {
	if runner.reqById != nil {
		return runner.createDocumentById(ctx, tenant, runner.reqById)
	}

	return runner.createDocuments(ctx, tenant, runner.req)
}

func (runner *CreateRunner) createDocumentById(ctx context.Context, tenant *metadata.Tenant, req *api.CreateByIdRequest) (Response, error) {
	index, err := runner.getIndex(tenant, req.GetProject(), req.GetIndex())
	if err != nil {
		return Response{}, err
	}

	decDoc, err := util.JSONToMap(req.Document)
	if err != nil {
		return Response{}, err
	}

	id, found := decDoc[schema.SearchId].(string)
	if !found {
		id = req.Id
		decDoc[schema.SearchId] = req.Id
	}
	if id != req.Id {
		return Response{}, errors.InvalidArgument("id passed in request '%s', is not matching id '%s' in body", req.Id, id)
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

	var (
		ids    []string
		buffer bytes.Buffer
	)
	if ids, err = runner.encodeDocuments(index, req.Documents, &buffer, true); err != nil {
		return Response{}, err
	}

	var storeResponses []search.IndexResp
	if storeResponses, err = runner.store.IndexDocuments(ctx, index.StoreIndexName(), &buffer, search.IndexDocumentsOptions{
		Action:    search.Create,
		BatchSize: len(req.Documents),
	}); err != nil {
		return Response{}, err
	}

	resp := &api.CreateDocumentResponse{}
	for i, id := range ids {
		resp.Status = append(resp.Status, &api.DocStatus{
			Id:    id,
			Error: convertStoreErrToApiErr(id, storeResponses[i].Code, storeResponses[i].Error),
		})
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

	var (
		ids    []string
		buffer bytes.Buffer
	)

	if ids, err = runner.encodeDocuments(index, runner.req.Documents, &buffer, true); err != nil {
		return Response{}, err
	}

	var storeResponses []search.IndexResp
	if storeResponses, err = runner.store.IndexDocuments(ctx, index.StoreIndexName(), &buffer, search.IndexDocumentsOptions{
		Action:    search.Replace,
		BatchSize: len(runner.req.Documents),
	}); err != nil {
		return Response{}, err
	}

	resp := &api.CreateOrReplaceDocumentResponse{}
	for i, id := range ids {
		resp.Status = append(resp.Status, &api.DocStatus{
			Id:    id,
			Error: convertStoreErrToApiErr(id, storeResponses[i].Code, storeResponses[i].Error),
		})
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

	var (
		ids    []string
		buffer bytes.Buffer
	)
	if ids, err = runner.encodeDocuments(index, runner.req.Documents, &buffer, false); err != nil {
		return Response{}, err
	}

	var storeResponses []search.IndexResp
	if storeResponses, err = runner.store.IndexDocuments(ctx, index.StoreIndexName(), &buffer, search.IndexDocumentsOptions{
		Action:    search.Update,
		BatchSize: len(runner.req.Documents),
	}); err != nil {
		return Response{}, err
	}

	resp := &api.UpdateDocumentResponse{}
	for i, id := range ids {
		resp.Status = append(resp.Status, &api.DocStatus{
			Id:    id,
			Error: convertStoreErrToApiErr(id, storeResponses[i].Code, storeResponses[i].Error),
		})
	}

	return Response{
		Response: resp,
	}, nil
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
	} else {
		return runner.deleteDocumentsById(ctx, tenant, runner.req)
	}
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

type IndexRunner struct {
	*baseRunner

	create *api.CreateOrUpdateIndexRequest
	delete *api.DeleteIndexRequest
	list   *api.ListIndexesRequest
}

func (runner *IndexRunner) SetCreateIndexReq(create *api.CreateOrUpdateIndexRequest) {
	runner.create = create
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
		factory, err := schema.BuildSearch(runner.create.GetName(), runner.create.GetSchema())
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

		type UserSchema struct {
			Name        string              `json:"title,omitempty"`
			Description string              `json:"description,omitempty"`
			Properties  jsoniter.RawMessage `json:"properties,omitempty"`
		}

		type IndexSource struct {
			Source schema.SearchSource `json:"source,omitempty"`
		}

		indexesResp := make([]*api.IndexInfo, len(indexes))
		for i, index := range indexes {
			indexesResp[i] = &api.IndexInfo{
				Name: index.Name,
			}

			var us UserSchema
			if err := jsoniter.Unmarshal(index.Schema, &us); err != nil {
				return Response{}, err
			}

			if indexesResp[i].Schema, err = jsoniter.Marshal(us); err != nil {
				return Response{}, err
			}

			var source IndexSource
			if err = jsoniter.Unmarshal(index.Schema, &source); err != nil {
				return Response{}, err
			}

			indexesResp[i].Source = &api.IndexSource{
				Type:       string(source.Source.Type),
				Collection: source.Source.CollectionName,
				Branch:     source.Source.DatabaseBranch,
			}
		}

		return Response{
			Response: &api.ListIndexesResponse{
				Indexes: indexesResp,
			},
		}, nil
	}

	return Response{}, nil
}
