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
	"fmt"
	"net/http"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/cdc"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc"
)

const (
	databasePath        = "/databases"
	databasePathPattern = databasePath + "/*"

	collectionPath        = databasePath + "/collections"
	collectionPathPattern = collectionPath + "/*"

	documentPath        = collectionPath + "/documents"
	documentPathPattern = documentPath + "/*"

	infoPath    = "/info"
	metricsPath = "/metrics"
)

type apiService struct {
	api.UnimplementedTigrisServer

	kvStore       kv.KeyValueStore
	txMgr         *transaction.Manager
	tenantMgr     *metadata.TenantManager
	cdcMgr        *cdc.Manager
	sessions      Session
	runnerFactory *QueryRunnerFactory
	versionH      *metadata.VersionHandler
	searchStore   search.Store
}

func newApiService(kv kv.KeyValueStore, searchStore search.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) *apiService {
	u := &apiService{
		kvStore:     kv,
		txMgr:       txMgr,
		versionH:    &metadata.VersionHandler{},
		searchStore: searchStore,
		cdcMgr:      cdc.NewManager(),
		tenantMgr:   tenantMgr,
	}

	ctx := context.TODO()
	tx, err := u.txMgr.StartTx(ctx)
	if ulog.E(err) {
		log.Fatal().Err(err).Msgf("error starting server: starting transaction failed")
	}

	if err := tenantMgr.Reload(ctx, tx); ulog.E(err) {
		// ToDo: no need to panic, probably handle through async thread.
		log.Fatal().Err(err).Msgf("error starting server: reloading tenants failed")
	}
	ulog.E(tx.Commit(ctx))

	var txListeners []TxListener
	if config.DefaultConfig.Cdc.Enabled {
		txListeners = append(txListeners, u.cdcMgr)
	}
	if config.DefaultConfig.Search.WriteEnabled {
		// just for testing so that we can disable it if needed
		txListeners = append(txListeners, NewSearchIndexer(searchStore, tenantMgr))
	}

	if config.DefaultConfig.Tracing.Enabled {
		u.sessions = NewSessionManagerWithMetrics(u.txMgr, u.tenantMgr, u.versionH, txListeners, metadata.NewCacheTracker(tenantMgr, txMgr))
	} else {
		u.sessions = NewSessionManager(u.txMgr, u.tenantMgr, u.versionH, txListeners, metadata.NewCacheTracker(tenantMgr, txMgr))
	}
	u.runnerFactory = NewQueryRunnerFactory(u.txMgr, u.cdcMgr, u.searchStore)

	return u
}

func (s *apiService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
		runtime.WithIncomingHeaderMatcher(api.CustomMatcher),
		runtime.WithOutgoingHeaderMatcher(api.CustomMatcher),
	)

	if err := api.RegisterTigrisHandlerClient(context.TODO(), mux, api.NewTigrisClient(inproc)); err != nil {
		return err
	}

	api.RegisterTigrisServer(inproc, s)

	router.HandleFunc(apiPathPrefix+databasePathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+collectionPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+documentPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+infoPath, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.Handle(metricsPath, metrics.Reporter.HTTPHandler())
	return nil
}

func (s *apiService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterTigrisServer(grpc, s)
	return nil
}

func (s *apiService) BeginTransaction(ctx context.Context, _ *api.BeginTransactionRequest) (*api.BeginTransactionResponse, error) {
	// explicit transactions needed to be tracked
	session, err := s.sessions.Create(ctx, true, true, true)
	if err != nil {
		return nil, err
	}

	return &api.BeginTransactionResponse{
		TxCtx: session.txCtx,
	}, nil
}

func (s *apiService) CommitTransaction(ctx context.Context, _ *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
	session, _ := s.sessions.Get(ctx)
	if session == nil {
		return nil, api.Errorf(api.Code_NOT_FOUND, "session not found")
	}
	defer func() {
		if err := s.sessions.Remove(ctx); err != nil {
			ulog.E(err)
		}
	}()

	err := session.Commit(s.versionH, session.tx.Context().GetStagedDatabase() != nil, nil)
	if err != nil {
		return nil, err
	}

	return &api.CommitTransactionResponse{}, nil
}

func (s *apiService) RollbackTransaction(ctx context.Context, _ *api.RollbackTransactionRequest) (*api.RollbackTransactionResponse, error) {
	session, _ := s.sessions.Get(ctx)
	if session == nil {
		return nil, api.Errorf(api.Code_NOT_FOUND, "session not found")
	}
	defer func() {
		if err := s.sessions.Remove(ctx); err != nil {
			ulog.E(err)
		}
	}()

	_ = session.Rollback()

	return &api.RollbackTransactionResponse{}, nil
}

// Insert new object returns an error if object already exists
// Operations done individually not in actual batch
func (s *apiService) Insert(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
	qm := metrics.WriteQueryMetrics{}
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetInsertQueryRunner(r, &qm), &ReqOptions{
		txCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.InsertResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
		Keys: resp.allKeys,
	}, nil
}

func (s *apiService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	qm := metrics.WriteQueryMetrics{}
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetReplaceQueryRunner(r, &qm), &ReqOptions{
		txCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.ReplaceResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
		Keys: resp.allKeys,
	}, nil
}

func (s *apiService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	queryMetrics := metrics.WriteQueryMetrics{}
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetUpdateQueryRunner(r, &queryMetrics), &ReqOptions{
		txCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.UpdateResponse{
		Status:        resp.status,
		ModifiedCount: resp.modifiedCount,
		Metadata: &api.ResponseMetadata{
			UpdatedAt: resp.updatedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Delete(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
	queryMetrics := metrics.WriteQueryMetrics{}
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetDeleteQueryRunner(r, &queryMetrics), &ReqOptions{
		txCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.DeleteResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			DeletedAt: resp.deletedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Read(r *api.ReadRequest, stream api.Tigris_ReadServer) error {
	var err error
	queryMetrics := metrics.StreamingQueryMetrics{}
	if api.GetTransaction(stream.Context()) != nil {
		_, err = s.sessions.Execute(stream.Context(), s.runnerFactory.GetStreamingQueryRunner(r, stream, &queryMetrics), &ReqOptions{
			txCtx:              api.GetTransaction(stream.Context()),
			instantVerTracking: true,
		})
	} else {
		_, err = s.sessions.ReadOnlyExecute(stream.Context(), s.runnerFactory.GetStreamingQueryRunner(r, stream, &queryMetrics), &ReqOptions{})
	}
	return err
}

func (s *apiService) Search(r *api.SearchRequest, stream api.Tigris_SearchServer) error {
	queryMetrics := metrics.SearchQueryMetrics{}
	_, err := s.sessions.ReadOnlyExecute(stream.Context(), s.runnerFactory.GetSearchQueryRunner(r, stream, &queryMetrics), &ReqOptions{
		instantVerTracking: true,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *apiService) CreateOrUpdateCollection(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) (*api.CreateOrUpdateCollectionResponse, error) {
	collectionType, err := schema.GetCollectionType(r.Schema)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "not able to extract collection type from the schema")
	}

	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetCreateOrUpdateCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &ReqOptions{
		txCtx:              api.GetTransaction(ctx),
		metadataChange:     true,
		instantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateOrUpdateCollectionResponse{
		Status:  resp.status,
		Message: fmt.Sprintf("collection of type '%s' created successfully", collectionType),
	}, nil
}

func (s *apiService) DropCollection(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetDropCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &ReqOptions{
		txCtx:              api.GetTransaction(ctx),
		metadataChange:     true,
		instantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.DropCollectionResponse{
		Status:  resp.status,
		Message: "collection dropped successfully",
	}, nil
}

func (s *apiService) ListCollections(ctx context.Context, r *api.ListCollectionsRequest) (*api.ListCollectionsResponse, error) {
	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetListCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &ReqOptions{
		txCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListCollectionsResponse), nil
}

func (s *apiService) ListDatabases(ctx context.Context, r *api.ListDatabasesRequest) (*api.ListDatabasesResponse, error) {
	runner := s.runnerFactory.GetDatabaseQueryRunner()
	runner.SetListDatabaseReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &ReqOptions{})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListDatabasesResponse), nil
}

func (s *apiService) CreateDatabase(ctx context.Context, r *api.CreateDatabaseRequest) (*api.CreateDatabaseResponse, error) {
	runner := s.runnerFactory.GetDatabaseQueryRunner()
	runner.SetCreateDatabaseReq(r)
	resp, err := s.sessions.Execute(ctx, runner, &ReqOptions{
		metadataChange:     true,
		instantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateDatabaseResponse{
		Status:  resp.status,
		Message: "database created successfully",
	}, nil
}

func (s *apiService) DropDatabase(ctx context.Context, r *api.DropDatabaseRequest) (*api.DropDatabaseResponse, error) {
	runner := s.runnerFactory.GetDatabaseQueryRunner()
	runner.SetDropDatabaseReq(r)
	resp, err := s.sessions.Execute(ctx, runner, &ReqOptions{
		metadataChange:     true,
		instantVerTracking: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.DropDatabaseResponse{
		Status:  resp.status,
		Message: "database dropped successfully",
	}, nil
}

func (s *apiService) DescribeCollection(ctx context.Context, r *api.DescribeCollectionRequest) (*api.DescribeCollectionResponse, error) {
	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetDescribeCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &ReqOptions{})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeCollectionResponse), nil
}

func (s *apiService) DescribeDatabase(ctx context.Context, r *api.DescribeDatabaseRequest) (*api.DescribeDatabaseResponse, error) {
	runner := s.runnerFactory.GetDatabaseQueryRunner()
	runner.SetDescribeDatabaseReq(r)

	resp, err := s.sessions.Execute(ctx, runner, &ReqOptions{})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeDatabaseResponse), nil
}

func (s *apiService) GetInfo(_ context.Context, _ *api.GetInfoRequest) (*api.GetInfoResponse, error) {
	return &api.GetInfoResponse{
		ServerVersion: util.Version,
	}, nil
}

func (s *apiService) Events(r *api.EventsRequest, stream api.Tigris_EventsServer) error {
	if !config.DefaultConfig.Cdc.Enabled {
		return api.Errorf(api.Code_METHOD_NOT_ALLOWED, "change streams is disabled for this collection")
	}

	if len(r.Collection) == 0 {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "collection name is missing")
	}

	publisher := s.cdcMgr.GetPublisher(r.GetDb())
	streamer, err := publisher.NewStreamer(s.kvStore)
	if err != nil {
		return err
	}
	defer streamer.Close()

	reqDatabaseId, reqCollectionId := uint32(0), uint32(0)
	for tx := range streamer.Txs {
		for _, op := range tx.Ops {
			if reqDatabaseId == 0 || reqCollectionId == 0 {
				if reqDatabaseId, reqCollectionId = s.tenantMgr.GetDatabaseAndCollectionId(r.GetDb(), r.Collection); reqDatabaseId == 0 || reqCollectionId == 0 {
					// neither is ready yet
					continue
				}
			}

			_, dbId, cId, ok := s.tenantMgr.GetEncoder().DecodeTableName(op.Table)
			if !ok {
				log.Err(err).Str("table", string(op.Table)).Msg("unexpected key in event streams")
				return api.Errorf(api.Code_INTERNAL, "unexpected key in event streams")
			}

			if dbId != reqDatabaseId || cId != reqCollectionId {
				//  the event is no for the collection we are listening to
				continue
			}

			var data []byte
			if op.Op != kv.DeleteEvent && op.Op != kv.DeleteRangeEvent {
				td, err := internal.Decode(op.Data)
				if err != nil {
					log.Err(err).Str("data", string(op.Data)).Msg("failed to decode data")
					return api.Errorf(api.Code_INTERNAL, "failed to decode data")
				}
				data = td.RawData
			}

			event := &api.StreamEvent{
				TxId:       tx.Id,
				Collection: r.Collection,
				Op:         op.Op,
				Key:        op.Key,
				Lkey:       op.LKey,
				Rkey:       op.RKey,
				Data:       data,
				Last:       op.Last,
			}

			response := &api.EventsResponse{
				Event: event,
			}

			if err := stream.Send(response); ulog.E(err) {
				return err
			}
		}
	}

	return nil
}

func (s *apiService) Publish(ctx context.Context, r *api.PublishRequest) (*api.PublishResponse, error) {
	resp, err := s.sessions.Execute(ctx, s.runnerFactory.GetPublishQueryRunner(r), &ReqOptions{
		txCtx: api.GetTransaction(ctx),
	})
	if err != nil {
		return nil, err
	}

	return &api.PublishResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
		Keys: resp.allKeys,
	}, nil
}

func (s *apiService) Subscribe(r *api.SubscribeRequest, stream api.Tigris_SubscribeServer) error {
	_, err := s.sessions.Execute(stream.Context(), s.runnerFactory.GetSubscribeQueryRunner(r, stream), &ReqOptions{
		txCtx: api.GetTransaction(stream.Context()),
	})
	if err != nil {
		return err
	}

	return nil
}
