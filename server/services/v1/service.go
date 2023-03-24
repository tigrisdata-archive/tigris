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

package v1

import (
	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/services/v1/auth"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"google.golang.org/grpc"
)

type ContentType string

type Service interface {
	RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error
	RegisterGRPC(grpc *grpc.Server) error
}

func GetRegisteredServicesRealtime(kvStore kv.TxStore, searchStore search.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) []Service {
	var v1Services []Service
	v1Services = append(v1Services, newRealtimeService(kvStore, searchStore, tenantMgr, txMgr))
	v1Services = append(v1Services, newHealthService(txMgr))
	v1Services = append(v1Services, newObservabilityService(tenantMgr))
	return v1Services
}

func GetRegisteredServices(kvStore kv.TxStore, searchStore search.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager, forSearchTxMgr *transaction.Manager) []Service {
	var v1Services []Service
	v1Services = append(v1Services, newHealthService(txMgr))

	userStore := metadata.NewUserStore(metadata.DefaultNameRegistry)

	authProvider := auth.NewProvider(userStore, txMgr)
	v1Services = append(v1Services, newApiService(kvStore, searchStore, tenantMgr, txMgr, authProvider))

	if config.DefaultConfig.Auth.EnableOauth {
		v1Services = append(v1Services, newAuthService(authProvider))
	}
	if config.DefaultConfig.Management.Enabled {
		v1Services = append(v1Services, newManagementService(authProvider, txMgr, tenantMgr, userStore, tenantMgr.GetNamespaceStore()))
	}

	v1Services = append(v1Services, newObservabilityService(tenantMgr))
	v1Services = append(v1Services, newCacheService(tenantMgr, txMgr))
	v1Services = append(v1Services, newSearchService(searchStore, tenantMgr, forSearchTxMgr))

	return v1Services
}
