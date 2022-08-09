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
	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"google.golang.org/grpc"
)

type ContentType string

const (
	JSON ContentType = "application/json"
)

type Service interface {
	RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error
	RegisterGRPC(grpc *grpc.Server) error
}

func GetRegisteredServices(kvStore kv.KeyValueStore, searchStore search.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) []Service {
	var v1Services []Service

	v1Services = append(v1Services, newApiService(kvStore, searchStore, tenantMgr, txMgr))
	v1Services = append(v1Services, newHealthService())
	v1Services = append(v1Services, newAdminService(tenantMgr, txMgr))
	v1Services = append(v1Services, newAuthService())
	v1Services = append(v1Services, newObservabilityService())
	return v1Services
}
