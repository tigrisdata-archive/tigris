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

package metrics

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
)

var ignoredFieldsForWrites = [][]byte{
	[]byte("_tigris_created_at"),
	[]byte("_tigris_updated_at"),
}

type GlobalStatus struct {
	mu          sync.Mutex
	activeChunk *TenantStatusTimeChunk
}

type TenantStatusTimeChunk struct {
	startTime time.Time
	// Only filled when Flush is called in the returned copy
	endTime time.Time
	tenants map[string]*TenantStatus
}

type TenantStatus struct {
	// TODO: add support for collection level
	// Counted from readBytes at the end of the request
	readUnits int64
	// Counted from writeBytes at the end of the request
	writeUnits     int64
	readBytes      int64
	writeBytes     int64
	ddlDropUnits   int64
	ddlCreateUnits int64
	ddlUpdateUnits int64
	// TODO: separate ddl units for branches
}

type RequestStatus struct {
	namespace string
	// A read request will report the read bytes after the successful completion of the request. This is data read
	// from storage, not the data returned to the client. readBytes is calculated while the data is streamed to
	// the client if the request is streaming (reads). For example, a full table scan is a single request, but it
	readBytes int64
	// For write requests, both readBytes and writeBytes are calculated at the end of a successful request. Write
	// bytes are the sum of document and index bytes written, and the read bytes in this case would be the data
	// update or delete request where there may be a scan depending on the query filter.
	writeBytes int64
	// Some operations have a fixed cost, and it should be calculated at the iterator level
	ddlDropUnits   int64
	ddlCreateUnits int64
	ddlUpdateUnits int64
}

type RequestStatusCtxKey struct{}

func NewRequestStatus(tenantName string) *RequestStatus {
	return &RequestStatus{namespace: tenantName, readBytes: 0, writeBytes: 0}
}

func NewGlobalStatus() *GlobalStatus {
	startTime := time.Now()
	tenantMap := make(map[string]*TenantStatus)
	g := &GlobalStatus{
		activeChunk: &TenantStatusTimeChunk{
			startTime: startTime,
			tenants:   tenantMap,
		},
	}
	return g
}

func NewTenantStatusTimeChunk(startTime time.Time) *TenantStatusTimeChunk {
	tenantMap := make(map[string]*TenantStatus)
	return &TenantStatusTimeChunk{tenants: tenantMap, startTime: startTime}
}

func NewTenantStatus() *TenantStatus {
	return &TenantStatus{}
}

func RequestStatusFromContext(ctx context.Context) (*RequestStatus, bool) {
	r, ok := ctx.Value(RequestStatusCtxKey{}).(*RequestStatus)
	return r, ok
}

func (r *RequestStatus) SaveRequestStatusToContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, RequestStatusCtxKey{}, r)
	return ctx
}

func (r *RequestStatus) ClearRequestStatusFromContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, RequestStatusCtxKey{}, nil)
}

func (r *RequestStatus) AddReadBytes(value int64) {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.readBytes += value
	log.Debug().Int64("Bytes read", value).Int64("Total bytes read", r.readBytes).Msg("Added read bytes")
}

func (r *RequestStatus) AddWriteBytes(value int64) {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.writeBytes += value
	log.Debug().Int64("Bytes written", value).Int64("Total bytes written", r.writeBytes).Msg("Added written bytes")
}

func (r *RequestStatus) AddDDLDropUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.ddlDropUnits += 1
	log.Debug().Msg("Added drop unit")
}

func (r *RequestStatus) AddDDLCreateUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.ddlCreateUnits += 1
	log.Debug().Msg("Add ddl create unit")
}

func (r *RequestStatus) AddDDLUpdateUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.ddlUpdateUnits += 1
	log.Debug().Msg("Add ddl update unit")
}

func (r *RequestStatus) IsKeySecondaryIndex(fdbKey []byte) bool {
	if bytes.Contains(fdbKey, []byte("skey")) {
		log.Debug().Bytes("fdbKey", fdbKey).Msg("Is secondary index")
		return true
	}
	return false
}

func (r *RequestStatus) IsSecondaryIndexFieldIgnored(fdbKey []byte) bool {
	for _, ignoredField := range ignoredFieldsForWrites {
		if bytes.Contains(fdbKey, ignoredField) {
			log.Debug().Bytes("fdbKey", fdbKey).Msg("Ignoring secondary index field")
			return true
		}
	}
	return false
}

func (r *RequestStatus) SetWriteBytes(value int64) {
	r.writeBytes = value
}

func (r *RequestStatus) SetReadBytes(value int64) {
	r.readBytes = value
}

func (r *RequestStatus) GetReadBytes() int64 {
	return r.readBytes
}

func (r *RequestStatus) GetWriteBytes() int64 {
	return r.writeBytes
}

func (r *RequestStatus) GetDDLDropUnits() int64 {
	return r.ddlDropUnits
}

func (r *RequestStatus) GetDDLUpdateUnits() int64 {
	return r.ddlUpdateUnits
}

func (r *RequestStatus) GetDDLCreateUnits() int64 {
	return r.ddlCreateUnits
}

func (g *GlobalStatus) ensureTenantForActiveChunk(tenantName string) {
	_, ok := g.activeChunk.tenants[tenantName]
	if !ok {
		g.activeChunk.tenants[tenantName] = NewTenantStatus()
	}
}

func (g *GlobalStatus) RecordRequestToActiveChunk(r *RequestStatus, tenantName string) {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	log.Debug().Msg("Recording request to active chunk")
	g.mu.Lock()
	writeUnits := getUnitsFromBytes(r.writeBytes, config.WriteUnitSize)
	readUnits := getUnitsFromBytes(r.readBytes, config.ReadUnitSize)
	g.ensureTenantForActiveChunk(tenantName)
	log.Debug().Int64("writeBytes", r.writeBytes).Str("tenantName", tenantName).Msg("Recording write bytes")
	g.activeChunk.tenants[tenantName].writeBytes += r.writeBytes
	log.Debug().Int64("readBytes", r.readBytes).Str("tenantName", tenantName).Msg("Recording read bytes")
	g.activeChunk.tenants[tenantName].readBytes += r.readBytes
	log.Debug().Int64("writeUnits", writeUnits).Str("tenantName", tenantName).Msg("Recording write units")
	g.activeChunk.tenants[tenantName].writeUnits += writeUnits
	log.Debug().Int64("readUnits", readUnits).Str("tenantName", tenantName).Msg("Recording read units")
	g.activeChunk.tenants[tenantName].readUnits += readUnits
	g.activeChunk.tenants[tenantName].ddlDropUnits += r.ddlDropUnits
	g.activeChunk.tenants[tenantName].ddlCreateUnits += r.ddlCreateUnits
	g.activeChunk.tenants[tenantName].ddlUpdateUnits += r.ddlUpdateUnits
	g.mu.Unlock()
}

func (g *GlobalStatus) Flush() TenantStatusTimeChunk {
	g.mu.Lock()
	startTime := time.Now()
	g.activeChunk.endTime = startTime
	res := *g.activeChunk
	g.activeChunk = NewTenantStatusTimeChunk(startTime)
	g.mu.Unlock()
	log.Debug().Int("number of tenants", len(res.tenants)).Msg("Flushing global status")
	for tenantName, status := range res.tenants {
		status.writeUnits = getUnitsFromBytes(status.writeBytes, config.WriteUnitSize)
		status.readUnits = getUnitsFromBytes(status.readBytes, config.ReadUnitSize)
		log.Debug().Int64("read units", status.readUnits).Int64("read bytes", status.readBytes).Int64("write units", status.writeUnits).Int64("write bytes", status.writeBytes).Int64("ddl drop units", status.ddlDropUnits).Int64("ddl update units", status.ddlUpdateUnits).Int64("ddl create units", status.ddlCreateUnits).Str("tenant name", tenantName).Msg("Flushed global status for tenant")
	}

	return res
}
