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

type UsageProvider interface {
	Flush() TenantStatusTimeChunk
}

type GlobalStatus struct {
	mu          sync.Mutex
	activeChunk *TenantStatusTimeChunk
}

type TenantStatusTimeChunk struct {
	StartTime time.Time
	// Only filled when Flush is called in the returned copy
	EndTime time.Time
	Tenants map[string]*TenantStatus
}

type TenantStatus struct {
	// TODO: add support for collection level
	// Counted from readBytes at the end of the request
	ReadUnits int64
	// Counted from writeBytes at the end of the request
	WriteUnits int64
	// Counted from searchWriteBytes at the end of the request
	SearchWriteUnits int64
	// Bytes read, converted to write units
	readBytes int64
	// Bytes written, converted to write units
	writeBytes int64
	// Bytes written to search, converted to write units
	searchWriteBytes int64
	// One drop operation (for example drop collection) is one drop unit
	ddlDropUnits int64
	// One create operation (for example one create collection) is one create unit
	ddlCreateUnits int64
	// One schema update operation is one update unit
	ddlUpdateUnits int64
	// One search request or getting the next page from search is one search unit
	SearchUnits int64
	// One collection search request or getting the next page from search is one collection search unit
	collectionSearchUnits int64
	// One create search index is one create search index unit
	searchCreateIndexUnits int64
	// One drop search index is one create search index unit
	searchDropIndexUnits int64
	// Deleting one document from a search index is one delete document unit
	searchDeleteDocumentUnits int64
	// TODO: separate ddl units for branches
}

type RequestStatus struct {
	namespace string
	// Fields related to database
	// A read request will report the read bytes after the successful completion of the request. This is data read
	// from storage, not the data returned to the client. readBytes is calculated while the data is streamed to
	// the client if the request is streaming (reads). For example, a full table scan is a single request, but it
	readBytes int64
	// For write requests, both readBytes and writeBytes are calculated at the end of a successful request. Write
	// bytes are the sum of document and index bytes written, and the read bytes in this case would be the data
	// update or delete request where there may be a scan depending on the query filter.
	writeBytes int64
	// For search request, the number of bytes written to search on a successful request.
	searchWriteBytes int64
	// number of documents
	nDocs int64
	// Type of search request 0 - api search, 1 - collection search (used internally)
	searchRequestType int
	// Collection read when the read is falling back to search
	collectionRead bool
	// One drop operation (for example drop collection) is one drop unit
	ddlDropUnits int64
	// One create operation (for example one create collection) is one create unit
	ddlCreateUnits int64
	// One schema update operation is one update unit
	ddlUpdateUnits int64
	// Fields related to search
	// TODO: implement bytes written for search
	// One collection search request or getting the next page from search is one collection search unit
	collectionSearchUnits int64
	// One search request or getting the next page from search is one search unit
	searchUnits int64
	// One create search index is one create search index unit
	searchCreateIndexUnits int64
	// One drop search index is one create search index unit
	searchDropIndexUnits int64
	// Deleting one document from a search index is one delete document unit
	searchDeleteDocumentUnits int64
	// Metadata operation that should not be counted
	metadataOperation bool
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
			StartTime: startTime,
			Tenants:   tenantMap,
		},
	}
	return g
}

func NewTenantStatusTimeChunk(startTime time.Time) *TenantStatusTimeChunk {
	tenantMap := make(map[string]*TenantStatus)
	return &TenantStatusTimeChunk{Tenants: tenantMap, StartTime: startTime}
}

func NewTenantStatus() *TenantStatus {
	return &TenantStatus{}
}

func RequestStatusFromContext(ctx context.Context) (*RequestStatus, bool) {
	r, ok := ctx.Value(RequestStatusCtxKey{}).(*RequestStatus)
	return r, ok
}

func SetMetadataOperationInContext(ctx context.Context) {
	reqStatus, exists := RequestStatusFromContext(ctx)
	if reqStatus != nil && exists {
		reqStatus.metadataOperation = true
	}
}

func AddSearchBytesInContext(ctx context.Context, value int64) {
	reqStatus, exists := RequestStatusFromContext(ctx)
	if reqStatus != nil && exists {
		reqStatus.writeBytes += value
	}
}

func (r *RequestStatus) IsMetadataOperation() bool {
	return r.metadataOperation
}

func (r *RequestStatus) SetCollectionSearchType() {
	r.searchRequestType = 1
}

func (r *RequestStatus) SetCollectionRead() {
	r.collectionRead = true
}

func (r *RequestStatus) IsCollectionRead() bool {
	return r.collectionRead
}

func (r *RequestStatus) SetApiSearchType() {
	r.searchRequestType = 0
}

func (r *RequestStatus) IsCollectionSearch() bool {
	return r.searchRequestType == 1
}

func (r *RequestStatus) IsApiSearch() bool {
	return r.searchRequestType == 0
}

func (r *RequestStatus) AddResultDocs(value int64) {
	r.nDocs += value
}

func (r *RequestStatus) GetResultDocs() int64 {
	return r.nDocs
}

func (r *RequestStatus) GetCollectionSearchUnits() int64 {
	return r.collectionSearchUnits
}

func (r *RequestStatus) GetApiSearchUnits() int64 {
	return r.searchUnits
}

func (r *RequestStatus) SaveRequestStatusToContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, RequestStatusCtxKey{}, r)
	return ctx
}

func (*RequestStatus) ClearRequestStatusFromContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, RequestStatusCtxKey{}, nil)
}

func (r *RequestStatus) AddReadBytes(value int64) {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.readBytes += value
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Int64("Bytes read", value).Int64("Total bytes read", r.readBytes).Msg("Added read bytes")
	}
}

func (r *RequestStatus) AddWriteBytes(value int64) {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.writeBytes += value
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Int64("Bytes written", value).Int64("Total bytes written", r.writeBytes).Msg("Added written bytes")
	}
}

func (r *RequestStatus) AddSearchWriteByes(value int64) {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.searchWriteBytes += value
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Int64("search bytes written", value).Int64("Total search bytes written", r.searchWriteBytes).Msg("Added written search bytes")
	}
}

func (r *RequestStatus) AddDDLDropUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.ddlDropUnits++
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Added drop unit")
	}
}

func (r *RequestStatus) AddSearchCreateIndexUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.searchCreateIndexUnits++
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Added search create index unit")
	}
}

func (r *RequestStatus) GetSearchCreateIndexUnits() int64 {
	return r.searchCreateIndexUnits
}

func (r *RequestStatus) AddSearchDropIndexUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.searchDropIndexUnits++
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Added search drop index unit")
	}
}

func (r *RequestStatus) GetSearchDropIndexUnits() int64 {
	return r.searchDropIndexUnits
}

func (r *RequestStatus) AddSearchDeleteDocumentUnit(count int) {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.searchDeleteDocumentUnits += int64(count)
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Added search delete index unit")
	}
}

func (r *RequestStatus) GetSearchDeleteDocumentUnits() int64 {
	return r.searchDeleteDocumentUnits
}

func (r *RequestStatus) AddSearchUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.searchUnits++
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Added api search unit")
	}
}

func (r *RequestStatus) AddCollectionSearchUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.collectionSearchUnits++
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Added collection search unit")
	}
}

func (r *RequestStatus) AddDDLCreateUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.ddlCreateUnits++
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Add ddl create unit")
	}
}

func (r *RequestStatus) AddDDLUpdateUnit() {
	if !config.DefaultConfig.GlobalStatus.Enabled {
		return
	}
	r.ddlUpdateUnits++
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Add ddl update unit")
	}
}

func (*RequestStatus) IsKeySecondaryIndex(fdbKey []byte) bool {
	if bytes.Contains(fdbKey, []byte("skey")) {
		if config.DefaultConfig.GlobalStatus.DebugMessages {
			log.Debug().Bytes("fdbKey", fdbKey).Msg("Is secondary index")
		}
		return true
	}
	return false
}

func (*RequestStatus) IsSecondaryIndexFieldIgnored(fdbKey []byte) bool {
	for _, ignoredField := range ignoredFieldsForWrites {
		if bytes.Contains(fdbKey, ignoredField) {
			if config.DefaultConfig.GlobalStatus.DebugMessages {
				log.Debug().Bytes("fdbKey", fdbKey).Msg("Ignoring secondary index field")
			}
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
	_, ok := g.activeChunk.Tenants[tenantName]
	if !ok {
		g.activeChunk.Tenants[tenantName] = NewTenantStatus()
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
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Msg("Recording request to active chunk")
		log.Debug().Int64("writeBytes", r.writeBytes).
			Str("tenantName", tenantName).Msg("Recording write bytes")
		log.Debug().Int64("readBytes", r.readBytes).
			Str("tenantName", tenantName).Msg("Recording read bytes")
		log.Debug().Int64("writeUnits", writeUnits).
			Str("tenantName", tenantName).Msg("Recording write units")
		log.Debug().Int64("readUnits", readUnits).
			Str("tenantName", tenantName).Msg("Recording read units")
		log.Debug().Int64("SearchUnits", r.searchUnits).
			Str("tenantName", tenantName).Msg("Recording api search units")
		log.Debug().Int64("collectionSearchUnits", r.collectionSearchUnits).
			Str("tenantName", tenantName).Msg("Recording collection search units")
		log.Debug().Int64("ddlDropUnits", r.ddlDropUnits).
			Str("tenantName", tenantName).Msg("Recording ddl drop units")
		log.Debug().Int64("ddlCreateUnits", r.ddlCreateUnits).
			Str("tenantName", tenantName).Msg("Recording ddl create units")
		log.Debug().Int64("ddlUpdateUnits", r.ddlUpdateUnits).
			Str("tenantName", tenantName).Msg("Recording ddl update units")
		log.Debug().Int64("searchCreateIndexUnits", r.searchCreateIndexUnits).
			Str("tenantName", tenantName).Msg("Recording search create index units")
		log.Debug().Int64("searchDropIndexUnits", r.searchDropIndexUnits).
			Str("tenantName", tenantName).Msg("Recording search drop index units")
		log.Debug().Int64("searchDeleteDocumentUnits", r.searchDeleteDocumentUnits).
			Str("tenantName", tenantName).Msg("Recording search delete documents units")
	}
	g.activeChunk.Tenants[tenantName].writeBytes += r.writeBytes
	g.activeChunk.Tenants[tenantName].readBytes += r.readBytes
	g.activeChunk.Tenants[tenantName].WriteUnits += writeUnits
	g.activeChunk.Tenants[tenantName].ReadUnits += readUnits
	g.activeChunk.Tenants[tenantName].SearchUnits += r.searchUnits
	g.activeChunk.Tenants[tenantName].collectionSearchUnits += r.collectionSearchUnits
	g.activeChunk.Tenants[tenantName].ddlDropUnits += r.ddlDropUnits
	g.activeChunk.Tenants[tenantName].ddlCreateUnits += r.ddlCreateUnits
	g.activeChunk.Tenants[tenantName].ddlUpdateUnits += r.ddlUpdateUnits
	g.activeChunk.Tenants[tenantName].searchCreateIndexUnits += r.searchCreateIndexUnits
	g.activeChunk.Tenants[tenantName].searchDropIndexUnits += r.searchDropIndexUnits
	g.activeChunk.Tenants[tenantName].searchDeleteDocumentUnits += r.searchDeleteDocumentUnits
	g.mu.Unlock()
}

func (g *GlobalStatus) Flush() TenantStatusTimeChunk {
	g.mu.Lock()
	startTime := time.Now()
	g.activeChunk.EndTime = startTime
	res := *g.activeChunk
	g.activeChunk = NewTenantStatusTimeChunk(startTime)
	g.mu.Unlock()
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Int("number of tenants", len(res.Tenants)).Msg("Flushing global status")
	}
	for _, status := range res.Tenants {
		status.WriteUnits = getUnitsFromBytes(status.writeBytes, config.WriteUnitSize)
		status.ReadUnits = getUnitsFromBytes(status.readBytes, config.ReadUnitSize)
		status.SearchWriteUnits = getUnitsFromBytes(status.searchWriteBytes, config.SearchUnitSize)
	}
	if config.DefaultConfig.GlobalStatus.DebugMessages {
		log.Debug().Time("start time", res.StartTime).Time("end time", res.EndTime).Msg("flush results")
	}
	for tenantName, status := range res.Tenants {
		if config.DefaultConfig.GlobalStatus.DebugMessages {
			log.Debug().Int64("read units", status.ReadUnits).
				Int64("write units", status.WriteUnits).Str("tenant name", tenantName).Msg("db units flushed")
			log.Debug().Int64("search units", status.SearchUnits).
				Int64("collection search units", status.collectionSearchUnits).Str("tenant name", tenantName).
				Msg("flushed search units")
			log.Debug().Int64("search write units", status.SearchWriteUnits).Str("tenant name", tenantName).
				Msg("search write units flushed")
		}
	}
	return res
}
