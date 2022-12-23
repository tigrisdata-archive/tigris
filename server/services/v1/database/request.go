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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
)

const (
	InsertedStatus string = "inserted"
	ReplacedStatus string = "replaced"
	UpdatedStatus  string = "updated"
	DeletedStatus  string = "deleted"
	CreatedStatus  string = "created"
	DroppedStatus  string = "dropped"
)

// Streaming is a wrapper interface for passing around for streaming reads.
type Streaming interface {
	api.Tigris_ReadServer
}

type SearchStreaming interface {
	api.Tigris_SearchServer
}

// ReqOptions are options used by queryLifecycle to execute a query.
type ReqOptions struct {
	TxCtx              *api.TransactionCtx
	MetadataChange     bool
	InstantVerTracking bool
}

// Response is a wrapper on api.Response.
type Response struct {
	api.Response
	Status        string
	CreatedAt     *internal.Timestamp
	UpdatedAt     *internal.Timestamp
	DeletedAt     *internal.Timestamp
	ModifiedCount int32
	AllKeys       [][]byte
}
