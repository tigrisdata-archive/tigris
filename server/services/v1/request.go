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
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/schema"
)

// Streaming is a wrapper interface for passing around for streaming reads
type Streaming interface {
	api.TigrisDB_ReadServer
}

// Request is a wrapper on api.Request.
type Request struct {
	api.Request

	collection  schema.Collection
	documents   [][]byte
	queryRunner QueryRunner
}

// Response is a wrapper on api.Response
type Response struct {
	api.Response
}
