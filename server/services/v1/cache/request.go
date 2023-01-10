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

package cache

import api "github.com/tigrisdata/tigris/api/server/v1"

const (
	SetStatus     string = "set"
	DeletedStatus string = "deleted"
	CreatedStatus string = "created"
)

// Response is a wrapper on api.Response.
type Response struct {
	api.Response
	Status       string
	Data         []byte
	OldValue     []byte
	Keys         []string
	DeletedCount int64
	Caches       []*api.CacheMetadata
}
