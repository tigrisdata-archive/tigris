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
	"context"

	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
)

func countDDLCreateUnit(ctx context.Context) {
	if config.DefaultConfig.GlobalStatus.Enabled {
		reqStatus, ok := metrics.RequestStatusFromContext(ctx)
		if ok && reqStatus != nil {
			reqStatus.AddDDLCreateUnit()
			reqStatus.SetWriteBytes(int64(0))
			reqStatus.SetReadBytes(int64(0))
		}
	}
}

func countDDLDropUnit(ctx context.Context) {
	if config.DefaultConfig.GlobalStatus.Enabled {
		reqStatus, ok := metrics.RequestStatusFromContext(ctx)
		if ok && reqStatus != nil {
			reqStatus.AddDDLDropUnit()
			reqStatus.SetWriteBytes(int64(0))
			reqStatus.SetReadBytes(int64(0))
		}
	}
}

func countDDLUpdateUnit(ctx context.Context, cond bool) {
	if config.DefaultConfig.GlobalStatus.Enabled {
		reqStatus, ok := metrics.RequestStatusFromContext(ctx)
		if cond && ok && reqStatus != nil {
			reqStatus.AddDDLUpdateUnit()
			reqStatus.SetWriteBytes(int64(0))
			reqStatus.SetReadBytes(int64(0))
		}
	}
}
