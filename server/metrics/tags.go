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

package metrics

import (
	"errors"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

func getOkTagKeys() []string {
	// Tag keys should be pre-defined to be able to emit the metrics in prometheus format, the values do not
	return []string{
		"db",
		"collection",
		"env",
		"grpc_method",
		"grpc_service",
		"grpc_service_type",
		"tigris_tenant",
	}
}

func mergeTags(tagSets ...map[string]string) map[string]string {
	res := make(map[string]string)
	for _, tagSet := range tagSets {
		for k, v := range tagSet {
			if _, ok := res[k]; !ok {
				res[k] = v
			} else {
				if res[k] == "unknown" {
					res[k] = v
				}
			}
		}
	}
	return res
}

func getErrorTags(err error) map[string]string {
	var tigrisErr *api.TigrisError
	var fdbErr fdb.Error
	if errors.As(err, &fdbErr) {
		return map[string]string{
			"error_source": "fdb",
			"error_value":  strconv.Itoa(fdbErr.Code),
		}
	}
	if errors.As(err, &tigrisErr) {
		return map[string]string{
			"error_source": "tigris_server",
			"error_value":  tigrisErr.Code.String(),
		}
	}
	// TODO: handle search errors
	return map[string]string{}
}

func getDbTags(dbName string) map[string]string {
	return map[string]string{
		"db": dbName,
	}
}

func getDbCollTags(dbName string, collName string) map[string]string {
	return map[string]string{
		"db":         dbName,
		"collection": collName,
	}
}

func GetDbCollTagsForReq(req interface{}) map[string]string {
	if rc, ok := req.(api.RequestWithDbAndCollection); ok {
		return getDbCollTags(rc.GetDb(), rc.GetCollection())
	}
	if r, ok := req.(api.RequestWithDb); ok {
		return getDbTags(r.GetDb())
	}
	return map[string]string{}
}
