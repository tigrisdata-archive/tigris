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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/defaults"
	"github.com/tigrisdata/tigris/util"
	"google.golang.org/grpc"
)

func mergeTags(tagSets ...map[string]string) map[string]string {
	res := make(map[string]string)
	for _, tagSet := range tagSets {
		for k, v := range tagSet {
			if _, ok := res[k]; !ok {
				res[k] = v
			} else if res[k] == defaults.UnknownValue {
				res[k] = v
			}
		}
	}
	return res
}

func getFdbError(err error) (string, bool) {
	var fdbErr fdb.Error
	if errors.As(err, &fdbErr) {
		return strconv.Itoa(fdbErr.Code), true
	}
	return "", false
}

func getTigrisError(err error) (string, bool) {
	var tigrisErr *api.TigrisError
	if errors.As(err, &tigrisErr) {
		return tigrisErr.Code.String(), true
	}
	return "", false
}

func getTagsForError(err error) map[string]string {
	value, isFdbError := getFdbError(err)
	if isFdbError {
		return map[string]string{
			"error_source": "fdb",
			"error_value":  value,
		}
	}

	value, isTigrisError := getTigrisError(err)
	if isTigrisError {
		return map[string]string{
			"error_source": "tigris_server",
			"error_value":  value,
		}
	}

	// Only log specific errors in case it's known that there will be only a few possible error values
	return map[string]string{
		"error_source": defaults.UnknownValue,
		"error_value":  defaults.UnknownValue,
	}
}

func GetProjectCollTags(project string, collection string) map[string]string {
	if project != "" && collection != "" {
		return map[string]string{
			"project":    project,
			"db":         project,
			"collection": collection,
		}
	}
	if project != "" {
		return map[string]string{
			"project": project,
			"db":      project,
		}
	}
	return map[string]string{}
}

func getDefaultValue(tagKey string) string {
	switch tagKey {
	case "env":
		return config.GetEnvironment()
	case "service":
		return util.Service
	case "version":
		return getVersion()
	default:
		return defaults.UnknownValue
	}
}

func filterTags(tags map[string]string, filterList []string) map[string]string {
	if len(filterList) == 0 {
		return tags
	}
	res := make(map[string]string)
	for tagKey, tagValue := range tags {
		filtered := false
		for _, filteredTag := range filterList {
			if tagKey == filteredTag {
				filtered = true
			}
		}
		if !filtered {
			res[tagKey] = tagValue
		}
	}
	return res
}

func standardizeTags(tags map[string]string, stdKeys []string) map[string]string {
	res := tags
	for _, tagKey := range stdKeys {
		if _, ok := tags[tagKey]; !ok {
			// tag is missing, need to add it
			res[tagKey] = getDefaultValue(tagKey)
		} else if res[tagKey] == "" {
			res[tagKey] = getDefaultValue(tagKey)
		}
	}
	for k := range res {
		extraTag := true
		// result has an extra tag that should not be there
		for _, stdKey := range stdKeys {
			if stdKey == k {
				extraTag = false
			}
		}
		if extraTag {
			delete(res, k)
		}
	}
	return res
}

func getGrpcTagsFromContext(ctx context.Context) map[string]string {
	fullMethodName, fullMethodNameFound := grpc.Method(ctx)
	if fullMethodNameFound {
		return map[string]string{
			"grpc_method": strings.Split(fullMethodName, "/")[2],
		}
	} else {
		return map[string]string{
			"grpc_method": defaults.UnknownValue,
		}
	}
}

func GetTenantNameTagValue(namespace string, namespaceName string) string {
	// The namespace is usually uuid format and the namespaceName is human readable, but not guanrateed to be unique
	return fmt.Sprintf("%v_%v", namespaceName, namespace)
}
