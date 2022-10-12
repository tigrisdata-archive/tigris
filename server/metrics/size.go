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
	"github.com/uber-go/tally"
)

var (
	NamespaceSize  tally.Scope
	DbSize         tally.Scope
	CollectionSize tally.Scope
)

func initializeSizeScopes() {
	NamespaceSize = SizeMetrics.SubScope("namespace")
	DbSize = SizeMetrics.SubScope("db")
	CollectionSize = SizeMetrics.SubScope("collection")
}

func getNameSpaceSizeTagKeys() []string {
	return []string{
		"env",
		"service",
		"tigris_tenant",
		"version",
	}
}

func getDbSizeTagKeys() []string {
	return []string{
		"env",
		"service",
		"tigris_tenant",
		"version",
		"db",
	}
}

func getCollectionSizeTagKeys() []string {
	return []string{
		"env",
		"service",
		"tigris_tenant",
		"version",
		"db",
		"collection",
	}
}

func getNamespaceSizeTags(namespaceName string) map[string]string {
	return map[string]string{
		"tigris_tenant": namespaceName,
	}
}

func getDbSizeTags(namespaceName string, dbName string) map[string]string {
	return map[string]string{
		"tigris_tenant": namespaceName,
		"db":            dbName,
	}
}

func getCollectionSizeTags(namespaceName string, dbName string, collectionName string) map[string]string {
	return map[string]string{
		"tigris_tenant": namespaceName,
		"db":            dbName,
		"collection":    collectionName,
	}
}

func UpdateNameSpaceSizeMetrics(namespaceName string, size int64) {
	if NamespaceSize != nil {
		NamespaceSize.Tagged(getNamespaceSizeTags(namespaceName)).Gauge("bytes").Update(float64(size))
	}
}

func UpdateDbSizeMetrics(namespaceName string, dbName string, size int64) {
	if NamespaceSize != nil {
		DbSize.Tagged(getDbSizeTags(namespaceName, dbName)).Gauge("bytes").Update(float64(size))
	}
}

func UpdateCollectionSizeMetrics(namespaceName string, dbName string, collectionName string, size int64) {
	if NamespaceSize != nil {
		CollectionSize.Tagged(getCollectionSizeTags(namespaceName, dbName, collectionName)).Gauge("bytes").Update(float64(size))
	}
}
