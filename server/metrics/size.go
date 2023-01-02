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
		"tigris_tenant_name",
		"version",
	}
}

func getDbSizeTagKeys() []string {
	return []string{
		"env",
		"service",
		"tigris_tenant",
		"tigris_tenant_name",
		"version",
		"db",
		"project",
	}
}

func getCollectionSizeTagKeys() []string {
	return []string{
		"env",
		"service",
		"tigris_tenant",
		"tigris_tenant_name",
		"version",
		"db",
		"project",
		"collection",
	}
}

func getNamespaceSizeTags(namespace string, namespaceName string) map[string]string {
	return map[string]string{
		"tigris_tenant":      namespace,
		"tigris_tenant_name": GetTenantNameTagValue(namespace, namespaceName),
	}
}

func getDbSizeTags(namespace string, namespaceName string, dbName string) map[string]string {
	return map[string]string{
		"tigris_tenant":      namespace,
		"tigris_tenant_name": GetTenantNameTagValue(namespace, namespaceName),
		"db":                 dbName,
		"project":            dbName,
	}
}

func getCollectionSizeTags(namespace string, namespaceName string, dbName string, collectionName string) map[string]string {
	return map[string]string{
		"tigris_tenant":      namespace,
		"tigris_tenant_name": GetTenantNameTagValue(namespace, namespaceName),
		"db":                 dbName,
		"project":            dbName,
		"collection":         collectionName,
	}
}

func UpdateNameSpaceSizeMetrics(namespace string, namespaceName string, size int64) {
	if NamespaceSize != nil {
		NamespaceSize.Tagged(getNamespaceSizeTags(namespace, namespaceName)).Gauge("bytes").Update(float64(size))
	}
}

func UpdateDbSizeMetrics(namespace string, namespaceName string, dbName string, size int64) {
	if NamespaceSize != nil {
		DbSize.Tagged(getDbSizeTags(namespace, namespaceName, dbName)).Gauge("bytes").Update(float64(size))
	}
}

func UpdateCollectionSizeMetrics(namespace string, namespaceName string, dbName string, collectionName string, size int64) {
	if NamespaceSize != nil {
		CollectionSize.Tagged(getCollectionSizeTags(namespace, namespaceName, dbName, collectionName)).Gauge("bytes").Update(float64(size))
	}
}
