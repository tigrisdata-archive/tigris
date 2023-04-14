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

package billing

import (
	"time"

	biller "github.com/tigrisdata/metronome-go-client"
)

const (
	EventTypeUsage   string = "usage"
	EventTypeStorage string = "storage"
)

type UsageEvent struct {
	biller.Event
}

func NewUsageEventBuilder() *UsageEventBuilder {
	return &UsageEventBuilder{}
}

type UsageEventBuilder struct {
	namespaceId   string
	transactionId string
	timestamp     string
	databaseUnits *int64
	searchUnits   *int64
}

func (ub *UsageEventBuilder) WithNamespaceId(id string) *UsageEventBuilder {
	ub.namespaceId = id
	return ub
}

func (ub *UsageEventBuilder) WithTransactionId(id string) *UsageEventBuilder {
	ub.transactionId = id
	return ub
}

func (ub *UsageEventBuilder) WithTimestamp(ts time.Time) *UsageEventBuilder {
	ub.timestamp = ts.Format(TimeFormat)
	return ub
}

func (ub *UsageEventBuilder) WithDatabaseUnits(value int64) *UsageEventBuilder {
	ub.databaseUnits = &value
	return ub
}

func (ub *UsageEventBuilder) WithSearchUnits(value int64) *UsageEventBuilder {
	ub.searchUnits = &value
	return ub
}

func (ub *UsageEventBuilder) Build() *UsageEvent {
	billingMetric := &UsageEvent{}
	billingMetric.EventType = EventTypeUsage
	billingMetric.CustomerId = ub.namespaceId
	billingMetric.TransactionId = ub.transactionId
	billingMetric.Timestamp = ub.timestamp
	props := make(map[string]interface{})

	// the key names must match the registered billing metrics in metronome
	if ub.searchUnits != nil {
		props["search_units"] = *ub.searchUnits
	}

	if ub.databaseUnits != nil {
		props["database_units"] = *ub.databaseUnits
	}
	billingMetric.Properties = &props
	return billingMetric
}

type StorageEvent struct {
	biller.Event
}

func NewStorageEventBuilder() *StorageEventBuilder {
	return &StorageEventBuilder{}
}

type StorageEventBuilder struct {
	namespaceId   string
	transactionId string
	timestamp     string
	databaseBytes *int64
	indexBytes    *int64
}

func (sb *StorageEventBuilder) WithNamespaceId(id string) *StorageEventBuilder {
	sb.namespaceId = id
	return sb
}

func (sb *StorageEventBuilder) WithTransactionId(id string) *StorageEventBuilder {
	sb.transactionId = id
	return sb
}

func (sb *StorageEventBuilder) WithTimestamp(ts time.Time) *StorageEventBuilder {
	sb.timestamp = ts.Format(TimeFormat)
	return sb
}

func (sb *StorageEventBuilder) WithDatabaseBytes(value int64) *StorageEventBuilder {
	sb.databaseBytes = &value
	return sb
}

func (sb *StorageEventBuilder) WithIndexBytes(value int64) *StorageEventBuilder {
	sb.indexBytes = &value
	return sb
}

func (sb *StorageEventBuilder) Build() *StorageEvent {
	billingMetric := &StorageEvent{}
	billingMetric.EventType = EventTypeStorage
	billingMetric.CustomerId = sb.namespaceId
	billingMetric.TransactionId = sb.transactionId
	billingMetric.Timestamp = sb.timestamp
	props := make(map[string]interface{})

	// the key names must match the registered billing metrics in metronome
	if sb.indexBytes != nil {
		props["index_bytes"] = *sb.indexBytes
	}

	if sb.databaseBytes != nil {
		props["database_bytes"] = *sb.databaseBytes
	}
	billingMetric.Properties = &props
	return billingMetric
}
