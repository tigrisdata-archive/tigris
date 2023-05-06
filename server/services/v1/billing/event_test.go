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
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func TestStorageEvent(t *testing.T) {
	t.Run("with single property", func(t *testing.T) {
		billingEvent := NewStorageEventBuilder().
			WithDatabaseBytes(24).
			WithNamespaceId("cid").
			Build()

		jsonEvent, err := jsoniter.Marshal(billingEvent)
		require.NoError(t, err)
		var actual map[string]any
		err = jsoniter.Unmarshal(jsonEvent, &actual)
		require.NoError(t, err)

		require.Equal(t, actual["customer_id"], billingEvent.CustomerId)
		require.Equal(t, actual["transaction_id"], billingEvent.TransactionId)
		require.NotEqual(t, actual["timestamp"], "")
		require.Equal(t, actual["event_type"], "storage")
		require.Equal(t, actual["properties"], map[string]any{
			StorageDbBytes: float64(24),
		})
	})

	t.Run("with 0 values", func(t *testing.T) {
		billingEvent := NewStorageEventBuilder().
			WithDatabaseBytes(0).
			WithIndexBytes(0).Build()

		jsonEvent, err := jsoniter.Marshal(billingEvent)
		require.NoError(t, err)
		var actual map[string]any
		err = jsoniter.Unmarshal(jsonEvent, &actual)
		require.NoError(t, err)

		require.Equal(t, actual["customer_id"], billingEvent.CustomerId)
		require.Equal(t, actual["transaction_id"], billingEvent.TransactionId)
		require.NotEqual(t, actual["timestamp"], "")
		require.Equal(t, actual["event_type"], "storage")
		require.Equal(t, actual["properties"], map[string]any{
			StorageIndexBytes: float64(0),
			StorageDbBytes:    float64(0),
		})
	})

	t.Run("complete object", func(t *testing.T) {
		billingEvent := NewStorageEventBuilder().
			WithNamespaceId("c1").
			WithTransactionId("t1").
			WithTimestamp(time.Date(2023, 2, 21, 8, 53, 41, 0, time.UTC)).
			WithIndexBytes(12).
			WithDatabaseBytes(345).
			Build()

		jsonEvent, err := jsoniter.Marshal(billingEvent)
		require.NoError(t, err)
		var actual map[string]any
		err = jsoniter.Unmarshal(jsonEvent, &actual)
		require.NoError(t, err)

		require.Equal(t, actual["customer_id"], billingEvent.CustomerId)
		require.Equal(t, actual["transaction_id"], billingEvent.TransactionId)
		require.Equal(t, actual["timestamp"], "2023-02-21T08:53:41Z")
		require.Equal(t, actual["event_type"], "storage")
		require.Equal(t, actual["properties"], map[string]any{
			StorageIndexBytes: float64(12),
			StorageDbBytes:    float64(345),
		})
	})

	t.Run("without transaction id", func(t *testing.T) {
		billingEvent := NewStorageEventBuilder().
			WithNamespaceId("c1").
			WithTimestamp(time.Date(2023, 2, 21, 8, 53, 41, 0, time.UTC)).
			Build()

		jsonEvent, err := jsoniter.Marshal(billingEvent)
		require.NoError(t, err)
		var actual map[string]any
		err = jsoniter.Unmarshal(jsonEvent, &actual)
		require.NoError(t, err)
		require.Equal(t, actual["customer_id"], billingEvent.CustomerId)
		require.Equal(t, actual["transaction_id"], "c1_1676969621000")
		require.Equal(t, actual["timestamp"], "2023-02-21T08:53:41Z")
		require.Equal(t, actual["event_type"], "storage")
		require.Equal(t, actual["properties"], map[string]any{})
	})
}

func TestUsageEvent(t *testing.T) {
	t.Run("with single property", func(t *testing.T) {
		billingEvent := NewUsageEventBuilder().WithDatabaseUnits(55).WithNamespaceId("cid").Build()

		jsonEvent, err := jsoniter.Marshal(billingEvent)
		require.NoError(t, err)
		var actual map[string]any
		err = jsoniter.Unmarshal(jsonEvent, &actual)
		require.NoError(t, err)

		require.Equal(t, actual["customer_id"], billingEvent.CustomerId)
		require.Equal(t, actual["transaction_id"], billingEvent.TransactionId)
		require.NotEqual(t, actual["timestamp"], "")
		require.Equal(t, actual["event_type"], "usage")
		require.Equal(t, actual["properties"], map[string]any{
			UsageDbUnits: float64(55),
		})
	})

	t.Run("with 0 values", func(t *testing.T) {
		billingEvent := NewUsageEventBuilder().WithDatabaseUnits(0).WithSearchUnits(0).Build()

		jsonEvent, err := jsoniter.Marshal(billingEvent)
		require.NoError(t, err)
		var actual map[string]any
		err = jsoniter.Unmarshal(jsonEvent, &actual)
		require.NoError(t, err)

		require.Equal(t, actual["customer_id"], billingEvent.CustomerId)
		require.Equal(t, actual["transaction_id"], billingEvent.TransactionId)
		require.NotEqual(t, actual["timestamp"], "")
		require.Equal(t, actual["event_type"], "usage")
		require.Equal(t, actual["properties"], map[string]any{
			UsageDbUnits:     float64(0),
			UsageSearchUnits: float64(0),
		})
	})

	t.Run("complete object", func(t *testing.T) {
		billingEvent := NewUsageEventBuilder().
			WithNamespaceId("c1").
			WithTransactionId("t1").
			WithTimestamp(time.Date(2023, 2, 21, 8, 53, 41, 0, time.UTC)).
			WithDatabaseUnits(123).
			WithSearchUnits(542).
			Build()

		jsonEvent, err := jsoniter.Marshal(billingEvent)
		require.NoError(t, err)
		var actual map[string]any
		err = jsoniter.Unmarshal(jsonEvent, &actual)
		require.NoError(t, err)

		require.Equal(t, actual["customer_id"], billingEvent.CustomerId)
		require.Equal(t, actual["transaction_id"], billingEvent.TransactionId)
		require.Equal(t, actual["timestamp"], "2023-02-21T08:53:41Z")
		require.Equal(t, actual["event_type"], "usage")
		require.Equal(t, actual["properties"], map[string]any{
			UsageDbUnits:     float64(123),
			UsageSearchUnits: float64(542),
		})
	})

	t.Run("without transaction id", func(t *testing.T) {
		billingEvent := NewUsageEventBuilder().
			WithNamespaceId("c1").
			WithTimestamp(time.Date(2023, 2, 21, 8, 53, 41, 0, time.UTC)).
			Build()

		jsonEvent, err := jsoniter.Marshal(billingEvent)
		require.NoError(t, err)
		var actual map[string]any
		err = jsoniter.Unmarshal(jsonEvent, &actual)
		require.NoError(t, err)

		require.Equal(t, actual["customer_id"], billingEvent.CustomerId)
		require.Equal(t, actual["transaction_id"], "c1_1676969621000")
		require.Equal(t, actual["timestamp"], "2023-02-21T08:53:41Z")
		require.Equal(t, actual["event_type"], "usage")
		require.Equal(t, actual["properties"], map[string]any{})
	})
}
