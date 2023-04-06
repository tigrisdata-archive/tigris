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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
)

func TestMetronome_CreateAccount(t *testing.T) {
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome, err := NewMetronomeProvider(cfg)
	require.NoError(t, err)
	ctx := context.TODO()

	t.Run("creating account succeeds in metronome", func(t *testing.T) {
		namespaceId := "nsId_123"
		tenantName := "foo tenant"
		gock.New(cfg.URL).
			Post("/customers").
			MatchHeader("Authorization", cfg.ApiKey).
			MatchHeader("Content-Type", "application/json").
			MatchType("json").
			JSON(map[string]interface{}{
				"name":           tenantName,
				"ingest_aliases": []string{namespaceId},
			}).
			Reply(200).
			JSON(map[string]interface{}{
				"data": map[string]string{
					"id": "16d145ec-d18e-11ed-afa1-0242ac120002",
				},
			})

		createdId, err := metronome.CreateAccount(ctx, namespaceId, tenantName)
		require.NoError(t, err)
		require.Equal(t, "16d145ec-d18e-11ed-afa1-0242ac120002", createdId.String())
		require.True(t, gock.IsDone())
	})

	t.Run("Invalid API key", func(t *testing.T) {
		gock.New(cfg.URL).
			Reply(401).
			JSON(map[string]string{
				"message": "Unauthorized",
			})

		createdId, err := metronome.CreateAccount(ctx, "nsId1", "foo_tenant")
		require.ErrorContains(t, err, "Unauthorized")
		require.Empty(t, createdId)
		require.True(t, gock.IsDone())
	})

	t.Run("account already exists", func(t *testing.T) {
		gock.New(cfg.URL).
			Post("/customers").
			Reply(409).
			JSON(map[string]string{
				"message": "ingest alias conflict",
			})

		createdId, err := metronome.CreateAccount(ctx, "nsId1", "foo_tenant")
		require.ErrorContains(t, err, "ingest alias conflict")
		require.Empty(t, createdId)
		require.True(t, gock.IsDone())
	})
}

func TestMetronome_AddDefaultPlan(t *testing.T) {
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome, err := NewMetronomeProvider(cfg)
	require.NoError(t, err)
	ctx := context.TODO()

	t.Run("create new time", func(t *testing.T) {
		metronomeId := uuid.New()
		yyyy, mm, dd := time.Now().UTC().Date()
		expectedDate := time.Date(yyyy, mm, dd, 0, 0, 0, 0, time.UTC).Format(TimeFormat)

		gock.New(cfg.URL).
			Post(fmt.Sprintf("/customers/%s/plans/add", metronomeId)).
			MatchHeader("Authorization", cfg.ApiKey).
			MatchHeader("Content-Type", "application/json").
			MatchType("json").
			JSON(map[string]interface{}{
				"plan_id":     cfg.DefaultPlan,
				"starting_on": expectedDate,
			}).
			Reply(200).
			JSON(map[string]interface{}{
				"data": map[string]string{
					"id": "47eda90f-d2e8-4184-8955-cb3a64677821",
				},
			})

		added, err := metronome.AddDefaultPlan(ctx, metronomeId)
		require.NoError(t, err)
		require.True(t, added)
		require.True(t, gock.IsDone())
	})

	t.Run("bad request when no plan exists", func(t *testing.T) {
		metronomeId := uuid.New()

		gock.New(cfg.URL).
			Post(fmt.Sprintf("/customers/%s/plans/add", metronomeId)).
			Reply(400).
			JSON(map[string]string{
				"message": "No such plan",
			})

		added, err := metronome.AddDefaultPlan(ctx, metronomeId)
		require.ErrorContains(t, err, "No such plan")
		require.False(t, added)
		require.True(t, gock.IsDone())
	})
}

func TestMetronome_PushStorageEvents(t *testing.T) {
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome, err := NewMetronomeProvider(cfg)
	require.NoError(t, err)
	ctx := context.TODO()

	t.Run("Skips invalid events", func(t *testing.T) {
		namespaceId := "ns123"
		events := []*StorageEvent{
			NewStorageEventBuilder().
				WithNamespaceId(namespaceId).
				WithTransactionId("t12354").
				WithTimestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)).
				WithIndexBytes(256000).
				WithDatabaseBytes(920_000_000).
				Build(),
			NewStorageEventBuilder().
				WithNamespaceId(namespaceId).
				WithTransactionId("t12355").
				WithTimestamp(time.Date(2023, 2, 2, 0, 0, 0, 0, time.UTC)).
				WithIndexBytes(8_750_000_000).
				Build(),
			nil,
			NewStorageEventBuilder().
				WithNamespaceId(namespaceId).
				WithTransactionId("t12355").
				WithTimestamp(time.Date(2023, 2, 2, 0, 0, 0, 0, time.UTC)).
				Build(),
		}

		gock.New(cfg.URL).
			Post("/ingest").
			MatchHeader("Authorization", cfg.ApiKey).
			MatchHeader("Content-Type", "application/json").
			MatchType("json").
			JSON([]map[string]interface{}{
				{
					"customer_id":    namespaceId,
					"transaction_id": "t12354",
					"timestamp":      "2023-01-01T00:00:00Z",
					"event_type":     "storage",
					"properties": map[string]interface{}{
						"database_bytes": 920_000_000,
						"index_bytes":    256000,
					},
				},
				{
					"customer_id":    namespaceId,
					"transaction_id": "t12355",
					"timestamp":      "2023-02-02T00:00:00Z",
					"event_type":     "storage",
					"properties": map[string]interface{}{
						"index_bytes": 8_750_000_000,
					},
				},
			}).
			Reply(200).
			JSON(nil)

		err := metronome.PushStorageEvents(ctx, events)
		require.NoError(t, err)
		require.True(t, gock.IsDone())
	})

	t.Run("bad request", func(t *testing.T) {
		gock.New(cfg.URL).
			Post("/ingest").
			Reply(400).
			JSON(map[string]string{
				"message": "Bad request",
			})

		err := metronome.PushStorageEvents(ctx, []*StorageEvent{
			NewStorageEventBuilder().
				WithNamespaceId("someId").
				WithTransactionId("t12354").
				WithTimestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)).
				WithIndexBytes(256000).
				WithDatabaseBytes(920_000_000).
				Build(),
		})
		require.ErrorContains(t, err, "Bad request")
		require.True(t, gock.IsDone())
	})

	t.Run("empty events", func(t *testing.T) {
		gock.New(cfg.URL).
			Post("/ingest").
			Reply(200).
			JSON(nil)

		// no properties
		err := metronome.PushStorageEvents(ctx, []*StorageEvent{
			NewStorageEventBuilder().
				WithNamespaceId("someId").
				WithTransactionId("t12354").
				WithTimestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)).
				Build(),
		})
		require.NoError(t, err)
		require.False(t, gock.IsDone())
	})
}

func TestMetronome_PushUsageEvents(t *testing.T) {
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome, err := NewMetronomeProvider(cfg)
	require.NoError(t, err)
	ctx := context.TODO()

	t.Run("Skips invalid events", func(t *testing.T) {
		namespaceId := "ns123"
		events := []*UsageEvent{
			NewUsageEventBuilder().
				WithNamespaceId(namespaceId).
				WithTransactionId("t12354").
				WithTimestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)).
				WithSearchUnits(256).
				WithDatabaseUnits(920).
				Build(),
			NewUsageEventBuilder().
				WithNamespaceId(namespaceId).
				WithTransactionId("t12355").
				WithTimestamp(time.Date(2023, 2, 2, 0, 0, 0, 0, time.UTC)).
				WithSearchUnits(8_750).
				Build(),
			nil,
			NewUsageEventBuilder().
				WithNamespaceId(namespaceId).
				WithTransactionId("t12355").
				WithTimestamp(time.Date(2023, 2, 2, 0, 0, 0, 0, time.UTC)).
				Build(),
		}

		gock.New(cfg.URL).
			Post("/ingest").
			MatchHeader("Authorization", cfg.ApiKey).
			MatchHeader("Content-Type", "application/json").
			MatchType("json").
			JSON([]map[string]interface{}{
				{
					"customer_id":    namespaceId,
					"transaction_id": "t12354",
					"timestamp":      "2023-01-01T00:00:00Z",
					"event_type":     "usage",
					"properties": map[string]interface{}{
						"database_units": 920,
						"search_units":   256,
					},
				},
				{
					"customer_id":    namespaceId,
					"transaction_id": "t12355",
					"timestamp":      "2023-02-02T00:00:00Z",
					"event_type":     "usage",
					"properties": map[string]interface{}{
						"search_units": 8_750,
					},
				},
			}).
			Reply(200).
			JSON(nil)

		err := metronome.PushUsageEvents(ctx, events)
		require.NoError(t, err)
		require.True(t, gock.IsDone())
	})

	t.Run("bad request", func(t *testing.T) {
		gock.New(cfg.URL).
			Post("/ingest").
			Reply(400).
			JSON(map[string]string{
				"message": "Bad request",
			})

		err := metronome.PushUsageEvents(ctx, []*UsageEvent{
			NewUsageEventBuilder().
				WithNamespaceId("someId").
				WithTransactionId("t12354").
				WithTimestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)).
				WithSearchUnits(256).
				WithDatabaseUnits(920).
				Build(),
		})
		require.ErrorContains(t, err, "Bad request")
		require.True(t, gock.IsDone())
	})

	t.Run("empty events", func(t *testing.T) {
		gock.New(cfg.URL).
			Post("/ingest").
			Reply(200).
			JSON(nil)

		// no properties
		err := metronome.PushUsageEvents(ctx, []*UsageEvent{
			NewUsageEventBuilder().
				WithNamespaceId("someId").
				WithTransactionId("t12354").
				WithTimestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)).
				Build(),
		})
		require.NoError(t, err)
		require.False(t, gock.IsDone())
	})
}
