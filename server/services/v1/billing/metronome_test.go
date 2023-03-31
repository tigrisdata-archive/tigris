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

	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
)

func TestMetronome_CreateAccount(t *testing.T) {
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome := &Metronome{Config: cfg}
	ctx := context.TODO()

	t.Run("creating account succeeds in metronome", func(t *testing.T) {
		namespaceId := "nsId_123"
		tenantName := "foo tenant"
		gock.New(cfg.URL).
			Post("customers").
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
					"id": "metronome123",
				},
			})

		createdId, err := metronome.CreateAccount(ctx, namespaceId, tenantName)
		require.NoError(t, err)
		require.Equal(t, "metronome123", createdId)
	})

	t.Run("Invalid API key", func(t *testing.T) {
		gock.New(cfg.URL).
			Reply(401).
			JSON(map[string]string{
				"message": "Unauthorized",
			})

		createdId, err := metronome.CreateAccount(ctx, "nsId1", "foo_tenant")
		require.ErrorContains(t, err, "401 Unauthorized")
		require.Empty(t, createdId)
	})

	require.True(t, gock.IsDone())
}

func TestMetronome_AddDefaultPlan(t *testing.T) {
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome := &Metronome{Config: cfg}
	ctx := context.TODO()

	t.Run("create new time", func(t *testing.T) {
		metronomeId := "nsId_123"
		yyyy, mm, dd := time.Now().UTC().Date()
		expectedDate := time.Date(yyyy, mm, dd, 0, 0, 0, 0, time.UTC).Format(TimeFormat)

		gock.New(cfg.URL).
			Post(fmt.Sprintf("customers/%s/plans/add", metronomeId)).
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
					"id": "plan123",
				},
			})

		added, err := metronome.AddDefaultPlan(ctx, metronomeId)
		require.NoError(t, err)
		require.True(t, added)
	})

	t.Run("bad request when no plan exists", func(t *testing.T) {
		metronomeId := "nsId_123"

		gock.New(cfg.URL).
			Post(fmt.Sprintf("customers/%s/plans/add", metronomeId)).
			Reply(400).
			JSON(map[string]string{
				"message": "No such plan",
			})

		added, err := metronome.AddDefaultPlan(ctx, metronomeId)
		require.ErrorContains(t, err, "No such plan")
		require.False(t, added)
	})

	require.True(t, gock.IsDone())
}
