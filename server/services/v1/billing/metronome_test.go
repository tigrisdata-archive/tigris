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
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/h2non/gock"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	biller "github.com/tigrisdata/metronome-go-client"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/uber-go/tally"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMetronome_CreateAccount(t *testing.T) {
	initializeMetricsForTest()
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
			JSON(map[string]any{
				"name":           tenantName,
				"ingest_aliases": []string{namespaceId},
			}).
			Reply(200).
			JSON(map[string]any{
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
		require.Equal(t, err, NewMetronomeError(401, []byte(`{"message":"Unauthorized"}`+"\n")))
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
		require.Equal(t, err, NewMetronomeError(409, []byte(`{"message":"ingest alias conflict"}`+"\n")))
		require.Empty(t, createdId)
		require.True(t, gock.IsDone())
	})
}

func TestMetronome_AddDefaultPlan(t *testing.T) {
	initializeMetricsForTest()
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
			JSON(map[string]any{
				"plan_id":     cfg.DefaultPlan,
				"starting_on": expectedDate,
			}).
			Reply(200).
			JSON(map[string]any{
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
		require.Equal(t, err, NewMetronomeError(400, []byte(`{"message":"No such plan"}`+"\n")))
		require.False(t, added)
		require.True(t, gock.IsDone())
	})
}

func TestMetronome_PushStorageEvents(t *testing.T) {
	initializeMetricsForTest()
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
			JSON([]map[string]any{
				{
					"customer_id":    namespaceId,
					"transaction_id": "t12354",
					"timestamp":      "2023-01-01T00:00:00Z",
					"event_type":     "storage",
					"properties": map[string]any{
						StorageDbBytes:    920_000_000,
						StorageIndexBytes: 256000,
					},
				},
				{
					"customer_id":    namespaceId,
					"transaction_id": "t12355",
					"timestamp":      "2023-02-02T00:00:00Z",
					"event_type":     "storage",
					"properties": map[string]any{
						StorageIndexBytes: 8_750_000_000,
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
		require.Equal(t, err, NewMetronomeError(400, []byte(`{"message":"Bad request"}`+"\n")))
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
	initializeMetricsForTest()
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
			JSON([]map[string]any{
				{
					"customer_id":    namespaceId,
					"transaction_id": "t12354",
					"timestamp":      "2023-01-01T00:00:00Z",
					"event_type":     "usage",
					"properties": map[string]any{
						UsageDbUnits:     920,
						UsageSearchUnits: 256,
					},
				},
				{
					"customer_id":    namespaceId,
					"transaction_id": "t12355",
					"timestamp":      "2023-02-02T00:00:00Z",
					"event_type":     "usage",
					"properties": map[string]any{
						UsageSearchUnits: 8_750,
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
		require.Equal(t, err, NewMetronomeError(400, []byte(`{"message":"Bad request"}`+"\n")))
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

func TestMetronome_FetchInvoices(t *testing.T) {
	initializeMetricsForTest()
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome, err := NewMetronomeProvider(cfg)
	require.NoError(t, err)
	ctx := context.TODO()

	t.Run("fetches valid invoices", func(t *testing.T) {
		accountId, err := uuid.Parse("b2108db4-6768-469b-93bb-23e49a4311d6")
		require.NoError(t, err)
		nextPage := "nextPage"
		startingTime := time.Date(2022, 12, 10, 0, 0, 0, 0, time.UTC)
		gock.New(cfg.URL).
			Get(fmt.Sprintf("/customers/%s/invoices", accountId)).
			MatchHeader("Authorization", cfg.ApiKey).
			MatchParam("limit", "20").
			MatchParam("next_page", nextPage).
			MatchParam("starting_on", "2022-12-10T00:00:00Z").
			Reply(200).
			JSON(map[string]any{
				"data": []map[string]any{
					{
						"id":         "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7",
						"plan_name":  "Free Tier",
						"line_items": []map[string]any{},
					},
				},
			})

		resp, err := metronome.GetInvoices(ctx, accountId, &api.ListInvoicesRequest{
			NextPage:   &nextPage,
			StartingOn: &timestamppb.Timestamp{Seconds: startingTime.Unix()},
		})
		require.NoError(t, err)

		require.NotNil(t, resp)
		require.Nil(t, resp.NextPage)
		require.Len(t, resp.Data, 1)
		require.Equal(t, "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7", resp.GetData()[0].GetId())
		require.Equal(t, "Free Tier", resp.GetData()[0].GetPlanName())
		require.True(t, gock.IsDone())
	})

	t.Run("fetches empty invoices", func(t *testing.T) {
		accountId, err := uuid.Parse("b2108db4-6768-469b-93bb-23e49a4311d6")
		require.NoError(t, err)
		gock.New(cfg.URL).
			Get(fmt.Sprintf("/customers/%s/invoices", accountId)).
			Reply(200).
			JSON(map[string]any{
				"data": []map[string]any{},
			})

		resp, err := metronome.GetInvoices(ctx, accountId, &api.ListInvoicesRequest{})
		require.NoError(t, err)

		require.NotNil(t, resp)
		require.Nil(t, resp.NextPage)
		require.Empty(t, resp.Data)
		require.True(t, gock.IsDone())
	})

	t.Run("bad request no customer", func(t *testing.T) {
		accountId, err := uuid.Parse("b2108db4-6768-469b-93bb-23e49a4311d6")
		require.NoError(t, err)
		gock.New(cfg.URL).
			Get(fmt.Sprintf("/customers/%s/invoices", accountId)).
			Reply(400).
			JSON(map[string]any{
				"message": "no customer exists",
			})

		resp, err := metronome.GetInvoices(ctx, accountId, &api.ListInvoicesRequest{})
		require.Equal(t, err, NewMetronomeError(400, []byte(`{"message":"no customer exists"}`+"\n")))
		require.Nil(t, resp)
		require.True(t, gock.IsDone())
	})
}

func TestMetronome_GetInvoiceById(t *testing.T) {
	initializeMetricsForTest()
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome, err := NewMetronomeProvider(cfg)
	require.NoError(t, err)
	ctx := context.TODO()

	t.Run("fetches valid invoices", func(t *testing.T) {
		accountId, err := uuid.Parse("b2108db4-6768-469b-93bb-23e49a4311d6")
		require.NoError(t, err)
		invoiceId := "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7"
		gock.New(cfg.URL).
			Get(fmt.Sprintf("/customers/%s/invoices/%s", accountId, invoiceId)).
			MatchHeader("Authorization", cfg.ApiKey).
			Reply(200).
			JSON(map[string]any{
				"data": map[string]any{
					"id":        "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7",
					"plan_name": "Free Tier",
					"line_items": []map[string]any{
						{
							"name":       "Monthly Platform Fee",
							"quantity":   1,
							"total":      0,
							"product_id": "b8b2e361-7e31-4283-a6e7-9b4c04a8a7eb",
							"sub_line_items": []map[string]any{
								{
									"name":      "Monthly Platform Fee",
									"price":     0,
									"quantity":  1,
									"subtotal":  0,
									"charge_id": "fc7e4e9d-2ce3-4850-87bc-4b972679aaab",
								},
							},
						},
					},
				},
			})

		resp, err := metronome.GetInvoiceById(ctx, accountId, invoiceId)
		require.NoError(t, err)

		require.NotNil(t, resp)
		require.Nil(t, resp.NextPage)
		require.Len(t, resp.Data, 1)
		require.Equal(t, "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7", resp.GetData()[0].GetId())
		require.Equal(t, "Free Tier", resp.GetData()[0].GetPlanName())
		require.True(t, gock.IsDone())
	})

	t.Run("fetches empty invoices", func(t *testing.T) {
		accountId, err := uuid.Parse("b2108db4-6768-469b-93bb-23e49a4311d6")
		require.NoError(t, err)
		invoiceId := "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7"
		gock.New(cfg.URL).
			Get(fmt.Sprintf("/customers/%s/invoices/%s", accountId, invoiceId)).
			Reply(200).
			JSON(map[string]any{
				"data": map[string]any{},
			})

		resp, err := metronome.GetInvoiceById(ctx, accountId, invoiceId)
		require.NoError(t, err)

		require.NotNil(t, resp)
		require.Nil(t, resp.NextPage)
		require.Empty(t, resp.Data)
		require.True(t, gock.IsDone())
	})

	t.Run("no invoice found", func(t *testing.T) {
		accountId, err := uuid.Parse("b2108db4-6768-469b-93bb-23e49a4311d6")
		require.NoError(t, err)
		invoiceId := "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7"
		gock.New(cfg.URL).
			Get(fmt.Sprintf("/customers/%s/invoices/%s", accountId, invoiceId)).
			Reply(404).
			JSON(map[string]any{
				"message": "no invoice found",
			})

		resp, err := metronome.GetInvoiceById(ctx, accountId, invoiceId)
		require.Equal(t, err, NewMetronomeError(404, []byte(`{"message":"no invoice found"}`+"\n")))
		require.Nil(t, resp)
		require.True(t, gock.IsDone())
	})

	t.Run("bad invoice id", func(t *testing.T) {
		accountId, err := uuid.Parse("b2108db4-6768-469b-93bb-23e49a4311d6")
		require.NoError(t, err)

		resp, err := metronome.GetInvoiceById(ctx, accountId, "invalid-id")
		require.ErrorContains(t, err, "invoiceId is not valid")
		require.Nil(t, resp)
	})
}

func TestMetronome_GetUsage(t *testing.T) {
	initializeMetricsForTest()
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	cfg.BilledMetrics = map[string]string{
		"db_bytes":       "bbab00f7-f127-46cd-83bf-6f9c01903779",
		UsageSearchUnits: "327e631d-e6dc-4c67-a2a8-dfd3621a907f",
	}
	metronome, err := NewMetronomeProvider(cfg)
	require.NoError(t, err)
	ctx := context.TODO()

	t.Run("retrieves usage successfully", func(t *testing.T) {
		cid := uuid.New()
		startDate := time.Date(2023, 4, 30, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
		require.NoError(t, err)
		req := &UsageRequest{
			BillableMetric: nil,
			StartTime:      &startDate,
			EndTime:        &endDate,
			NextPage:       nil,
		}

		bodyMatcher := gock.NewBasicMatcher()
		bodyMatcher.Add(func(actualReq *http.Request, expectedReq *gock.Request) (bool, error) {
			actualBuf, err := io.ReadAll(actualReq.Body)
			if err != nil {
				return false, err
			}

			var actualBody biller.GetUsageBatchJSONRequestBody
			err = jsoniter.Unmarshal(actualBuf, &actualBody)
			if err != nil {
				return false, err
			}

			if actualBody.WindowSize != "hour" {
				return false, fmt.Errorf("window_size mismatch. expected: '%s', actual: '%s'", "hour", actualBody.WindowSize)
			}

			if len(*actualBody.CustomerIds) != 1 || (*actualBody.CustomerIds)[0] != cid {
				return false, fmt.Errorf("customer_ids mismatch. expected: '%s', actual: '%v'", cid, actualBody.CustomerIds)
			}

			if actualBody.StartingOn != startDate {
				return false, fmt.Errorf("starting_on mismatch. expected: '%s', actual: '%s'", startDate, actualBody.StartingOn)
			}

			if actualBody.EndingBefore != endDate {
				return false, fmt.Errorf("ending_before mismatch. expected: '%s', actual: '%s'", endDate, actualBody.EndingBefore)
			}

			if len(*actualBody.BillableMetrics) != len(cfg.BilledMetrics) {
				return false, fmt.Errorf("billable_metrics length mismatch")
			}

			for _, bm := range *actualBody.BillableMetrics {
				if _, ok := metronome.billedMetricsById[bm.Id]; !ok {
					return false, fmt.Errorf("billable_metrics mismatch. missing '%s'", bm.Id)
				}
			}

			return true, nil
		})

		gock.New(cfg.URL).
			Post("/usage").
			MatchHeader("Authorization", cfg.ApiKey).
			MatchHeader("Content-Type", "application/json").
			MatchType("json").
			SetMatcher(bodyMatcher).
			Reply(200).
			JSON(map[string]any{
				"data": []map[string]any{{
					"billable_metric_id": "327e631d-e6dc-4c67-a2a8-dfd3621a907f", // search_units metric
					"start_timestamp":    "2023-04-30T01:00:00+00:00",
					"end_timestamp":      "2023-04-30T02:00:00+00:00",
					"value":              25,
				}, {
					"billable_metric_id": "50ff7b3f-4a5e-4bf9-87a8-1a99a18fae9f", // unknown metric
					"start_timestamp":    "2023-04-30T02:00:00+00:00",
					"end_timestamp":      "2023-04-30T03:00:00+00:00",
					"value":              218,
				}},
			})

		resp, err := metronome.GetUsage(ctx, cid, req)
		require.NoError(t, err)

		// requested metrics should always be included in response
		// unknown metrics to be ignored
		require.Len(t, resp.Data, len(cfg.BilledMetrics))
		for n := range cfg.BilledMetrics {
			require.Contains(t, resp.Data, n)
		}

		// db_bytes are not returned in API response, hence should be empty
		require.Empty(t, resp.Data["db_bytes"])
		// search_units have a single value in API response
		require.Contains(t, resp.Data, UsageSearchUnits)
		require.Len(t, resp.Data[UsageSearchUnits].Series, 1)

		expectedStartTime, err := time.Parse(time.RFC3339, "2023-04-30T01:00:00+00:00")
		require.NoError(t, err)
		expectedEndTime, err := time.Parse(time.RFC3339, "2023-04-30T02:00:00+00:00")
		require.NoError(t, err)
		require.Equal(t, &api.Usage{
			StartTime: timestamppb.New(expectedStartTime),
			EndTime:   timestamppb.New(expectedEndTime),
			Value:     float32(25),
		}, resp.Data[UsageSearchUnits].Series[0])
		require.Nil(t, resp.NextPage)

		require.True(t, gock.IsDone())
	})

	t.Run("when start time is missing", func(t *testing.T) {
		endTime := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
		req := &UsageRequest{
			BillableMetric: nil,
			StartTime:      nil,
			EndTime:        &endTime,
		}
		resp, err := metronome.GetUsage(ctx, uuid.New(), req)
		require.ErrorContains(t, err, "is not a valid start time")
		require.Nil(t, resp)
	})

	t.Run("when end time is missing", func(t *testing.T) {
		startTime := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
		req := &UsageRequest{
			BillableMetric: nil,
			StartTime:      &startTime,
			EndTime:        &time.Time{},
		}
		resp, err := metronome.GetUsage(ctx, uuid.New(), req)
		require.ErrorContains(t, err, "is not a valid end time")
		require.Nil(t, resp)
	})

	t.Run("when invalid window size provided", func(t *testing.T) {
		ts := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
		req := &UsageRequest{
			BillableMetric: nil,
			StartTime:      &ts,
			EndTime:        &ts,
			AggWindow:      3,
		}
		resp, err := metronome.GetUsage(ctx, uuid.New(), req)
		require.ErrorContains(t, err, "is not a valid AggWindow")
		require.Nil(t, resp)
	})

	t.Run("specific billable metrics are requested with pagination", func(t *testing.T) {
		cid, requestedMetric := uuid.New(), "db_bytes"
		rt := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
		nextPageToken := "next_page_token"

		gock.New(cfg.URL).
			Post("usage").
			MatchParam("next_page", nextPageToken).
			MatchType("json").
			JSON(map[string]any{
				"customer_ids": []string{cid.String()},
				"billable_metrics": []map[string]any{
					{"id": cfg.BilledMetrics[requestedMetric]},
				},
				"window_size":   "hour",
				"starting_on":   "2023-05-01T00:00:00Z",
				"ending_before": "2023-05-01T00:00:00Z",
			}).
			Reply(200).
			JSON(map[string]any{
				"data":      []map[string]any{},
				"next_page": "resp_next_page",
			})

		resp, err := metronome.GetUsage(ctx, cid, &UsageRequest{
			BillableMetric: &[]string{requestedMetric},
			StartTime:      &rt,
			EndTime:        &rt,
			NextPage:       &nextPageToken,
		})

		require.NoError(t, err)
		require.Len(t, resp.Data, 1)
		require.Len(t, resp.Data[requestedMetric].Series, 0)
		require.Equal(t, *resp.NextPage, "resp_next_page")

		require.True(t, gock.IsDone())
	})

	t.Run("invalid billable metric requested", func(t *testing.T) {
		rt := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
		req := &UsageRequest{
			BillableMetric: &[]string{"invalid_metric"},
			StartTime:      &rt,
			EndTime:        &rt,
		}
		resp, err := metronome.GetUsage(ctx, uuid.New(), req)
		require.ErrorContains(t, err, "is not a valid billable metric")
		require.Nil(t, resp)
	})

	t.Run("remote service failure", func(t *testing.T) {
		rt := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
		gock.New(cfg.URL).Post("usage").Reply(500).JSON(map[string]any{
			"message": "request failed",
		})
		resp, err := metronome.GetUsage(ctx, uuid.New(), &UsageRequest{
			BillableMetric: nil,
			StartTime:      &rt,
			EndTime:        &rt,
		})

		require.ErrorContains(t, err, "request failed")
		require.Nil(t, resp)
		require.True(t, gock.IsDone())
	})

	t.Run("remote service times out", func(t *testing.T) {
		rt := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
		gock.New(cfg.URL).Post("usage").ReplyError(fmt.Errorf("request timed out"))
		resp, err := metronome.GetUsage(ctx, uuid.New(), &UsageRequest{
			BillableMetric: nil,
			StartTime:      &rt,
			EndTime:        &rt,
		})

		require.ErrorContains(t, err, "request timed out")
		require.Nil(t, resp)
		require.True(t, gock.IsDone())
	})
}

func TestMetronome_GetAccountId(t *testing.T) {
	initializeMetricsForTest()
	defer gock.Off()
	cfg := config.DefaultConfig.Billing.Metronome
	metronome, err := NewMetronomeProvider(cfg)
	require.NoError(t, err)
	ctx := context.TODO()

	t.Run("when a valid customer exists", func(t *testing.T) {
		nsId, expectedAccountId := "namespaceId", uuid.New()

		gock.New(cfg.URL).
			Get("customers").
			MatchParam("ingest_alias", nsId).
			Reply(200).
			JSON(map[string]any{
				"data": []map[string]any{{
					"id":   expectedAccountId,
					"name": "new customer",
				}},
			})
		id, err := metronome.GetAccountId(ctx, nsId)

		require.NoError(t, err)
		require.Equal(t, expectedAccountId, id)
		require.True(t, gock.IsDone())
	})

	t.Run("given a invalid namespaceId", func(t *testing.T) {
		id, err := metronome.GetAccountId(ctx, "")
		require.ErrorContains(t, err, "cannot be empty")
		require.Equal(t, uuid.Nil, id)
		require.True(t, gock.IsDone())
	})

	t.Run("no customer exists on metronome", func(t *testing.T) {
		nsId := "namespaceId"

		gock.New(cfg.URL).
			Get("customers").
			MatchParam("ingest_alias", nsId).
			Reply(200).
			JSON(map[string]any{
				"data": []map[string]any{},
			})

		id, err := metronome.GetAccountId(ctx, nsId)
		require.ErrorContains(t, err, "no account found")
		require.Equal(t, uuid.Nil, id)
		require.True(t, gock.IsDone())
	})

	t.Run("remote service failure", func(t *testing.T) {
		nsId := "namespaceId"

		gock.New(cfg.URL).
			Get("customers").
			MatchParam("ingest_alias", nsId).
			Reply(400).
			JSON(map[string]any{
				"message": "bad request",
			})

		id, err := metronome.GetAccountId(ctx, nsId)
		require.ErrorContains(t, err, "bad request")
		require.Equal(t, uuid.Nil, id)
		require.True(t, gock.IsDone())
	})

	t.Run("remote service times out", func(t *testing.T) {
		gock.New(cfg.URL).Get("customers").ReplyError(fmt.Errorf("request timed out"))
		id, err := metronome.GetAccountId(ctx, "ns123")

		require.ErrorContains(t, err, "request timed out")
		require.Equal(t, uuid.Nil, id)
		require.True(t, gock.IsDone())
	})
	require.True(t, gock.IsDone())
}

func TestMetronome_buildInvoice(t *testing.T) {
	t.Run("deserializes complete invoice", func(t *testing.T) {
		payload := `
		{
            "id": "f9f8168c-dab6-4fd6-9008-e41dabda7bd5",
            "customer_id": "b2108db4-6768-469b-93bb-23e49a4311d6",
            "credit_type": {
                "id": "2714e483-4ff1-48e4-9e25-ac732e8f24f2",
                "name": "USD (cents)"
            },
            "invoice_adjustments": [],
            "plan_id": "433b615b-523d-4515-80af-88cdfc3b213d",
            "plan_name": "Free Tier",
            "line_items": [
                {
                    "custom_fields": {},
                    "name": "Monthly Platform Fee",
                    "quantity": 1,
                    "total": 0,
                    "credit_type": {
                        "id": "2714e483-4ff1-48e4-9e25-ac732e8f24f2",
                        "name": "USD (cents)"
                    },
                    "product_id": "b8b2e361-7e31-4283-a6e7-9b4c04a8a7eb",
                    "sub_line_items": [
                        {
                            "name": "Monthly Platform Fee",
                            "price": 0,
                            "quantity": 1,
                            "subtotal": 0,
                            "charge_id": "fc7e4e9d-2ce3-4850-87bc-4b972679aaab",
                            "custom_fields": {}
                        }
                    ]
                },
                {
                    "custom_fields": {},
                    "name": "Search",
                    "quantity": 1,
                    "total": 16190,
                    "credit_type": {
                        "id": "2714e483-4ff1-48e4-9e25-ac732e8f24f2",
                        "name": "USD (cents)"
                    },
                    "product_id": "7054b561-327f-4dc5-a673-e8b07aecb7ad",
                    "sub_line_items": [
                        {
                            "name": "Max Search Index size (MB)",
                            "quantity": 8345,
                            "subtotal": 16190,
                            "charge_id": "65a9ca10-7a90-478a-9b07-45eeee2a6df1",
                            "custom_fields": {},
                            "tiers": [
                                {
                                    "starting_at": 0,
                                    "quantity": 250,
                                    "price": 0,
                                    "subtotal": 0
                                },
                                {
                                    "starting_at": 250,
                                    "quantity": 8095,
                                    "price": 2,
                                    "subtotal": 16190
                                }
                            ]
                        },
                        {
                            "name": "Total Search Request Units ( x 1000)",
                            "quantity": 0,
                            "subtotal": 0,
                            "charge_id": "ed8c01e2-96b5-4b3f-b7cb-14ab7d7cd9f4",
                            "custom_fields": {}
                        }
                    ]
                },
                {
                    "custom_fields": {},
                    "name": "Database",
                    "quantity": 1,
                    "total": 5700,
                    "credit_type": {
                        "id": "2714e483-4ff1-48e4-9e25-ac732e8f24f2",
                        "name": "USD (cents)"
                    },
                    "product_id": "cdd6ae4e-560d-4270-bafd-ee842ff88bf7",
                    "sub_line_items": [
                        {
                            "name": "Max DB size (GB)",
                            "quantity": 62,
                            "subtotal": 5700,
                            "charge_id": "ceb4e1d8-02f5-4c89-a878-4ee04a0312da",
                            "custom_fields": {},
                            "tiers": [
                                {
                                    "starting_at": 0,
                                    "quantity": 5,
                                    "price": 0,
                                    "subtotal": 0
                                },
                                {
                                    "starting_at": 5,
                                    "quantity": 57,
                                    "price": 100,
                                    "subtotal": 5700
                                }
                            ]
                        },
                        {
                            "name": "Total Database Request Units ( x 1,000,000)",
                            "quantity": 0,
                            "subtotal": 0,
                            "charge_id": "23ab4655-9d9f-49e9-8fd5-9c60d8c6490a",
                            "custom_fields": {}
                        }
                    ]
                }
            ],
            "start_timestamp": "2023-04-01T00:00:00+00:00",
            "end_timestamp": "2023-05-01T00:00:00+00:00",
            "status": "DRAFT",
            "subtotal": 21890,
            "total": 21890,
            "external_invoice": null
        }`
		var mInvoice biller.Invoice
		err := jsoniter.Unmarshal([]byte(payload), &mInvoice)

		require.NoError(t, err)

		built := buildInvoice(mInvoice)
		require.NotNil(t, built)

		require.Equal(t, mInvoice.Id.String(), built.GetId())
		require.Equal(t, mInvoice.StartTimestamp.Unix(), built.GetStartTime().GetSeconds())
		require.Equal(t, mInvoice.EndTimestamp.Unix(), built.GetEndTime().GetSeconds())
		require.Equal(t, mInvoice.Subtotal, built.GetSubtotal())
		require.Equal(t, mInvoice.Total, built.GetTotal())
		require.Equal(t, *mInvoice.PlanName, built.GetPlanName())

		// entries
		require.Len(t, built.GetEntries(), len(mInvoice.LineItems))
		for i, b := range built.Entries {
			line := mInvoice.LineItems[i]
			require.Equal(t, line.Name, b.GetName())
			require.Equal(t, line.Quantity, b.GetQuantity())
			require.Equal(t, line.Total, b.GetTotal())

			// charges
			require.Len(t, b.GetCharges(), len(line.SubLineItems))
			for j, charge := range b.GetCharges() {
				sub := line.SubLineItems[j]
				require.Equal(t, sub.Name, charge.GetName())
				require.Equal(t, sub.Quantity, charge.GetQuantity())
				require.Equal(t, sub.Subtotal, charge.GetSubtotal())

				// tiers
				if sub.Tiers == nil {
					require.Len(t, charge.GetTiers(), 0)
				} else {
					require.Len(t, charge.GetTiers(), len(*sub.Tiers))
					for k, tier := range charge.GetTiers() {
						mTier := (*sub.Tiers)[k]
						require.Equal(t, mTier.StartingAt, tier.GetStartingAt())
						require.Equal(t, mTier.Quantity, tier.GetQuantity())
						require.Equal(t, mTier.Subtotal, tier.GetSubtotal())
						require.Equal(t, mTier.Price, tier.GetPrice())
					}
				}
			}
		}
	})
}

func initializeMetricsForTest() {
	metrics.MetronomeCreateAccount = tally.NewTestScope("create_account", map[string]string{})
	metrics.MetronomeAddPlan = tally.NewTestScope("add_plan", map[string]string{})
	metrics.MetronomeIngest = tally.NewTestScope("ingest", map[string]string{})
	metrics.MetronomeListInvoices = tally.NewTestScope("list_invoices", map[string]string{})
	metrics.MetronomeGetInvoice = tally.NewTestScope("get_invoice", map[string]string{})
	metrics.MetronomeGetUsage = tally.NewTestScope("get_usage", map[string]string{})
	metrics.MetronomeGetCustomer = tally.NewTestScope("get_customer", map[string]string{})
}
