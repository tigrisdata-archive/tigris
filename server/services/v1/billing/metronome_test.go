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
			JSON(map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id":         "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7",
						"plan_name":  "Free Tier",
						"line_items": []map[string]interface{}{},
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
			JSON(map[string]interface{}{
				"data": []map[string]interface{}{},
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
			JSON(map[string]interface{}{
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
			JSON(map[string]interface{}{
				"data": map[string]interface{}{
					"id":        "50670f17-ca2d-4f9b-82a7-dc0e7c1f6ed7",
					"plan_name": "Free Tier",
					"line_items": []map[string]interface{}{
						{
							"name":       "Monthly Platform Fee",
							"quantity":   1,
							"total":      0,
							"product_id": "b8b2e361-7e31-4283-a6e7-9b4c04a8a7eb",
							"sub_line_items": []map[string]interface{}{
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
			JSON(map[string]interface{}{
				"data": map[string]interface{}{},
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
			JSON(map[string]interface{}{
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
	metrics.MetronomeResponseCode = tally.NewTestScope("resp", map[string]string{})
	metrics.MetronomeErrors = tally.NewTestScope("errors", map[string]string{})
	metrics.MetronomeLatency = tally.NewTestScope("latency", map[string]string{})
	metrics.MetronomeEvents = tally.NewTestScope("latency", map[string]string{})
}
