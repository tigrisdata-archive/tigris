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
	"net/http"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/deepmap/oapi-codegen/pkg/types"
	"github.com/google/uuid"
	biller "github.com/tigrisdata/metronome-go-client"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/uber-go/tally"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Metronome struct {
	Config              config.Metronome
	client              *biller.ClientWithResponses
	billedMetricsByName map[string]uuid.UUID
	billedMetricsById   map[uuid.UUID]string
}

func NewMetronomeProvider(conf config.Metronome) (*Metronome, error) {
	bearerTokenProvider, err := securityprovider.NewSecurityProviderBearerToken(conf.ApiKey)
	if err != nil {
		return nil, err
	}
	client, err := biller.NewClientWithResponses(conf.URL, biller.WithRequestEditorFn(bearerTokenProvider.Intercept))
	if err != nil {
		return nil, err
	}
	// billable metrics should be uuids
	bm, bu := map[string]uuid.UUID{}, map[uuid.UUID]string{}
	for name, id := range conf.BilledMetrics {
		uid, err := uuid.Parse(id)
		if err != nil {
			return nil, err
		}
		bm[name] = uid
		bu[uid] = name
	}
	return &Metronome{Config: conf, client: client, billedMetricsByName: bm, billedMetricsById: bu}, nil
}

func (*Metronome) measure(ctx context.Context, scope tally.Scope, operation string, f func(ctx context.Context) (*http.Response, error)) {
	me := metrics.NewMeasurement(
		metrics.MetronomeServiceName,
		operation,
		metrics.MetronomeSpanType,
		map[string]string{})
	me.StartTracing(ctx, true)

	resp, err := f(ctx)
	if resp != nil {
		defer resp.Body.Close()
		// e.g.:- metronome_create_account_request
		// tags: response_code: 200
		me.IncrementCount(scope, metrics.GetResponseCodeTags(resp.StatusCode), "request", 1)
	}

	availability := int64(100)
	var errTags map[string]string
	if err != nil {
		availability = 0
		errTags = metrics.GetErrorCodeTags(err)
	}
	// e.g.:- metronome_create_account_availability
	// tags: error_value: err.Error()
	me.IncrementCount(scope, errTags, "availability", availability)
	_ = me.FinishTracing(ctx)
	me.RecordDuration(scope, me.GetTags())
}

func (m *Metronome) CreateAccount(ctx context.Context, namespaceId string, name string) (AccountId, error) {
	var (
		resp *biller.CreateCustomerResponse
		err  error
	)

	body := biller.CreateCustomerJSONRequestBody{
		IngestAliases: &[]string{namespaceId},
		Name:          name,
	}

	m.measure(ctx, metrics.MetronomeCreateAccount, "create_account", func(ctx context.Context) (*http.Response, error) {
		resp, err = m.client.CreateCustomerWithResponse(ctx, body)
		if resp == nil {
			return nil, err
		}
		return resp.HTTPResponse, err
	})

	if err != nil {
		return uuid.Nil, err
	}

	if resp.JSON200 == nil {
		return uuid.Nil, NewMetronomeError(resp.StatusCode(), resp.Body)
	}

	return resp.JSON200.Data.Id, nil
}

func (m *Metronome) AddDefaultPlan(ctx context.Context, accountId AccountId) (bool, error) {
	planId, err := uuid.Parse(m.Config.DefaultPlan)
	if err != nil {
		return false, err
	}
	return m.AddPlan(ctx, accountId, planId)
}

func (m *Metronome) AddPlan(ctx context.Context, accountId AccountId, planId uuid.UUID) (bool, error) {
	var (
		resp *biller.AddPlanToCustomerResponse
		err  error
	)
	body := biller.AddPlanToCustomerJSONRequestBody{
		PlanId: planId,
		// plans can only start at UTC midnight, so we either +1 or -1 from current day
		StartingOn: pastMidnight(),
	}

	m.measure(ctx, metrics.MetronomeAddPlan, "add_plan", func(ctx context.Context) (*http.Response, error) {
		resp, err = m.client.AddPlanToCustomerWithResponse(ctx, accountId, body)
		if resp == nil {
			return nil, err
		}
		return resp.HTTPResponse, err
	})

	if err != nil {
		return false, err
	}

	if resp.JSON200 == nil {
		return false, NewMetronomeError(resp.StatusCode(), resp.Body)
	}

	return true, nil
}

func (m *Metronome) PushUsageEvents(ctx context.Context, events []*UsageEvent) error {
	var billingEvents []biller.Event
	for _, se := range events {
		if se != nil && se.Properties != nil && len(*se.Properties) > 0 {
			billingEvents = append(billingEvents, se.Event)
		}
	}

	me := metrics.NewMeasurement(
		metrics.MetronomeServiceName,
		"ingest",
		metrics.MetronomeSpanType,
		metrics.GetIngestEventTags("usage"))
	me.IncrementCount(metrics.MetronomeIngest, me.GetTags(), "events", int64(len(billingEvents)))

	return m.pushBillingEvents(ctx, billingEvents)
}

func (m *Metronome) PushStorageEvents(ctx context.Context, events []*StorageEvent) error {
	var billingEvents []biller.Event
	for _, se := range events {
		if se != nil && se.Properties != nil && len(*se.Properties) > 0 {
			billingEvents = append(billingEvents, se.Event)
		}
	}
	me := metrics.NewMeasurement(
		metrics.MetronomeServiceName,
		"ingest",
		metrics.MetronomeSpanType,
		metrics.GetIngestEventTags("storage"))
	me.IncrementCount(metrics.MetronomeIngest, me.GetTags(), "events", int64(len(billingEvents)))

	return m.pushBillingEvents(ctx, billingEvents)
}

func (m *Metronome) pushBillingEvents(ctx context.Context, events []biller.Event) error {
	var (
		resp *biller.IngestResponse
		err  error
	)

	if len(events) == 0 {
		return nil
	}

	// todo: let page size be a const
	// batching events for better throughput
	pageSize := 100
	pages := len(events) / pageSize
	if len(events)%pageSize > 0 {
		pages++
	}

	for p := 0; p < pages; p++ {
		high := (p + 1) * pageSize
		if high > len(events) {
			high = len(events)
		}

		page := events[p*pageSize : high]

		// content encoding - gzip?
		m.measure(ctx, metrics.MetronomeIngest, "ingest", func(ctx context.Context) (*http.Response, error) {
			resp, err = m.client.IngestWithResponse(ctx, page)
			if resp == nil {
				return nil, err
			}
			return resp.HTTPResponse, err
		})
		if err != nil {
			return err
		}

		if resp.StatusCode() != http.StatusOK {
			return NewMetronomeError(resp.StatusCode(), resp.Body)
		}
	}
	return nil
}

func (m *Metronome) GetInvoices(ctx context.Context, accountId AccountId, r *api.ListInvoicesRequest) (*api.ListInvoicesResponse, error) {
	var (
		resp *biller.ListInvoicesResponse
		err  error
	)
	pageLimit := 20 // default
	if r.GetPageSize() != 0 {
		pageLimit = int(r.GetPageSize())
	}
	params := &biller.ListInvoicesParams{
		Limit: &pageLimit,
	}
	if len(r.GetNextPage()) > 0 {
		np := r.GetNextPage()
		params.NextPage = &np
	}

	if r.GetStartingOn() != nil {
		t := r.GetStartingOn().AsTime()
		params.StartingOn = &t
	}

	if r.GetEndingBefore() != nil {
		t := r.GetEndingBefore().AsTime()
		params.EndingBefore = &t
	}

	m.measure(ctx, metrics.MetronomeListInvoices, "list_invoices", func(ctx context.Context) (*http.Response, error) {
		resp, err = m.client.ListInvoicesWithResponse(ctx, accountId, params)
		if resp == nil {
			return nil, err
		}
		return resp.HTTPResponse, err
	})
	if err != nil {
		return nil, err
	}

	if resp.JSON200 == nil {
		return nil, NewMetronomeError(resp.StatusCode(), resp.Body)
	}

	invoices := make([]*api.Invoice, len(resp.JSON200.Data))
	for i, data := range resp.JSON200.Data {
		invoices[i] = buildInvoice(data)
	}

	return &api.ListInvoicesResponse{
		Data:     invoices,
		NextPage: resp.JSON200.NextPage,
	}, nil
}

func (m *Metronome) GetInvoiceById(ctx context.Context, accountId AccountId, invoiceId string) (*api.ListInvoicesResponse, error) {
	var (
		resp *biller.GetInvoiceResponse
		err  error
	)

	invoiceUUID, err := uuid.Parse(invoiceId)
	if err != nil {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "invoiceId is not valid - %s", err.Error())
	}
	m.measure(ctx, metrics.MetronomeGetInvoice, "get_invoice", func(ctx context.Context) (*http.Response, error) {
		resp, err = m.client.GetInvoiceWithResponse(ctx, accountId, invoiceUUID)
		if resp == nil {
			return nil, err
		}
		return resp.HTTPResponse, err
	})

	if err != nil {
		return nil, err
	}

	if resp.JSON200 == nil {
		return nil, NewMetronomeError(resp.StatusCode(), resp.Body)
	}

	data := make([]*api.Invoice, 0, 1)
	if b := buildInvoice(resp.JSON200.Data); b != nil {
		data = append(data, b)
	}
	return &api.ListInvoicesResponse{
		Data: data,
	}, nil
}

func (m *Metronome) GetUsage(ctx context.Context, id AccountId, r *UsageRequest) (*UsageAggregate, error) {
	var (
		resp *biller.GetUsageBatchResponse
		err  error
	)
	if r.StartTime == nil || r.StartTime.IsZero() {
		return nil, errors.InvalidArgument("'%s' is not a valid start time", r.StartTime)
	}
	if r.EndTime == nil || r.EndTime.IsZero() {
		return nil, errors.InvalidArgument("'%s' is not a valid end time", r.EndTime)
	}

	type mBillableMetric struct {
		GroupBy *struct {
			Key    string    `json:"key"`
			Values *[]string `json:"values,omitempty"`
		} `json:"group_by,omitempty"`
		Id types.UUID `json:"id"`
	}

	reqParams := biller.GetUsageBatchJSONRequestBody{
		BillableMetrics: (*[]struct {
			GroupBy *struct {
				Key    string    `json:"key"`
				Values *[]string `json:"values,omitempty"`
			} `json:"group_by,omitempty"`
			Id types.UUID `json:"id"`
		})(&[]struct {
			GroupBy *struct {
				Key    string
				Values *[]string
			}
			Id uuid.UUID
		}{}),
		CustomerIds:  &[]AccountId{id},
		StartingOn:   *r.StartTime,
		EndingBefore: *r.EndTime,
		WindowSize:   "hour",
	}

	l := reqParams.BillableMetrics
	if r.BillableMetric == nil || len(*r.BillableMetric) == 0 {
		// query all metrics
		for _, bid := range m.billedMetricsByName {
			*l = append(*l, mBillableMetric{Id: bid})
		}
	} else {
		for _, name := range *r.BillableMetric {
			id, ok := m.billedMetricsByName[name]
			if !ok {
				return nil, errors.InvalidArgument("'%s' is not a valid billable metric", name)
			}

			*l = append(*l, mBillableMetric{Id: id})
		}
	}

	m.measure(ctx, metrics.MetronomeGetUsage, "get_usage", func(ctx context.Context) (*http.Response, error) {
		resp, err = m.client.GetUsageBatchWithResponse(ctx, &biller.GetUsageBatchParams{NextPage: r.NextPage}, reqParams)
		if resp == nil {
			return nil, err
		}
		return resp.HTTPResponse, err
	})

	if err != nil {
		return nil, err
	}

	if resp.JSON200 == nil {
		return nil, NewMetronomeError(resp.StatusCode(), resp.Body)
	}

	aggUsage := map[string][]*Usage{}
	for _, param := range *reqParams.BillableMetrics {
		n := m.billedMetricsById[param.Id]
		aggUsage[n] = []*Usage{}
	}

	for _, d := range resp.JSON200.Data {
		if n, ok := m.billedMetricsById[d.BillableMetricId]; ok {
			aggUsage[n] = append(aggUsage[n], &Usage{
				StartTime: d.StartTimestamp,
				EndTime:   d.EndTimestamp,
				Value:     d.Value,
			})
		}
	}

	return &UsageAggregate{
		Data:     aggUsage,
		NextPage: resp.JSON200.NextPage,
	}, nil
}

func buildInvoice(input biller.Invoice) *api.Invoice {
	if input.Id == uuid.Nil {
		return nil
	}
	built := &api.Invoice{
		Id:        input.Id.String(),
		Entries:   make([]*api.InvoiceLineItem, len(input.LineItems)),
		StartTime: timestamppb.New(input.StartTimestamp),
		EndTime:   timestamppb.New(input.EndTimestamp),
		Subtotal:  input.Subtotal,
		Total:     input.Total,
		PlanName:  *input.PlanName,
	}
	for i, li := range input.LineItems {
		built.Entries[i] = &api.InvoiceLineItem{
			Name:     li.Name,
			Quantity: li.Quantity,
			Total:    li.Total,
			Charges:  make([]*api.Charge, len(li.SubLineItems)),
		}

		for j, sub := range li.SubLineItems {
			built.Entries[i].Charges[j] = &api.Charge{
				Name:     sub.Name,
				Quantity: sub.Quantity,
				Subtotal: sub.Subtotal,
				Tiers:    []*api.ChargeTier{},
			}

			if sub.Tiers != nil {
				for _, t := range *sub.Tiers {
					built.Entries[i].Charges[j].Tiers = append(
						built.Entries[i].Charges[j].Tiers,
						&api.ChargeTier{
							StartingAt: t.StartingAt,
							Quantity:   t.Quantity,
							Price:      t.Price,
							Subtotal:   t.Subtotal,
						},
					)
				}
			}
		}
	}

	return built
}

func pastMidnight() time.Time {
	now := time.Now().UTC()
	yyyy, mm, dd := now.Date()
	return time.Date(yyyy, mm, dd, 0, 0, 0, 0, time.UTC)
}

type MetronomeError struct {
	HttpCode int
	Message  string
}

func (e *MetronomeError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.HttpCode, e.Message)
}

func NewMetronomeError(code int, message []byte) *MetronomeError {
	return &MetronomeError{
		HttpCode: code,
		Message:  string(message),
	}
}
