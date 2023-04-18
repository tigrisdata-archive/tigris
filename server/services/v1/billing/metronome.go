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
	"strconv"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/google/uuid"
	biller "github.com/tigrisdata/metronome-go-client"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
)

const (
	TimeFormat = time.RFC3339
)

type MetronomeId = uuid.UUID

type Metronome struct {
	Config config.Metronome
	client *biller.ClientWithResponses
}

func NewMetronomeProvider(config config.Metronome) (*Metronome, error) {
	bearerTokenProvider, err := securityprovider.NewSecurityProviderBearerToken(config.ApiKey)
	if err != nil {
		return nil, err
	}
	client, err := biller.NewClientWithResponses(config.URL, biller.WithRequestEditorFn(bearerTokenProvider.Intercept))
	if err != nil {
		return nil, err
	}
	return &Metronome{Config: config, client: client}, nil
}

func (m *Metronome) measure(ctx context.Context, operation string, f func(ctx context.Context) (*http.Response, error)) {
	me := metrics.NewMeasurement(
		metrics.MetronomeServiceName,
		operation,
		metrics.MetronomeSpanType,
		metrics.GetMetronomeTags(operation))

	resp, err := f(ctx)
	me.RecordDuration(metrics.MetronomeLatency, me.GetTags())
	if resp != nil {
		defer resp.Body.Close()
		me.IncrementCount(metrics.MetronomeResponseCode, me.GetTags(), strconv.Itoa(resp.StatusCode), 1)
	}

	var errCount int64
	if err != nil {
		errCount = 1
	}
	me.IncrementCount(metrics.MetronomeErrors, me.GetTags(), "error", errCount)
}

func (m *Metronome) CreateAccount(ctx context.Context, namespaceId string, name string) (MetronomeId, error) {
	var (
		resp *biller.CreateCustomerResponse
		err  error
	)

	body := biller.CreateCustomerJSONRequestBody{
		IngestAliases: &[]string{namespaceId},
		Name:          name,
	}

	m.measure(ctx, "createAccount", func(ctx context.Context) (*http.Response, error) {
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
		return uuid.Nil, NewMetronomeError(resp.StatusCode(), string(resp.Body))
	}

	return resp.JSON200.Data.Id, nil
}

func (m *Metronome) AddDefaultPlan(ctx context.Context, accountId MetronomeId) (bool, error) {
	planId, err := uuid.Parse(m.Config.DefaultPlan)
	if err != nil {
		return false, err
	}
	return m.AddPlan(ctx, accountId, planId)
}

func (m *Metronome) AddPlan(ctx context.Context, accountId MetronomeId, planId uuid.UUID) (bool, error) {
	var (
		resp *biller.AddPlanToCustomerResponse
		err  error
	)
	body := biller.AddPlanToCustomerJSONRequestBody{
		PlanId: planId,
		// plans can only start at UTC midnight, so we either +1 or -1 from current day
		StartingOn: pastMidnight(),
	}

	m.measure(ctx, "addPlan", func(ctx context.Context) (*http.Response, error) {
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
		return false, NewMetronomeError(resp.StatusCode(), string(resp.Body))
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

	return m.pushBillingEvents(ctx, billingEvents)
}

func (m *Metronome) PushStorageEvents(ctx context.Context, events []*StorageEvent) error {
	var billingEvents []biller.Event
	for _, se := range events {
		if se != nil && se.Properties != nil && len(*se.Properties) > 0 {
			billingEvents = append(billingEvents, se.Event)
		}
	}
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
		pages += 1
	}

	for p := 0; p < pages; p++ {
		high := (p + 1) * pageSize
		if high > len(events) {
			high = len(events)
		}

		page := events[p*pageSize : high]

		// content encoding - gzip?
		m.measure(ctx, "ingestBillingMetric", func(ctx context.Context) (*http.Response, error) {
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
			return NewMetronomeError(resp.StatusCode(), string(resp.Body))
		}
	}
	return nil
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

func NewMetronomeError(code int, message string) *MetronomeError {
	return &MetronomeError{
		HttpCode: code,
		Message:  message,
	}
}
