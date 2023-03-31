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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"golang.org/x/net/context/ctxhttp"
)

const (
	ContentTypeJSON = "application/json"
	TimeFormat      = time.RFC3339
)

type Metronome struct {
	Config config.Metronome
}

func (m *Metronome) CreateAccount(ctx context.Context, namespaceId string, name string) (string, error) {
	payload := map[string]interface{}{
		"name":           name,
		"ingest_aliases": []string{namespaceId},
	}
	req, err := m.createRequest(ctx, "/customers", payload)
	if err != nil {
		return "", err
	}

	respBytes, err := m.executeRequest(ctx, req)
	if err != nil {
		return "", err
	}

	var respBody CreateAccountResponse
	err = jsoniter.Unmarshal(respBytes, &respBody)
	if err != nil {
		return "", err
	}

	return respBody.Data.Id, nil
}

type CreateAccountResponse struct {
	Data struct {
		Id string
	}
}

func (m *Metronome) AddDefaultPlan(ctx context.Context, metronomeId string) (bool, error) {
	return m.AddPlan(ctx, metronomeId, m.Config.DefaultPlan)
}

func (m *Metronome) AddPlan(ctx context.Context, metronomeId string, planId string) (bool, error) {
	payload := map[string]any{
		"plan_id": planId,
		// plans can only start at UTC midnight, so we either +1 or -1 from current day
		"starting_on": pastMidnight().Format(TimeFormat),
	}

	req, err := m.createRequest(ctx, fmt.Sprintf("/customers/%s/plans/add", metronomeId), payload)
	if err != nil {
		return false, err
	}

	_, err = m.executeRequest(ctx, req)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (m *Metronome) createRequest(ctx context.Context, path string, payload map[string]any) (*http.Request, error) {
	jsonPayload, err := jsoniter.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.Config.URL+path, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", ContentTypeJSON)
	req.Header.Set("Authorization", "Bearer "+m.Config.ApiKey)
	return req, nil
}

func (m *Metronome) executeRequest(ctx context.Context, req *http.Request) ([]byte, error) {
	resp, err := ctxhttp.Do(ctx, nil, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		errStr := string(respBytes)
		return nil, errors.Internal("metronome failure: '%s'", resp.Status+" - "+errStr)
	}

	if err != nil {
		return []byte{}, err
	}
	return respBytes, nil
}

func pastMidnight() time.Time {
	now := time.Now().UTC()
	yyyy, mm, dd := now.Date()
	return time.Date(yyyy, mm, dd, 0, 0, 0, 0, time.UTC)
}
