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

	"github.com/tigrisdata/tigris/server/config"
)

type Provider interface {
	CreateAccount(ctx context.Context, namespaceId string, name string) (string, error)
	AddDefaultPlan(ctx context.Context, metronomeId string) (bool, error)
	AddPlan(ctx context.Context, metronomeId string, planId string) (bool, error)
}

func NewProvider() Provider {
	if config.DefaultConfig.Billing.Metronome.Enabled {
		return &Metronome{Config: config.DefaultConfig.Billing.Metronome}
	}
	return &noop{}
}
