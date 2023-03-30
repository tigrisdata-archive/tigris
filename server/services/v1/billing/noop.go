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

	"github.com/tigrisdata/tigris/errors"
)

type noop struct{}

func (n *noop) CreateAccount(_ context.Context, _ string, _ string) (string, error) {
	return "", errors.Unimplemented("billing not enabled on this server")
}

func (n *noop) AddDefaultPlan(ctx context.Context, id string) (bool, error) {
	return n.AddPlan(ctx, id, "")
}

func (*noop) AddPlan(ctx context.Context, metronomeId string, planId string) (bool, error) {
	return false, errors.Unimplemented("billing not enabled on this server")
}
