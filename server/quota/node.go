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

package quota

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/server/config"
	"golang.org/x/time/rate"
)

// This limiter protects the node from overloading,
// It limits the total number of requests
// handled by the node, by all the namespaces.

type node struct {
	state *State
	cfg   *config.QuotaConfig
}

func (c *node) Allow(_ context.Context, namespace string, size int, isWrite bool) error {
	units := toUnits(size, isWrite)

	// Request size is bigger then entire node quota
	if units > c.cfg.Node.Limit(isWrite) {
		return ErrMaxRequestSizeExceeded
	}

	return c.getState(namespace).Allow(units, isWrite)
}

func (c *node) Wait(ctx context.Context, namespace string, size int, isWrite bool) error {
	units := toUnits(size, isWrite)

	// Request size is bigger then entire node quota
	if units > c.cfg.Node.Limit(isWrite) {
		return ErrMaxRequestSizeExceeded
	}

	return c.getState(namespace).Wait(ctx, units, isWrite)
}

func (*node) Cleanup() {}

func (c *node) getState(_ string) *State {
	return c.state
}

func initNode(cfg *config.QuotaConfig) *node {
	log.Debug().Msg("Initializing per node quota manager")

	is := &State{
		Write: Limiter{
			isWrite: true,
			Rate:    rate.NewLimiter(rate.Limit(cfg.Node.WriteUnits), cfg.Node.WriteUnits),
		},
		Read: Limiter{
			Rate: rate.NewLimiter(rate.Limit(cfg.Node.ReadUnits), cfg.Node.ReadUnits),
		},
	}

	return &node{cfg: cfg, state: is}
}
