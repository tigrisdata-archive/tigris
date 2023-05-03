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

package database

import (
	"context"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	metadata2 "github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"google.golang.org/grpc/metadata"
)

type consumer interface {
	consume(r *api.ReadResponse) error
	done()
}

type StreamProducer struct {
	consumer consumer
	base     *BaseQueryRunner
	tenant   *metadata2.Tenant
}

func NewStreamer(_ context.Context, tenant *metadata2.Tenant, base *BaseQueryRunner, consumer consumer) *StreamProducer {
	return &StreamProducer{
		tenant:   tenant,
		base:     base,
		consumer: consumer,
	}
}

func (c *StreamProducer) SetHeader(_ metadata.MD) error {
	return nil
}

func (c *StreamProducer) SendHeader(_ metadata.MD) error {
	return nil
}

func (c *StreamProducer) SetTrailer(_ metadata.MD) {}

func (c *StreamProducer) Context() context.Context {
	return context.TODO()
}

func (c *StreamProducer) SendMsg(_ interface{}) error {
	return nil
}

func (c *StreamProducer) RecvMsg(_ interface{}) error {
	return nil
}

func (c *StreamProducer) Stream(req *api.ReadRequest) {
	streamingRunner := &StreamingQueryRunner{
		req:             req,
		streaming:       c,
		BaseQueryRunner: c.base,
		queryMetrics:    &metrics.StreamingQueryMetrics{},
	}

	go func() {
		defer c.consumer.done()

		_, _, err := streamingRunner.ReadOnly(context.Background(), c.tenant)
		if err != nil {
			// resume it based on last point
			log.Error().Err(err).Msgf("Completed building of search index with an error '%v'", err)
			return
		}
		log.Info().Msgf("Successfully completed building of search index for '%s'", streamingRunner.req.Collection)
	}()
}

func (c *StreamProducer) Send(r *api.ReadResponse) error {
	return c.consumer.consume(r)
}
