// Copyright 2022 Tigris Data, Inc.
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

package v1

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// QueryLifecycleFactory is responsible for returning queryLifecycle objects
type QueryLifecycleFactory struct{}

func NewQueryLifecycleFactory() *QueryLifecycleFactory {
	return &QueryLifecycleFactory{}
}

// Get will create and return a queryLifecycle object
func (f *QueryLifecycleFactory) Get() *queryLifecycle {
	return newQueryLifecycle()
}

// queryLifecycle manages the lifecycle of a query that it is handling. Single place that can be used to validate
// the query, authorize the query, or to log or emit metrics related to this query.
type queryLifecycle struct{}

func newQueryLifecycle() *queryLifecycle {
	return &queryLifecycle{}
}

func (q *queryLifecycle) run(ctx context.Context, req *Request) (*Response, error) {
	if req == nil {
		return nil, status.Errorf(codes.Internal, "empty request")
	}
	if req.queryRunner == nil {
		return nil, status.Errorf(codes.Internal, "query runner is missing")
	}

	return req.queryRunner.Run(ctx, req)
}
