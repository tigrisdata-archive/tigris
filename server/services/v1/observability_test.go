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
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/request"
)

func TestDatadogQueryFormation(t *testing.T) {
	ctx := context.TODO()
	req := &api.QueryTimeSeriesMetricsRequest{
		Db:               "",
		Collection:       "",
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_RATE,
	}
	formedQuery, err := formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{*}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "",
		Collection:       "",
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		SpaceAggregation: api.MetricQuerySpaceAggregation_AVG,
		Function:         api.MetricQueryFunction_COUNT,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "avg:requests_count_ok.count{*}.as_count()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "db1",
		Collection:       "",
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "db1",
		Collection:       "col1",
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_RATE}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1,collection:col1}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:                "db1",
		Collection:        "col1",
		From:              1,
		To:                10,
		MetricName:        "requests_count_ok.count",
		SpaceAggregatedBy: []string{"db,collection"},
		SpaceAggregation:  api.MetricQuerySpaceAggregation_SUM,
		Function:          api.MetricQueryFunction_RATE}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1,collection:col1} by {db,collection}.as_rate()", formedQuery)

	ctx = context.WithValue(ctx, request.RequestMetadataCtxKey{}, &request.RequestMetadata{})
	ctx = request.SetNamespace(ctx, "test-namespace")
	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "db1",
		Collection:       "col1",
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_RATE}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1,collection:col1,tigris_tenant:test-namespace}.as_rate()", formedQuery)
}

func TestDatadogQueryValidation(t *testing.T) {
	require.True(t, isAllowedMetricQueryInput("users"))
	require.True(t, isAllowedMetricQueryInput("user_db"))
	require.True(t, isAllowedMetricQueryInput("user_db_1"))
	require.True(t, isAllowedMetricQueryInput("requests_count_ok.count"))
	require.False(t, isAllowedMetricQueryInput("users:"))
	require.False(t, isAllowedMetricQueryInput("users "))
	require.False(t, isAllowedMetricQueryInput("users,foo:bar"))
}
