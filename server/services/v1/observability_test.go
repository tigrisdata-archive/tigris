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
		Function:         api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1 AND collection:col1}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "db1",
		From:             1,
		To:               10,
		MetricName:       "tigris.requests_count_ok.count",
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_COUNT,
		AdditionalFunctions: []*api.AdditionalFunction{
			{
				Rollup: &api.RollupFunction{
					Aggregator: api.RollupAggregator_ROLLUP_AGGREGATOR_SUM,
					Interval:   604_800,
				},
			},
		},
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:tigris.requests_count_ok.count{db:db1}.as_count().rollup(sum, 604800)", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:                "db1",
		Collection:        "col1",
		From:              1,
		To:                10,
		MetricName:        "requests_count_ok.count",
		SpaceAggregatedBy: []string{"db,collection"},
		SpaceAggregation:  api.MetricQuerySpaceAggregation_SUM,
		Function:          api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1 AND collection:col1} by {db,collection}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:                "db1",
		Collection:        "col1",
		From:              1,
		To:                10,
		MetricName:        "requests_count_ok.count",
		TigrisOperation:   api.TigrisOperation_ALL,
		SpaceAggregatedBy: []string{"db,collection"},
		SpaceAggregation:  api.MetricQuerySpaceAggregation_SUM,
		Function:          api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1 AND collection:col1} by {db,collection}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:                "db1",
		Collection:        "col1",
		From:              1,
		To:                10,
		MetricName:        "requests_count_ok.count",
		TigrisOperation:   api.TigrisOperation_READ,
		SpaceAggregatedBy: []string{"db,collection"},
		SpaceAggregation:  api.MetricQuerySpaceAggregation_SUM,
		Function:          api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{grpc_method IN (read,search,subscribe) AND db:db1 AND collection:col1} by {db,collection}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:                "db1",
		Collection:        "col1",
		From:              1,
		To:                10,
		MetricName:        "requests_count_ok.count",
		TigrisOperation:   api.TigrisOperation_WRITE,
		SpaceAggregatedBy: []string{"db,collection"},
		SpaceAggregation:  api.MetricQuerySpaceAggregation_SUM,
		Function:          api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{grpc_method IN (insert,update,delete,replace,publish) AND db:db1 AND collection:col1} by {db,collection}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		TigrisOperation:  api.TigrisOperation_WRITE,
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{grpc_method IN (insert,update,delete,replace,publish)}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		TigrisOperation:  api.TigrisOperation_READ,
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{grpc_method IN (read,search,subscribe)}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		TigrisOperation:  api.TigrisOperation_METADATA,
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{grpc_method IN (createorupdatecollection,dropcollection,listdatabases,listcollections,createdatabase,dropdatabase,describedatabase,describecollection)}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "db1",
		Collection:       "col1",
		From:             1,
		To:               10,
		MetricName:       "tigris.requests_response_time.quantile",
		SpaceAggregation: api.MetricQuerySpaceAggregation_AVG,
		Function:         api.MetricQueryFunction_NONE,
		Quantile:         0.5,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "avg:tigris.requests_response_time.quantile{db:db1 AND collection:col1 AND quantile:0.5}", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "db1",
		Collection:       "col1",
		From:             1,
		To:               10,
		MetricName:       "tigris.requests_response_time.quantile",
		SpaceAggregation: api.MetricQuerySpaceAggregation_AVG,
		Function:         api.MetricQueryFunction_NONE,
		Quantile:         0.999,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "avg:tigris.requests_response_time.quantile{db:db1 AND collection:col1 AND quantile:0.999}", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "db1",
		From:             1,
		To:               10,
		MetricName:       "tigris.size_db_bytes",
		SpaceAggregation: api.MetricQuerySpaceAggregation_MAX,
		Function:         api.MetricQueryFunction_NONE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "max:tigris.size_db_bytes{db:db1}", formedQuery)

	ctx = context.WithValue(ctx, request.RequestMetadataCtxKey{}, &request.RequestMetadata{})
	ctx = request.SetNamespace(ctx, "test-namespace")
	req = &api.QueryTimeSeriesMetricsRequest{
		Db:               "db1",
		Collection:       "col1",
		From:             1,
		To:               10,
		MetricName:       "requests_count_ok.count",
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		Function:         api.MetricQueryFunction_RATE,
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1 AND collection:col1 AND tigris_tenant:test-namespace}.as_rate()", formedQuery)
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
