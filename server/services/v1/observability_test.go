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
		Db:         "",
		Collection: "",
		From:       1,
		To:         10,
		MetricName: "requests_count_ok",
	}
	formedQuery, err := formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{*}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:         "db1",
		Collection: "",
		From:       1,
		To:         10,
		MetricName: "requests_count_ok",
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1}.as_rate()", formedQuery)

	req = &api.QueryTimeSeriesMetricsRequest{
		Db:         "db1",
		Collection: "col1",
		From:       1,
		To:         10,
		MetricName: "requests_count_ok",
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1,collection:col1}.as_rate()", formedQuery)

	ctx = context.WithValue(ctx, request.RequestMetadataCtxKey{}, &request.RequestMetadata{})
	ctx = request.SetNamespace(ctx, "test-namespace")
	req = &api.QueryTimeSeriesMetricsRequest{
		Db:         "db1",
		Collection: "col1",
		From:       1,
		To:         10,
		MetricName: "requests_count_ok",
	}
	formedQuery, err = formQuery(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "sum:requests_count_ok.count{db:db1,collection:col1,tigris_tenant:test-namespace}.as_rate()", formedQuery)
}
