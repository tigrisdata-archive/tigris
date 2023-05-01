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

package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/api/v1/datadog"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	ulog "github.com/tigrisdata/tigris/util/log"
)

var (
	dDApiKey           = "DD-API-KEY" //nolint:gosec
	dDAppKey           = "DD-APPLICATION-KEY"
	rateLimitLimit     = "X-RateLimit-Period"
	rateLimitPeriod    = "X-RateLimit-Period"
	rateLimitRemaining = "X-RateLimit-Remaining"
	rateLimitReset     = "X-RateLimit-Reset"
	rateLimitName      = "X-RateLimit-Name"
)

type Datadog struct {
	apiClient *datadog.APIClient
	host      map[string]string
}

func InitDatadog(cfg *config.Config) *Datadog {
	d := Datadog{}
	c := datadog.NewConfiguration()
	c.AddDefaultHeader(dDApiKey, cfg.Observability.ApiKey)
	c.AddDefaultHeader(dDAppKey, cfg.Observability.AppKey)

	d.apiClient = datadog.NewAPIClient(c)
	d.host = map[string]string{"site": cfg.Observability.ProviderUrl}

	return &d
}

func (d *Datadog) Query(ctx context.Context, from int64, to int64, query string) (*datadog.MetricsQueryResponse, error) {
	ctx = context.WithValue(ctx, datadog.ContextServerVariables, d.host)

	resp, hResp, err := d.apiClient.MetricsApi.QueryMetrics(ctx, from, to, query)
	if ulog.E(err) {
		return nil, errors.Internal("Failed to query metrics: reason = " + err.Error())
	}
	defer func() { _ = hResp.Body.Close() }()

	if hResp.StatusCode == http.StatusTooManyRequests {
		log.Warn().Str(rateLimitLimit, hResp.Header.Get(rateLimitLimit)).
			Str(rateLimitPeriod, hResp.Header.Get(rateLimitPeriod)).
			Str(rateLimitRemaining, hResp.Header.Get(rateLimitRemaining)).
			Str(rateLimitReset, hResp.Header.Get(rateLimitReset)).
			Str(rateLimitName, hResp.Header.Get(rateLimitName)).
			Msgf("Datadog rate-limit hit")
		return nil, errors.ResourceExhausted("Failed to get query metrics: reason = rate-limited, reason = %s", resp.GetError())
	}

	if resp.HasError() {
		log.Error().Msgf("Datadog response status code=%d", hResp.StatusCode)
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to get query metrics: reason = "+resp.GetError())
	}

	return &resp, nil
}

func FormDatadogQuery(namespace string, req *api.QueryTimeSeriesMetricsRequest) (string, error) {
	return FormDatadogQueryNoMeta(namespace, false, req)
}

func FormDatadogQueryNoMeta(namespace string, noMeta bool, req *api.QueryTimeSeriesMetricsRequest) (string, error) {
	// final version examples:
	// sum:tigris.requests_count_ok.count{project:ycsb_tigris,collection:user_tables}.as_rate()
	// sum:tigris.requests_count_ok.count{project:ycsb_tigris,tigris_tenant:default_namespace} by {project,collection}.as_rate()
	ddQuery := fmt.Sprintf("%s:%s", strings.ToLower(req.SpaceAggregation.String()), req.MetricName)
	var tags []string

	switch {
	case req.TigrisOperation == api.TigrisOperation_WRITE:
		if noMeta {
			tags = append(tags, "grpc_method IN (createproject,deleteproject,createorupdatecollection,dropcollection,insert,update,delete,replace,publish)")
		} else {
			tags = append(tags, "grpc_method IN (insert,update,delete,replace,publish)")
		}
	case req.TigrisOperation == api.TigrisOperation_READ:
		if noMeta {
			tags = append(tags, "grpc_method IN (listprojects,listcollections,describeproject,describecollection, read,search,subscribe)")
		} else {
			tags = append(tags, "grpc_method IN (read,search,subscribe)")
		}
	case req.TigrisOperation == api.TigrisOperation_METADATA:
		tags = append(tags, "grpc_method IN (createorupdatecollection,dropcollection,listprojects,listcollections,createproject,deleteproject,describeproject,describecollection)")
	}

	if config.GetEnvironment() != "" {
		tags = append(tags, "env:"+config.GetEnvironment())
	}

	if req.Db != "" {
		tags = append(tags, "project:"+req.Db)
	}

	if req.GetBranch() != "" {
		tags = append(tags, "branch:"+req.GetBranch())
	}

	if req.Collection != "" {
		tags = append(tags, "collection:"+req.Collection)
	}

	if namespace != "" {
		tags = append(tags, "tigris_tenant:"+namespace)
	}

	if req.Quantile != 0 {
		tags = append(tags, "quantile:"+fmt.Sprintf("%.3g", req.Quantile))
	}

	if len(tags) == 0 {
		ddQuery = fmt.Sprintf("%s{*}", ddQuery)
	} else {
		tagsQuery := ""
		for i := range tags {
			if tagsQuery != "" {
				tagsQuery += " AND "
			}
			tagsQuery += tags[i]
		}

		ddQuery = fmt.Sprintf("%s{%s}", ddQuery, tagsQuery)
	}

	if len(req.SpaceAggregatedBy) > 0 {
		aggregationBy := "by {"
		for _, field := range req.SpaceAggregatedBy {
			aggregationBy = fmt.Sprintf("%s%s,", aggregationBy, field)
		}
		// remove trailing ,
		aggregationBy = aggregationBy[0 : len(aggregationBy)-1]
		aggregationBy = fmt.Sprintf("%s}", aggregationBy)
		ddQuery = fmt.Sprintf("%s %s", ddQuery, aggregationBy)
	}

	if req.Function != api.MetricQueryFunction_NONE {
		ddQuery = fmt.Sprintf("%s.as_%s()", ddQuery, strings.ToLower(req.Function.String()))
	}

	for _, additionalFunction := range req.AdditionalFunctions {
		if additionalFunction.Rollup != nil {
			ddQuery = fmt.Sprintf("%s.rollup(%s, %d)", ddQuery, convertToDDAggregatorFunc(additionalFunction.Rollup.Aggregator), additionalFunction.Rollup.Interval)
		}
	}

	return ddQuery, nil
}

func convertToDDAggregatorFunc(aggregator api.RollupAggregator) string {
	switch aggregator {
	case api.RollupAggregator_ROLLUP_AGGREGATOR_AVG:
		return "avg"
	case api.RollupAggregator_ROLLUP_AGGREGATOR_SUM:
		return "sum"
	case api.RollupAggregator_ROLLUP_AGGREGATOR_COUNT:
		return "count"
	case api.RollupAggregator_ROLLUP_AGGREGATOR_MIN:
		return "min"
	case api.RollupAggregator_ROLLUP_AGGREGATOR_MAX:
		return "max"
	}
	return ""
}

func (d *Datadog) GetCurrentMetricValue(ctx context.Context, namespace string, metric string, tp api.TigrisOperation, avgLength time.Duration) (int64, error) {
	to := time.Now()
	from := time.Now().Add(-avgLength)

	rateQuery := &api.QueryTimeSeriesMetricsRequest{
		TigrisOperation:  tp,
		SpaceAggregation: api.MetricQuerySpaceAggregation_SUM,
		MetricName:       metric,
		Function:         api.MetricQueryFunction_RATE,
	}

	q, err := FormDatadogQueryNoMeta(namespace, true, rateQuery)
	if err != nil {
		return 0, err
	}

	resp, err := d.Query(ctx, from.Unix(), to.Unix(), q)
	if err != nil {
		return 0, err
	}

	var sum, count int64
	if len(resp.GetSeries()) > 0 && len(resp.GetSeries()[0].Pointlist) > 0 {
		for _, v := range resp.GetSeries()[0].Pointlist {
			if len(v) > 1 && v[1] != nil {
				sum += int64(*v[1])
				count++
			}
		}
	}

	// in the case of error we are not penalizing the user
	// and eventually give maximum per node quota
	if count == 0 {
		return 0, nil
	}

	log.Debug().Int64("sum", sum).Int64("count", count).Int64("avg", sum/count).Msg("metric value")

	return sum / count, nil
}
