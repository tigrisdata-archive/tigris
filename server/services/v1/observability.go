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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/grpc"
)

const (
	observabilityPattern     = "/observability/*"
	AcceptHeader             = "Accept"
	ApplicationJsonHeaderVal = "application/json"
	DDApiKey                 = "DD-API-KEY"
	DDAppKey                 = "DD-APPLICATION-KEY"
	Query                    = "query"
	DDQueryEndpointPath      = "/api/v1/query"
)

type observabilityService struct {
	api.UnimplementedObservabilityServer
	ObservableProvider
}

type ObservableProvider interface {
	QueryTimeSeriesMetrics(ctx context.Context, request *api.QueryTimeSeriesMetricsRequest) (*api.QueryTimeSeriesMetricsResponse, error)
}

type Datadog struct {
	config.ObservabilityConfig
}

type MetricsQueryResp struct {
	FromDate int64                    `json:"from_date"`
	ToDate   int64                    `json:"to_date"`
	Query    string                   `json:"query"`
	Series   []MetricsQueryRespSeries `json:"series"`
}

type MetricsQueryRespSeries struct {
	Metric   string `json:"metric"`
	FromDate int64  `json:"start"`
	ToDate   int64  `json:"end"`
	Scope    string `json:"scope"`

	PointList [][2]float64 `json:"pointlist"`
}

func (dd Datadog) QueryTimeSeriesMetrics(ctx context.Context, req *api.QueryTimeSeriesMetricsRequest) (*api.QueryTimeSeriesMetricsResponse, error) {
	err := validateQueryTimeSeriesMetricsRequest(req)
	if err != nil {
		return nil, err
	}

	ddReq, err := http.NewRequest(http.MethodGet, dd.ObservabilityConfig.ProviderUrl+DDQueryEndpointPath, nil)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to query metrics: reason = "+err.Error())
	}

	q := ddReq.URL.Query()
	q.Add("from", strconv.FormatInt(req.From, 10))
	q.Add("to", strconv.FormatInt(req.To, 10))
	ddQuery, err := formQuery(ctx, req)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to query metrics: reason = "+err.Error())
	}
	q.Add("query", ddQuery)
	ddReq.URL.RawQuery = q.Encode()
	ddReq.Header.Add(AcceptHeader, ApplicationJsonHeaderVal)
	ddReq.Header.Add(DDApiKey, config.DefaultConfig.Observability.ApiKey)
	ddReq.Header.Add(DDAppKey, config.DefaultConfig.Observability.AppKey)
	httpClient := &http.Client{}

	resp, err := httpClient.Do(ddReq)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to query metrics: reason = "+err.Error())
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "Failed to query metrics: reason = "+err.Error())
	}

	bodyStr := string(body)
	if resp.StatusCode == 200 {
		var ddResp MetricsQueryResp
		err = json.Unmarshal(body, &ddResp)
		result := api.QueryTimeSeriesMetricsResponse{
			From:  ddResp.FromDate,
			To:    ddResp.ToDate,
			Query: ddResp.Query,
		}
		result.Series = []*api.MetricSeries{}
		if err != nil {
			return nil, api.Errorf(api.Code_INTERNAL, "Failed to unmarshal remote response: reason = "+err.Error())
		}
		if len(ddResp.Series) > 0 {
			for _, series := range ddResp.Series {
				thisSeries := &api.MetricSeries{
					From:   series.FromDate,
					To:     series.ToDate,
					Metric: series.Metric,
					Scope:  series.Scope,
				}
				thisSeries.DataPoints = make([]*api.DataPoint, len(series.PointList))
				for i := range series.PointList {
					thisSeries.DataPoints[i] = &api.DataPoint{}
					thisSeries.DataPoints[i].Timestamp = int64(series.PointList[i][0])
					thisSeries.DataPoints[i].Value = series.PointList[i][1]
				}
				result.Series = append(result.Series, thisSeries)
			}
			return &result, nil
		} else {
			return nil, api.Errorf(api.Code_INTERNAL, "Unexpected remote response: reason = 0 series returned")
		}
	}
	log.Error().Msgf("Datadog response status code=%d", resp.StatusCode)
	return nil, api.Errorf(api.Code_INTERNAL, "Failed to get query metrics: reason = "+bodyStr)
}

func newObservabilityService() *observabilityService {

	if config.DefaultConfig.Observability.Provider == "datadog" {
		return &observabilityService{
			UnimplementedObservabilityServer: api.UnimplementedObservabilityServer{},
			ObservableProvider: &Datadog{
				ObservabilityConfig: config.DefaultConfig.Observability,
			},
		}
	}
	if config.DefaultConfig.Observability.Enabled {
		log.Error().Str("observabilityProvider", config.DefaultConfig.Observability.Provider).Msg("Unable to configure external observability provider")
		panic("Unable to configure external observability provider")
	}
	return nil
}

func (o *observabilityService) QueryTimeSeriesMetrics(ctx context.Context, req *api.QueryTimeSeriesMetricsRequest) (*api.QueryTimeSeriesMetricsResponse, error) {
	return o.ObservableProvider.QueryTimeSeriesMetrics(ctx, req)
}

func (o *observabilityService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
	)
	if err := api.RegisterObservabilityHandlerClient(context.TODO(), mux, api.NewObservabilityClient(inproc)); err != nil {
		return err
	}

	api.RegisterObservabilityServer(inproc, o)
	router.HandleFunc(observabilityPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	return nil
}

func (o *observabilityService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterObservabilityServer(grpc, o)
	return nil
}

func formQuery(ctx context.Context, req *api.QueryTimeSeriesMetricsRequest) (string, error) {
	// final version examples:
	// sum:tigris.requests_count_ok.count{db:ycsb_tigris,collection:user_tables}.as_rate()
	// sum:tigris.requests_count_ok.count{db:ycsb_tigris,tigris_tenant:default_namespace} by {db,collection}.as_rate()
	ddQuery := fmt.Sprintf("%s:%s", strings.ToLower(req.SpaceAggregation.String()), req.MetricName)
	var tags []string

	if req.TigrisOperation != api.TigrisOperation_ALL {
		if req.TigrisOperation == api.TigrisOperation_WRITE {
			tags = append(tags, "grpc_method IN (insert,update) AND ")
		} else {
			tags = append(tags, "grpc_method:read,")
		}
	}

	if config.GetEnvironment() != "" {
		tags = append(tags, "env:"+config.GetEnvironment()+",")
	}

	if req.Db != "" {
		tags = append(tags, "db:"+req.Db+",")
	}
	if req.Collection != "" {
		tags = append(tags, "collection:"+req.Collection+",")
	}
	namespace, err := request.GetNamespace(ctx)
	if err == nil && namespace != "" {
		tags = append(tags, "tigris_tenant:"+namespace+",")
	}

	if req.Quantile != 0 {
		tags = append(tags, "quantile:"+strconv.FormatFloat(float64(req.Quantile), 'f', -1, 64)+",")
	}

	if len(tags) == 0 {
		ddQuery = fmt.Sprintf("%s{*}", ddQuery)
	} else {
		tagsQuery := ""
		for i := range tags {
			tagsQuery += tags[i]
		}
		tagsQuery = strings.TrimSuffix(tagsQuery, ",")
		tagsQuery = strings.TrimSuffix(tagsQuery, " AND ")

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

func isAllowedMetricQueryInput(tagValue string) bool {
	allowedPattern := regexp.MustCompile("^[a-zA-Z0-9_.]*$")
	return allowedPattern.MatchString(tagValue)
}

func validateQueryTimeSeriesMetricsRequest(req *api.QueryTimeSeriesMetricsRequest) error {
	if !isAllowedMetricQueryInput(req.MetricName) || !isAllowedMetricQueryInput(req.Db) || !isAllowedMetricQueryInput(req.Collection) {
		return api.Errorf(api.Code_PERMISSION_DENIED, "Failed to query metrics: reason = invalid character detected in the input")
	}
	for _, aggregationField := range req.SpaceAggregatedBy {
		if !isAllowedMetricQueryInput(aggregationField) {
			return api.Errorf(api.Code_PERMISSION_DENIED, "Failed to query metrics: reason = invalid character detected in SpaceAggregatedBy")
		}
	}
	if strings.Contains(req.MetricName, ":") {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "Failed to query metrics: reason = Metric name cannot contain :")
	}
	if !(req.Quantile == 0 || req.Quantile == 0.5 || req.Quantile == 0.75 || req.Quantile == 0.95 || req.Quantile == 0.99 || req.Quantile == 0.999) {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "Failed to query metrics: reason = allowed quantile values are [0.5, 0.75, 0.95, 0.99, 0.999]")
	}
	return nil
}
