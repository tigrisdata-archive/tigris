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
	"strconv"

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
	FromDate int64 `json:"from_date"`
	ToDate   int64 `json:"to_date"`
	Query    string
	Series   [1]struct {
		PointList [][2]float64 `json:"pointlist"`
	}
}

func (dd Datadog) QueryTimeSeriesMetrics(ctx context.Context, req *api.QueryTimeSeriesMetricsRequest) (*api.QueryTimeSeriesMetricsResponse, error) {
	if !isAllowedMetric(req.MetricName, &config.DefaultConfig) {
		return nil, api.Errorf(api.Code_PERMISSION_DENIED, "Failed to query metrics: reason = not allowed to query metric: "+req.MetricName)
	}
	ddReq, err := http.NewRequest(http.MethodGet, dd.ObservabilityConfig.ProviderUrl+"/api/v1/query", nil)
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
		if err != nil {
			return nil, api.Errorf(api.Code_INTERNAL, "Failed to unmarshal remote response: reason = "+err.Error())
		}
		if len(ddResp.Series) > 0 {
			var dataPoints = make([]*api.DataPoint, len(ddResp.Series[0].PointList))
			for i := range ddResp.Series[0].PointList {
				dataPoints[i] = &api.DataPoint{}
				dataPoints[i].Timestamp = int64(ddResp.Series[0].PointList[i][0])
				dataPoints[i].Value = ddResp.Series[0].PointList[i][1]
			}
			queryTimeSeriesMetricsResponse := api.QueryTimeSeriesMetricsResponse{
				From:       ddResp.FromDate,
				To:         ddResp.ToDate,
				Query:      ddResp.Query,
				DataPoints: dataPoints,
			}

			return &queryTimeSeriesMetricsResponse, nil
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
	log.Error().Str("observabilityProvider", config.DefaultConfig.Observability.Provider).Msg("Unable to configure external observability provider")
	if config.DefaultConfig.Observability.EnableObservabilityService {
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
	// form query
	// final version example: sum:tigris.requests_count_ok.count{db:ycsb_tigris,collection:user_tables}.as_rate()
	// this step: sum:tigris.requests_count_ok.count
	ddQuery := fmt.Sprintf("sum:%s.count{", req.MetricName)
	if req.Db != "" {
		// this step: sum:tigris.requests_count_ok.count{db:ycsb_tigris
		ddQuery = fmt.Sprintf("%sdb:%s", ddQuery, req.Db)
	}
	if req.Collection != "" {
		// this step: sum:tigris.requests_count_ok.count{db:ycsb_tigris,collection:user_tables
		ddQuery = fmt.Sprintf("%s,collection:%s", ddQuery, req.Collection)
	}
	// this step: sum:tigris.requests_count_ok.count{db:ycsb_tigris,collection:user_tables,tigris_tenant:
	namespace, err := request.GetNamespace(ctx)
	if err == nil && namespace != "" {
		ddQuery = fmt.Sprintf("%s,tigris_tenant:%s", ddQuery, namespace)
	}

	if req.Db == "" && req.Collection == "" {
		// this step: sum:tigris.requests_count_ok.count{*}
		ddQuery = fmt.Sprintf("%s*}", ddQuery)
	} else {
		// this step: sum:tigris.requests_count_ok.count{db:ycsb_tigris,collection:user_tables}
		ddQuery = fmt.Sprintf("%s}", ddQuery)
	}
	return fmt.Sprintf("%s.as_rate()", ddQuery), nil
}
func isAllowedMetric(metricName string, config *config.Config) bool {
	for _, allowedMetric := range config.Observability.AllowedMetrics {
		if metricName == allowedMetric {
			return true
		}
	}
	return false
}
