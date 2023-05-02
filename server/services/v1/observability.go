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

package v1

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/quota"
	"github.com/tigrisdata/tigris/server/request"
	"github.com/tigrisdata/tigris/util"
	"google.golang.org/grpc"
)

const (
	observabilityPattern = "/" + version + "/observability/*"
)

type observabilityService struct {
	api.UnimplementedObservabilityServer
	Provider observableProvider
}

type observableProvider interface {
	QueryTimeSeriesMetrics(ctx context.Context, request *api.QueryTimeSeriesMetricsRequest) (*api.QueryTimeSeriesMetricsResponse, error)
	QueryQuotaUsage(ctx context.Context, request *api.QuotaUsageRequest) (*api.QuotaUsageResponse, error)
}

type Datadog struct {
	Tenants *metadata.TenantManager
	Datadog *metrics.Datadog
}

func (dd *Datadog) QueryTimeSeriesMetrics(ctx context.Context, req *api.QueryTimeSeriesMetricsRequest) (*api.QueryTimeSeriesMetricsResponse, error) {
	if err := validateQueryTimeSeriesMetricsRequest(req); err != nil {
		return nil, err
	}

	namespace, _ := request.GetNamespace(ctx)
	ddQuery, err := metrics.FormDatadogQuery(namespace, req)
	if err != nil {
		return nil, errors.Internal("Failed to query metrics: reason = " + err.Error())
	}

	ddResp, err := dd.Datadog.Query(ctx, req.From, req.To, ddQuery)
	if err != nil {
		return nil, errors.Internal("Failed to query metrics: reason = " + err.Error())
	}

	result := api.QueryTimeSeriesMetricsResponse{
		From:  ddResp.GetFromDate(),
		To:    ddResp.GetToDate(),
		Query: ddResp.GetQuery(),
	}
	result.Series = []*api.MetricSeries{}
	if err != nil {
		return nil, errors.Internal("Failed to unmarshal remote response: reason = " + err.Error())
	}

	if len(ddResp.Series) > 0 {
		for _, series := range ddResp.Series {
			thisSeries := &api.MetricSeries{
				From:   series.GetStart(),
				To:     series.GetEnd(),
				Metric: series.GetMetric(),
				Scope:  series.GetScope(),
			}
			thisSeries.DataPoints = make([]*api.DataPoint, len(series.GetPointlist()))
			for i, v := range series.GetPointlist() {
				thisSeries.DataPoints[i] = &api.DataPoint{}
				if len(v) < 2 || v[0] == nil || v[1] == nil {
					log.Debug().Msg("Malformed data point returned")
				} else {
					thisSeries.DataPoints[i].Timestamp = int64(*v[0])
					thisSeries.DataPoints[i].Value = *v[1]
				}
			}
			result.Series = append(result.Series, thisSeries)
		}
		return &result, nil
	}

	log.Debug().Msg("Unexpected remote response: reason = 0 series returned")
	return &result, nil
}

func (dd *Datadog) QueryQuotaUsage(ctx context.Context, _ *api.QuotaUsageRequest) (*api.QuotaUsageResponse, error) {
	ns, _ := request.GetNamespace(ctx)

	q := quota.Datadog{Datadog: dd.Datadog}
	ru, wu, err := q.CurRates(ctx, ns)
	if err != nil {
		return nil, errors.Internal("error reading quota usage")
	}

	tenant, err := dd.Tenants.GetTenant(ctx, ns)
	if err != nil {
		return nil, errors.Internal("error reading storage quota usage")
	}

	size, err := tenant.Size(ctx)
	if err != nil {
		return nil, errors.Internal("error reading storage quota usage")
	}

	rt, err := dd.Datadog.GetCurrentMetricValue(ctx, ns, "tigris.quota_throttled_read_units.count", api.TigrisOperation_ALL, quota.RunningAverageLength)
	if err != nil {
		return nil, errors.Internal("error reading quota usage")
	}

	wt, err := dd.Datadog.GetCurrentMetricValue(ctx, ns, "tigris.quota_throttled_write_units.count", api.TigrisOperation_ALL, quota.RunningAverageLength)
	if err != nil {
		return nil, errors.Internal("error reading quota usage")
	}

	st, err := dd.Datadog.GetCurrentMetricValue(ctx, ns, "tigris.quota_throttled_storage.count", api.TigrisOperation_ALL, quota.RunningAverageLength)
	if err != nil {
		return nil, errors.Internal("error reading quota usage")
	}

	return &api.QuotaUsageResponse{
		ReadUnits:            ru,
		WriteUnits:           wu,
		StorageSize:          size.StoredBytes,
		ReadUnitsThrottled:   rt,
		WriteUnitsThrottled:  wt,
		StorageSizeThrottled: st,
	}, nil
}

func newObservabilityService(tenants *metadata.TenantManager) *observabilityService {
	cfg := config.DefaultConfig.Observability

	log.Debug().Str("provider", cfg.Provider).Bool("enabled", cfg.Enabled).Str("url", cfg.ProviderUrl).Msg("Initializing observability service")

	if cfg.Provider == "datadog" {
		return &observabilityService{
			UnimplementedObservabilityServer: api.UnimplementedObservabilityServer{},
			Provider: &Datadog{
				Tenants: tenants,
				Datadog: metrics.InitDatadog(&config.DefaultConfig),
			},
		}
	}
	if cfg.Enabled {
		log.Error().Str("observabilityProvider", cfg.Provider).Msg("Unable to configure external observability provider")
		panic("Unable to configure external observability provider")
	}
	return nil
}

func (o *observabilityService) QueryTimeSeriesMetrics(ctx context.Context, req *api.QueryTimeSeriesMetricsRequest) (*api.QueryTimeSeriesMetricsResponse, error) {
	return o.Provider.QueryTimeSeriesMetrics(ctx, req)
}

func (o *observabilityService) QuotaLimits(ctx context.Context, _ *api.QuotaLimitsRequest) (*api.QuotaLimitsResponse, error) {
	ns, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, err
	}
	cfg := config.DefaultConfig.Quota.Namespace.NamespaceLimits(ns)
	return &api.QuotaLimitsResponse{
		ReadUnits:   int64(cfg.ReadUnits),
		WriteUnits:  int64(cfg.WriteUnits),
		StorageSize: config.DefaultConfig.Quota.Storage.NamespaceLimits(ns),
	}, nil
}

func (o *observabilityService) QuotaUsage(ctx context.Context, request *api.QuotaUsageRequest) (*api.QuotaUsageResponse, error) {
	return o.Provider.QueryQuotaUsage(ctx, request)
}

func (o *observabilityService) GetInfo(_ context.Context, _ *api.GetInfoRequest) (*api.GetInfoResponse, error) {
	return &api.GetInfoResponse{
		ServerVersion: util.Version,
	}, nil
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

func isAllowedMetricQueryInput(tagValue string) bool {
	allowedPattern := regexp.MustCompile("^[a-zA-Z]+[a-zA-Z0-9._-]+$")
	return allowedPattern.MatchString(tagValue)
}

func validateQueryTimeSeriesMetricsRequest(req *api.QueryTimeSeriesMetricsRequest) error {
	if !isAllowedMetricQueryInput(req.MetricName) || !isAllowedMetricQueryInput(req.Db) || !isAllowedMetricQueryInput(req.Collection) {
		log.Info().
			Str("metric_name", req.MetricName).
			Str("project", req.Db).
			Str("collection", req.Collection).
			Msg("Failed to query metrics: reason = invalid character detected in the input")
		return errors.PermissionDenied("Failed to query metrics: reason = invalid character detected in the input")
	}
	for _, aggregationField := range req.SpaceAggregatedBy {
		if !isAllowedMetricQueryInput(aggregationField) {
			log.Info().
				Str("metric_name", req.MetricName).
				Str("project", req.Db).
				Str("collection", req.Collection).
				Str("agg_field", aggregationField).
				Msg("Failed to query metrics: reason = invalid character detected in SpaceAggregatedBy")
			return errors.PermissionDenied("Failed to query metrics: reason = invalid character detected in SpaceAggregatedBy")
		}
	}
	if strings.Contains(req.MetricName, ":") {
		log.Info().
			Str("metric_name", req.MetricName).
			Str("project", req.Db).
			Str("collection", req.Collection).
			Msg("Failed to query metrics: reason = Metric name cannot contain :")
		return errors.InvalidArgument("Failed to query metrics: reason = Metric name cannot contain :")
	}
	if !(req.Quantile == 0 || req.Quantile == 0.5 || req.Quantile == 0.75 || req.Quantile == 0.95 || req.Quantile == 0.99 || req.Quantile == 0.999) {
		log.Info().
			Str("metric_name", req.MetricName).
			Str("project", req.Db).
			Str("collection", req.Collection).
			Float32("quantile", req.Quantile).
			Msg("Failed to query metrics: reason = allowed quantile values are [0.5, 0.75, 0.95, 0.99, 0.999]")
		return errors.InvalidArgument("Failed to query metrics: reason = allowed quantile values are [0.5, 0.75, 0.95, 0.99, 0.999]")
	}
	return nil
}
