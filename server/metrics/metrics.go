package metrics

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	GRPCMetrics        = grpc_prometheus.NewServerMetrics()
	PrometheusRegistry = prometheus.NewRegistry()
)

func init() {
	GRPCMetrics.EnableHandlingTimeHistogram()
	PrometheusRegistry.MustRegister(GRPCMetrics)
}
