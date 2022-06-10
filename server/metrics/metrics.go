package metrics

import (
	"io"
	"time"

	prom "github.com/m3db/prometheus_client_golang/prometheus"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
)

var (
	Root     tally.Scope
	Reporter promreporter.Reporter
)

func InitializeMetrics() io.Closer {
	var closer io.Closer
	registry := prom.NewRegistry()
	Reporter = promreporter.NewReporter(promreporter.Options{Registerer: registry})
	Root, closer = tally.NewRootScope(tally.ScopeOptions{
		Prefix:         "tigris",
		Tags:           map[string]string{},
		CachedReporter: Reporter,
		Separator:      promreporter.DefaultSeparator,
	}, 1*time.Second)
	return closer
}
