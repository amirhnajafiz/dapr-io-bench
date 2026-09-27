// metrics.go
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var labels = []string{"backend", "mode", "op"}

// Latency buckets are log-spaced from 100µs to 10s; stall buckets reach
// further because a saturated connector can hold a request back for the
// whole remainder of a step.
var (
	latencyBuckets = prometheus.ExponentialBucketsRange(0.0001, 10, 30)
	stallBuckets   = prometheus.ExponentialBucketsRange(0.0001, 120, 36)
)

var (
	Duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bench_op_duration_seconds",
		Help:    "Service latency of a single benchmarked operation, call to return.",
		Buckets: latencyBuckets,
	}, labels)
	Stall = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bench_op_stall_seconds",
		Help:    "Queueing delay before an operation could start: how long the caller waited for a free slot past its scheduled send time.",
		Buckets: stallBuckets,
	}, labels)
	Total = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bench_ops_total",
		Help: "Number of benchmarked operations attempted.",
	}, append(append([]string{}, labels...), "status"))
	// Carries the current sweep point as labels, so a scrape is self-describing:
	// whatever is being measured right now says which settings produced it.
	Step = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bench_step_info",
		Help: "The sweep point currently being measured (value is always 1).",
	}, []string{"sweep", "dimension", "connector", "rate", "payload_bytes", "concurrency"})
	Up = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bench_connector_up",
		Help: "1 if the connector connected successfully, 0 otherwise.",
	}, []string{"backend", "mode"})
)

func init() {
	prometheus.MustRegister(Duration, Stall, Total, Step, Up)
}

// Observe records one completed operation. err == nil is recorded as a success;
// failures are counted but their latency is still observed, since a slow failure
// is itself part of the overhead story.
func Observe(backend, mode, op string, latency, stall time.Duration, err error) {
	status := "ok"
	if err != nil {
		status = "error"
	}

	Duration.WithLabelValues(backend, mode, op).Observe(latency.Seconds())
	Stall.WithLabelValues(backend, mode, op).Observe(stall.Seconds())
	Total.WithLabelValues(backend, mode, op, status).Inc()
}

// SetStep publishes the point being measured, replacing the previous one.
func SetStep(sweep, dimension, connector, rate, payloadBytes, concurrency string) {
	Step.Reset()
	Step.WithLabelValues(sweep, dimension, connector, rate, payloadBytes, concurrency).Set(1)
}

// Serve starts the /metrics endpoint. It blocks until the server stops.
func Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// trivial liveness endpoint so compose can gate Prometheus on the app
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return http.ListenAndServe(addr, mux)
}
