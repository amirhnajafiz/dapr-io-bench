// main.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/amirtkz/dapr-overhead-bench/internal/config"
	"github.com/amirtkz/dapr-overhead-bench/internal/connector"
	"github.com/amirtkz/dapr-overhead-bench/internal/metrics"
	"github.com/amirtkz/dapr-overhead-bench/internal/sweep"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[cmd-bench] ")
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// one workload for every connector: the sweep resizes its payload per step
	workload := connector.NewWorkload(cfg.Plan.Baseline.PayloadBytes, cfg.Plan.Keyspace)

	// a signal ends the sweep cleanly, letting connectors close and the trace
	// file keep every step completed so far
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// the exporter comes up before any connecting happens, so Prometheus can
	// scrape bench_connector_up even when a backend is unreachable
	go func() {
		log.Printf("metrics exporter listening on %s/metrics", cfg.MetricsAddr)
		if err := metrics.Serve(cfg.MetricsAddr); err != nil {
			log.Fatalf("metrics exporter stopped: %v", err)
		}
	}()

	var candidates []connector.Connector
	for _, name := range cfg.Connectors {
		c, err := connector.New(name, cfg.Endpoints, workload)
		if err != nil {
			log.Fatalf("config: %v", err)
		}
		candidates = append(candidates, c)
	}

	connected := connect(ctx, candidates)
	if len(connected) == 0 {
		log.Fatal("no connector could be established; check the backing services")
	}
	defer func() {
		for _, c := range connected {
			if err := c.Close(); err != nil {
				log.Printf("close %s: %v", connector.Name(c), err)
			}
		}
	}()

	path, err := sweep.Run(ctx, connected, workload, cfg.Plan, cfg.ResultsDir)
	if err != nil {
		log.Printf("sweep stopped early: %v (steps completed so far are in %s)", err, path)
		return
	}
	log.Printf("sweep finished; trace written to %s", path)
}

// connect brings up each connector, retrying to absorb the slow start of the
// backends and the sidecar. A connector that never comes up is reported as down
// and skipped rather than aborting the whole comparison.
func connect(ctx context.Context, candidates []connector.Connector) []connector.Connector {
	// backends start alongside the bench rather than ahead of it, so give them
	// a minute to come up
	const attempts = 30

	var connected []connector.Connector
	for _, c := range candidates {
		name := connector.Name(c)
		var err error
		for attempt := 1; attempt <= attempts; attempt++ {
			if err = c.Connect(ctx); err == nil {
				break
			}

			log.Printf("connect %s (attempt %d/%d): %v", name, attempt, attempts, err)

			select {
			case <-ctx.Done():
				return connected
			case <-time.After(2 * time.Second):
			}
		}

		if err != nil {
			log.Printf("giving up on %s: %v", name, err)
			metrics.Up.WithLabelValues(c.Backend(), c.Mode()).Set(0)
			continue
		}

		log.Printf("connected %s", name)
		metrics.Up.WithLabelValues(c.Backend(), c.Mode()).Set(1)
		connected = append(connected, c)
	}

	return connected
}
