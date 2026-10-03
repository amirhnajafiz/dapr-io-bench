// config.go
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/amirtkz/dapr-overhead-bench/internal/connector"
	"github.com/amirtkz/dapr-overhead-bench/internal/sweep"
)

// MaxConnectors bounds how many data paths one run may hold open. Keeping it
// at a direct/Dapr pair means the backend under test is only ever touched by
// the one comparison in progress, and the sidecar only ever serves one
// component's worth of traffic at a time.
const MaxConnectors = 2

// Config is the full set of knobs for a benchmark run.
type Config struct {
	Endpoints connector.Endpoints

	// Which data paths to compare, run one at a time at every point.
	Connectors []string

	// Load schedule.
	Plan sweep.Plan

	// Where the trace file goes.
	ResultsDir string

	// Prometheus exporter
	MetricsAddr string
}

// Load reads the configuration from the environment, falling back to values
// that work with the provided docker compose stack.
func Load() (Config, error) {
	cfg := Config{
		Endpoints: connector.Endpoints{
			NATSURL:     env("NATS_URL", "nats://nats:4222"),
			RedisAddr:   env("REDIS_ADDR", "redis:6379"),
			PostgresDSN: env("POSTGRES_DSN", "postgres://bench:bench@postgres:5432/bench?sslmode=disable"),
			FSDir:       env("FS_DIR", "/data/direct"),

			DaprGRPCPort:   env("DAPR_GRPC_PORT", "50001"),
			DaprPubsubNATS: env("DAPR_PUBSUB_NATS", "pubsub-nats"),
			DaprStateRedis: env("DAPR_STATE_REDIS", "statestore-redis"),
			DaprStatePG:    env("DAPR_STATE_POSTGRES", "statestore-postgres"),
			DaprBindingFS:  env("DAPR_BINDING_FS", "binding-fs"),
		},

		Connectors: envList("CONNECTORS", "redis-direct,redis-dapr"),

		Plan: sweep.Plan{
			Mode: env("SWEEP_MODE", sweep.ModeOneAtATime),
			Baseline: sweep.Point{
				Agents:       envInt("AGENTS", 8),
				PayloadBytes: envInt("PAYLOAD_BYTES", 256),
			},
			Agents:       envInts("SWEEP_AGENTS", "1,8,32,128,512,2048,8192,32768,131072,1000000"),
			PayloadBytes: envInts("SWEEP_PAYLOAD_BYTES", "256,1024,4096,16384,1048576,16777216,134217728,1073741824"),

			// large payloads are moved by a single agent; many agents move the
			// baseline payload
			PayloadSweepAgents: envInt("PAYLOAD_SWEEP_AGENTS", 1),

			Tasks:        envInt("TASKS", 100_000),
			MaxTaskBytes: envInt64("MAX_TASK_BYTES", 8<<30),
			WarmupTasks:  envInt("WARMUP_TASKS", 1000),

			Keyspace:        envInt("KEYSPACE", 1000),
			MaxDatasetBytes: envInt64("MAX_DATASET_BYTES", 256<<20),

			Cooldown:    envDuration("STEP_COOLDOWN", 5*time.Second),
			OpTimeout:   envDuration("OP_TIMEOUT", 5*time.Second),
			StepTimeout: envDuration("STEP_TIMEOUT", 30*time.Minute),
		},

		ResultsDir:  env("RESULTS_DIR", "traces"),
		MetricsAddr: env("METRICS_ADDR", ":9100"),
	}

	return cfg, cfg.validate()
}

func (c Config) validate() error {
	if len(c.Connectors) == 0 {
		return fmt.Errorf("CONNECTORS is empty; choose from %s", strings.Join(connector.Names(), ", "))
	}
	if len(c.Connectors) > MaxConnectors {
		return fmt.Errorf("CONNECTORS lists %d connectors; at most %d may be compared in one run, so the backend under test stays healthy", len(c.Connectors), MaxConnectors)
	}
	if c.Plan.Mode != sweep.ModeOneAtATime && c.Plan.Mode != sweep.ModeGrid {
		return fmt.Errorf("SWEEP_MODE must be %q or %q, got %q", sweep.ModeOneAtATime, sweep.ModeGrid, c.Plan.Mode)
	}
	if c.Plan.Tasks < 1 {
		return fmt.Errorf("TASKS must be at least 1")
	}
	if c.Plan.StepTimeout <= 0 {
		return fmt.Errorf("STEP_TIMEOUT must be positive")
	}
	if c.Plan.Baseline.Agents < 1 || c.Plan.PayloadSweepAgents < 1 {
		return fmt.Errorf("AGENTS and PAYLOAD_SWEEP_AGENTS must be at least 1")
	}
	for _, n := range c.Plan.Agents {
		if n < 1 {
			return fmt.Errorf("SWEEP_AGENTS levels must be at least 1")
		}
	}
	return nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(env(key, "")); err == nil {
		return v
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if v, err := strconv.ParseInt(env(key, ""), 10, 64); err == nil {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(env(key, "")); err == nil {
		return v
	}
	return fallback
}

// envList splits a comma-separated variable, dropping blanks. A variable that
// is set but empty yields no items, which is how a sweep dimension is disabled.
func envList(key, fallback string) []string {
	raw, ok := os.LookupEnv(key)
	if !ok {
		raw = fallback
	}
	var out []string
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func envInts(key, fallback string) []int {
	var out []int
	for _, item := range envList(key, fallback) {
		if n, err := strconv.Atoi(item); err == nil {
			out = append(out, n)
		}
	}
	return out
}
