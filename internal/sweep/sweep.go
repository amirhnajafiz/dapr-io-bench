// sweep.go
package sweep

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/amirtkz/dapr-overhead-bench/internal/connector"
	"github.com/amirtkz/dapr-overhead-bench/internal/metrics"
	"github.com/amirtkz/dapr-overhead-bench/internal/runner"
	"github.com/amirtkz/dapr-overhead-bench/internal/stats"
)

// Dimensions a sweep can vary. "baseline" tags the point where every
// dimension sits at its baseline value; in one-at-a-time mode that point is
// shared by all three curves and measured once.
const (
	DimBaseline    = "baseline"
	DimRate        = "rate"
	DimPayload     = "payload_bytes"
	DimConcurrency = "concurrency"
	DimGrid        = "grid"
)

// Sweep modes.
const (
	ModeOneAtATime = "oat"  // vary one dimension, hold the other two at baseline
	ModeGrid       = "grid" // every combination of the three lists
)

// Point is one load setting: the three dimensions and which one it varies.
type Point struct {
	Dimension    string `json:"dimension"`
	Rate         int    `json:"rate"`
	PayloadBytes int    `json:"payload_bytes"`
	Concurrency  int    `json:"concurrency"`
}

// Plan is the load schedule of a sweep.
type Plan struct {
	Mode          string `json:"mode"`
	Baseline      Point  `json:"baseline"`
	Rates         []int  `json:"rates"`
	PayloadBytes  []int  `json:"payload_bytes"`
	Concurrencies []int  `json:"concurrencies"`

	Keyspace  int           `json:"keyspace"`
	Warmup    time.Duration `json:"-"`
	Duration  time.Duration `json:"-"`
	Cooldown  time.Duration `json:"-"`
	OpTimeout time.Duration `json:"-"`
}

// Points expands the plan into the ordered list of settings to measure.
func (p Plan) Points() []Point {
	if p.Mode == ModeGrid {
		var pts []Point
		for _, r := range orDefault(p.Rates, p.Baseline.Rate) {
			for _, b := range orDefault(p.PayloadBytes, p.Baseline.PayloadBytes) {
				for _, c := range orDefault(p.Concurrencies, p.Baseline.Concurrency) {
					pts = append(pts, Point{DimGrid, r, b, c})
				}
			}
		}
		return pts
	}

	base := p.Baseline
	base.Dimension = DimBaseline
	pts := []Point{base}
	for _, r := range p.Rates {
		if r != base.Rate {
			pts = append(pts, Point{DimRate, r, base.PayloadBytes, base.Concurrency})
		}
	}
	for _, b := range p.PayloadBytes {
		if b != base.PayloadBytes {
			pts = append(pts, Point{DimPayload, base.Rate, b, base.Concurrency})
		}
	}
	for _, c := range p.Concurrencies {
		if c != base.Concurrency {
			pts = append(pts, Point{DimConcurrency, base.Rate, base.PayloadBytes, c})
		}
	}
	return pts
}

func orDefault(list []int, fallback int) []int {
	if len(list) == 0 {
		return []int{fallback}
	}
	return list
}

// Header is the first line of a trace file: everything needed to interpret
// the step records that follow it.
type Header struct {
	Type       string   `json:"type"`
	Sweep      string   `json:"sweep"`
	StartedAt  string   `json:"started_at"`
	Connectors []string `json:"connectors"`
	Plan       Plan     `json:"plan"`
	Warmup     string   `json:"warmup"`
	Duration   string   `json:"duration"`
	Cooldown   string   `json:"cooldown"`
	OpTimeout  string   `json:"op_timeout"`
	Steps      int      `json:"steps"`
}

// Record is one operation of one connector at one point: a line of the trace.
type Record struct {
	Type       string `json:"type"`
	Sweep      string `json:"sweep"`
	CapturedAt string `json:"captured_at"`

	Dimension    string `json:"dimension"`
	Rate         int    `json:"rate"`
	PayloadBytes int    `json:"payload_bytes"`
	Concurrency  int    `json:"concurrency"`
	Keyspace     int    `json:"keyspace"`

	Connector string `json:"connector"`
	Backend   string `json:"backend"`
	Mode      string `json:"mode"`
	Op        string `json:"op"`

	Paced        bool    `json:"paced"`
	Elapsed      float64 `json:"elapsed_seconds"`
	AchievedRate float64 `json:"achieved_rate"`
	Ops          uint64  `json:"ops"`
	Errors       uint64  `json:"errors"`
	Timeouts     uint64  `json:"timeouts"`
	FirstError   string  `json:"first_error,omitempty"`
	Throughput   float64 `json:"throughput"`

	Latency stats.Summary `json:"latency"`
	Stall   stats.Summary `json:"stall"`
	Wait    stats.Summary `json:"wait"`
}

// Run measures every connector at every point of the plan, one connector at a
// time, and appends one JSON record per operation to a trace file in dir.
// It returns the path of that file.
func Run(ctx context.Context, conns []connector.Connector, w *connector.Workload, plan Plan, dir string) (string, error) {
	id := sweepID(conns)
	pts := plan.Points()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create results dir: %w", err)
	}
	path := filepath.Join(dir, id+".jsonl")
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create trace file: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	names := make([]string, len(conns))
	for i, c := range conns {
		names[i] = connector.Name(c)
	}
	if err := enc.Encode(Header{
		Type: "sweep", Sweep: id, StartedAt: time.Now().UTC().Format(time.RFC3339),
		Connectors: names, Plan: plan,
		Warmup: plan.Warmup.String(), Duration: plan.Duration.String(),
		Cooldown: plan.Cooldown.String(), OpTimeout: plan.OpTimeout.String(),
		Steps: len(pts) * len(conns),
	}); err != nil {
		return path, err
	}

	log.Printf("sweep %s: %d points x %d connectors, %s warmup + %s measured per step, writing %s",
		id, len(pts), len(conns), plan.Warmup, plan.Duration, path)

	step := 0
	for _, pt := range pts {
		w.SetPayloadBytes(pt.PayloadBytes)

		for _, c := range conns {
			step++
			name := connector.Name(c)
			rate := strconv.Itoa(pt.Rate)
			if pt.Rate == 0 {
				rate = "closed-loop"
			}
			log.Printf("step %d/%d [%s] %s rate=%s payload=%dB concurrency=%d",
				step, len(pts)*len(conns), pt.Dimension, name, rate, pt.PayloadBytes, pt.Concurrency)
			metrics.SetStep(id, pt.Dimension, name,
				strconv.Itoa(pt.Rate), strconv.Itoa(pt.PayloadBytes), strconv.Itoa(pt.Concurrency))

			if err := runner.Seed(ctx, c, plan.Keyspace, plan.OpTimeout); err != nil {
				if ctx.Err() != nil {
					return path, ctx.Err()
				}
				log.Printf("  skipping %s at this point: %v", name, err)
				continue
			}

			res, err := runner.Step(ctx, c, runner.Options{
				Concurrency: pt.Concurrency,
				Rate:        pt.Rate,
				Warmup:      plan.Warmup,
				Duration:    plan.Duration,
				OpTimeout:   plan.OpTimeout,
			})
			if err != nil {
				return path, err
			}

			now := time.Now().UTC().Format(time.RFC3339)
			for _, o := range res.Ops {
				rec := Record{
					Type: "step", Sweep: id, CapturedAt: now,
					Dimension: pt.Dimension, Rate: pt.Rate, PayloadBytes: pt.PayloadBytes,
					Concurrency: pt.Concurrency, Keyspace: plan.Keyspace,
					Connector: name, Backend: c.Backend(), Mode: c.Mode(), Op: o.Op,
					Paced: res.Paced, Elapsed: res.Elapsed.Seconds(), AchievedRate: res.AchievedRate,
					Ops: o.Ops, Errors: o.Errors, Timeouts: o.Timeouts, FirstError: o.FirstError, Throughput: o.Throughput,
					Latency: o.Latency, Stall: o.Stall, Wait: o.Wait,
				}
				if err := enc.Encode(rec); err != nil {
					return path, fmt.Errorf("write record: %w", err)
				}
			}

			// let the backend drain and the sidecar settle before the next
			// connector touches it, so one step's tail never bleeds into the next
			if plan.Cooldown > 0 && step < len(pts)*len(conns) {
				select {
				case <-ctx.Done():
					return path, ctx.Err()
				case <-time.After(plan.Cooldown):
				}
			}
		}
	}

	metrics.Step.Reset()
	return path, nil
}

// sweepID names a trace after when it ran and which backends it covered,
// e.g. 20260927T101500Z-redis.
func sweepID(conns []connector.Connector) string {
	seen := map[string]bool{}
	var backends []string
	for _, c := range conns {
		if !seen[c.Backend()] {
			seen[c.Backend()] = true
			backends = append(backends, c.Backend())
		}
	}
	return time.Now().UTC().Format("20060102T150405Z") + "-" + strings.Join(backends, "+")
}
