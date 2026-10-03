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

// Dimensions a sweep can vary. "baseline" tags the point where both sit at
// their baseline value; in one-at-a-time mode that point is shared by both
// curves and measured once.
const (
	DimBaseline = "baseline"
	DimAgents   = "agents"
	DimPayload  = "payload_bytes"
	DimGrid     = "grid"
)

// Sweep modes.
const (
	ModeOneAtATime = "oat"  // vary one dimension, hold the other at its profile value
	ModeGrid       = "grid" // every combination of the two lists
)

// Point is one load setting: how many agents share the tasks and how large
// each value is. Dimension says which curve the point belongs to; a point on
// two curves carries both names, comma-separated.
type Point struct {
	Dimension    string `json:"dimension"`
	Agents       int    `json:"agents"`
	PayloadBytes int    `json:"payload_bytes"`
}

// Plan is the load schedule of a sweep.
//
// The two dimensions have their own load profiles rather than one shared
// baseline: large payloads are moved by few agents and many agents move the
// baseline payload, so a sweep finds where storage falls behind on each axis
// without paying for the combinations that would only exhaust memory.
type Plan struct {
	Mode         string `json:"mode"`
	Baseline     Point  `json:"baseline"`
	Agents       []int  `json:"agents"`
	PayloadBytes []int  `json:"payload_bytes"`

	// Agents driving the payload sweep (the agents sweep uses the baseline payload).
	PayloadSweepAgents int `json:"payload_sweep_agents"`

	// Tasks is the number of operations per step at small payloads. It shrinks
	// as payloads grow so a step never moves more than MaxTaskBytes, and it is
	// never fewer than the agents, so every agent gets at least one task.
	Tasks        int   `json:"tasks"`
	MaxTaskBytes int64 `json:"max_task_bytes"`
	WarmupTasks  int   `json:"warmup_tasks"`

	// Keyspace is the number of distinct keys at small payloads. It shrinks
	// as payloads grow so the seeded dataset never exceeds MaxDatasetBytes.
	Keyspace        int   `json:"keyspace"`
	MaxDatasetBytes int64 `json:"max_dataset_bytes"`

	Cooldown    time.Duration `json:"-"`
	OpTimeout   time.Duration `json:"-"`
	StepTimeout time.Duration `json:"-"`
}

// Points expands the plan into the ordered list of settings to measure.
func (p Plan) Points() []Point {
	if p.Mode == ModeGrid {
		var pts []Point
		for _, a := range orDefault(p.Agents, p.Baseline.Agents) {
			for _, b := range orDefault(p.PayloadBytes, p.Baseline.PayloadBytes) {
				pts = append(pts, Point{DimGrid, a, b})
			}
		}
		return pts
	}

	base := p.Baseline
	base.Dimension = DimBaseline
	pts := []Point{base}
	// a load that belongs to both dimensions is measured once and tagged with
	// both, comma-separated, so each curve still has its point
	add := func(pt Point) {
		for i := range pts {
			if pts[i].sameLoad(pt) {
				if pts[i].Dimension != DimBaseline {
					pts[i].Dimension += "," + pt.Dimension
				}
				return
			}
		}
		pts = append(pts, pt)
	}
	for _, b := range p.PayloadBytes {
		add(Point{DimPayload, p.PayloadSweepAgents, b})
	}
	for _, a := range p.Agents {
		add(Point{DimAgents, a, base.PayloadBytes})
	}
	return pts
}

func (pt Point) sameLoad(o Point) bool {
	return pt.Agents == o.Agents && pt.PayloadBytes == o.PayloadBytes
}

func orDefault(list []int, fallback int) []int {
	if len(list) == 0 {
		return []int{fallback}
	}
	return list
}

// KeyspaceFor shrinks the keyspace as the payload grows, so the seeded
// dataset stays under MaxDatasetBytes: 1000 keys at 256 B, but a single key
// at a gigabyte.
func (p Plan) KeyspaceFor(payloadBytes int) int {
	if p.MaxDatasetBytes <= 0 || payloadBytes <= 0 {
		return p.Keyspace
	}
	fit := int(p.MaxDatasetBytes / int64(payloadBytes))
	return max(1, min(p.Keyspace, fit))
}

// TasksFor shrinks the task count as the payload grows, so a step never moves
// more than MaxTaskBytes, and never hands out fewer tasks than there are
// agents.
func (p Plan) TasksFor(pt Point) int {
	tasks := p.Tasks
	if p.MaxTaskBytes > 0 && pt.PayloadBytes > 0 {
		tasks = min(tasks, int(p.MaxTaskBytes/int64(pt.PayloadBytes)))
	}
	return max(tasks, pt.Agents, 1)
}

// Header is the first line of a trace file: everything needed to interpret
// the step records that follow it.
type Header struct {
	Type        string   `json:"type"`
	Sweep       string   `json:"sweep"`
	StartedAt   string   `json:"started_at"`
	Connectors  []string `json:"connectors"`
	Plan        Plan     `json:"plan"`
	Cooldown    string   `json:"cooldown"`
	OpTimeout   string   `json:"op_timeout"`
	StepTimeout string   `json:"step_timeout"`
	Steps       int      `json:"steps"`
}

// Record is one operation of one connector at one point: a line of the trace.
type Record struct {
	Type       string `json:"type"`
	Sweep      string `json:"sweep"`
	CapturedAt string `json:"captured_at"`

	Dimension    string `json:"dimension"`
	Agents       int    `json:"agents"`
	PayloadBytes int    `json:"payload_bytes"`
	Tasks        int    `json:"tasks"`
	Keyspace     int    `json:"keyspace"`

	Connector string `json:"connector"`
	Backend   string `json:"backend"`
	Mode      string `json:"mode"`
	Op        string `json:"op"`

	Elapsed    float64 `json:"elapsed_seconds"` // makespan of the whole step, all ops
	Completed  uint64  `json:"completed"`       // tasks finished in the step, all ops
	Truncated  bool    `json:"truncated"`       // the step hit its timeout first
	Ops        uint64  `json:"ops"`
	Errors     uint64  `json:"errors"`
	Timeouts   uint64  `json:"timeouts"`
	FirstError string  `json:"first_error,omitempty"`
	Throughput float64 `json:"throughput"`

	Latency stats.Summary `json:"latency"`
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
		Cooldown: plan.Cooldown.String(), OpTimeout: plan.OpTimeout.String(),
		StepTimeout: plan.StepTimeout.String(), Steps: len(pts) * len(conns),
	}); err != nil {
		return path, err
	}

	log.Printf("sweep %s: %d points x %d connectors, up to %d tasks per step, writing %s",
		id, len(pts), len(conns), plan.Tasks, path)

	step := 0
	for _, pt := range pts {
		w.SetPayloadBytes(pt.PayloadBytes)
		keyspace := plan.KeyspaceFor(pt.PayloadBytes)
		w.SetKeyspace(keyspace)
		tasks := plan.TasksFor(pt)

		for _, c := range conns {
			step++
			name := connector.Name(c)
			log.Printf("step %d/%d [%s] %s agents=%d payload=%dB tasks=%d keyspace=%d",
				step, len(pts)*len(conns), pt.Dimension, name, pt.Agents, pt.PayloadBytes, tasks, keyspace)
			metrics.SetStep(id, pt.Dimension, name,
				strconv.Itoa(pt.Agents), strconv.Itoa(pt.PayloadBytes), strconv.Itoa(tasks))

			if err := runner.Seed(ctx, c, keyspace, plan.OpTimeout); err != nil {
				if ctx.Err() != nil {
					return path, ctx.Err()
				}
				log.Printf("  skipping %s at this point: %v", name, err)
				continue
			}

			res, err := runner.Step(ctx, c, runner.Options{
				Agents:      pt.Agents,
				Tasks:       tasks,
				WarmupTasks: min(plan.WarmupTasks, tasks/10),
				OpTimeout:   plan.OpTimeout,
				StepTimeout: plan.StepTimeout,
			})
			if err != nil {
				return path, err
			}

			now := time.Now().UTC().Format(time.RFC3339)
			for _, o := range res.Ops {
				rec := Record{
					Type: "step", Sweep: id, CapturedAt: now,
					Dimension: pt.Dimension, Agents: pt.Agents, PayloadBytes: pt.PayloadBytes,
					Tasks: tasks, Keyspace: keyspace,
					Connector: name, Backend: c.Backend(), Mode: c.Mode(), Op: o.Op,
					Elapsed: res.Elapsed.Seconds(), Completed: res.Completed, Truncated: res.Truncated,
					Ops: o.Ops, Errors: o.Errors, Timeouts: o.Timeouts, FirstError: o.FirstError,
					Throughput: o.Throughput, Latency: o.Latency,
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
