package runner

import (
	"context"
	"testing"
	"time"

	"github.com/amirtkz/dapr-overhead-bench/internal/connector"
)

// slow is a connector whose every operation takes a fixed time.
type slow struct{ latency time.Duration }

func (s slow) Backend() string                 { return "fake" }
func (s slow) Mode() string                    { return "direct" }
func (s slow) Connect(context.Context) error   { return nil }
func (s slow) Seed(context.Context, int) error { return nil }
func (s slow) Close() error                    { return nil }
func (s slow) Ops() []connector.Op {
	return []connector.Op{{Name: "op", Run: func(ctx context.Context, _ int) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.latency):
			return nil
		}
	}}}
}

func TestRunsExactlyTheTasks(t *testing.T) {
	res, err := Step(context.Background(), slow{time.Millisecond}, Options{
		Agents: 4, Tasks: 200, WarmupTasks: 20, OpTimeout: time.Second, StepTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Completed != 200 || res.Ops[0].Ops != 200 {
		t.Errorf("completed %d, recorded %d, want 200", res.Completed, res.Ops[0].Ops)
	}
	if res.Truncated {
		t.Error("step should not be truncated")
	}
	// 200 tasks of 1ms over 4 agents is about 50ms of makespan
	if res.Elapsed < 40*time.Millisecond || res.Elapsed > 400*time.Millisecond {
		t.Errorf("elapsed %v, want ~50ms", res.Elapsed)
	}
	if res.Ops[0].Latency.P50 < 0.0009 || res.Ops[0].Latency.P50 > 0.01 {
		t.Errorf("latency p50 %v, want ~1ms", res.Ops[0].Latency.P50)
	}
}

func TestMoreAgentsFinishSooner(t *testing.T) {
	one, _ := Step(context.Background(), slow{2 * time.Millisecond}, Options{
		Agents: 1, Tasks: 50, OpTimeout: time.Second, StepTimeout: time.Minute,
	})
	ten, _ := Step(context.Background(), slow{2 * time.Millisecond}, Options{
		Agents: 10, Tasks: 50, OpTimeout: time.Second, StepTimeout: time.Minute,
	})
	if ten.Elapsed*3 > one.Elapsed {
		t.Errorf("10 agents took %v, 1 agent %v; want a clear speedup", ten.Elapsed, one.Elapsed)
	}
}

func TestStepTimeoutTruncates(t *testing.T) {
	res, err := Step(context.Background(), slow{10 * time.Millisecond}, Options{
		Agents: 1, Tasks: 1000, OpTimeout: time.Second, StepTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || res.Completed >= 1000 || res.Completed == 0 {
		t.Errorf("completed %d truncated %v, want a partial, truncated step", res.Completed, res.Truncated)
	}
}

func TestTimeoutIsCountedAsError(t *testing.T) {
	res, err := Step(context.Background(), slow{50 * time.Millisecond}, Options{
		Agents: 1, Tasks: 5, OpTimeout: 5 * time.Millisecond, StepTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	op := res.Ops[0]
	if op.Errors != 5 || op.Timeouts != 5 {
		t.Errorf("errors=%d timeouts=%d, want every op to time out", op.Errors, op.Timeouts)
	}
	if op.Throughput != 0 {
		t.Errorf("throughput counts successes only, got %v", op.Throughput)
	}
}

func TestManyAgentsShareShards(t *testing.T) {
	res, err := Step(context.Background(), slow{time.Millisecond}, Options{
		Agents: 5000, Tasks: 5000, OpTimeout: time.Second, StepTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Completed != 5000 || res.Ops[0].Errors != 0 {
		t.Errorf("completed %d errors %d", res.Completed, res.Ops[0].Errors)
	}
}
