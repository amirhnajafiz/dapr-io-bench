package runner

import (
	"context"
	"testing"
	"time"

	"github.com/amirtkz/dapr-overhead-bench/internal/connector"
)

// slow is a connector whose every operation takes a fixed time.
type slow struct{ latency time.Duration }

func (s slow) Backend() string               { return "fake" }
func (s slow) Mode() string                  { return "direct" }
func (s slow) Connect(context.Context) error { return nil }
func (s slow) Close() error                  { return nil }
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

func TestOpenLoopStallGrowsWhenOverloaded(t *testing.T) {
	// one caller, 10ms per call: capacity is 100/s. Asking for 400/s means
	// three quarters of the schedule falls behind and the stall climbs for the
	// whole window.
	res, err := Step(context.Background(), slow{10 * time.Millisecond}, Options{
		Concurrency: 1, Rate: 400, Duration: 500 * time.Millisecond, OpTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Paced {
		t.Fatal("expected a paced run")
	}
	op := res.Ops[0]
	if op.Ops < 30 || op.Ops > 60 {
		t.Errorf("ops: got %d, want ~50", op.Ops)
	}
	if op.Latency.P50 < 0.009 || op.Latency.P50 > 0.02 {
		t.Errorf("latency p50: got %v, want ~10ms", op.Latency.P50)
	}
	// the last call was due ~375ms before it started
	if op.Stall.Max < 0.25 {
		t.Errorf("stall max: got %v, want a few hundred ms", op.Stall.Max)
	}
	if op.Stall.P50 < 0.05 {
		t.Errorf("stall p50: got %v, want well above zero", op.Stall.P50)
	}
	if op.Wait.P50 <= op.Latency.P50 {
		t.Errorf("wait p50 %v should exceed latency p50 %v", op.Wait.P50, op.Latency.P50)
	}
}

func TestOpenLoopNoStallWhenKeepingUp(t *testing.T) {
	res, err := Step(context.Background(), slow{time.Millisecond}, Options{
		Concurrency: 4, Rate: 100, Duration: 300 * time.Millisecond, OpTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	op := res.Ops[0]
	if op.Ops < 20 || op.Ops > 40 {
		t.Errorf("ops: got %d, want ~30", op.Ops)
	}
	if op.Stall.P99 > 0.01 {
		t.Errorf("stall p99: got %v, want ~0 (scheduler jitter only)", op.Stall.P99)
	}
	if op.Errors != 0 {
		t.Errorf("errors: got %d", op.Errors)
	}
}

func TestClosedLoop(t *testing.T) {
	res, err := Step(context.Background(), slow{time.Millisecond}, Options{
		Concurrency: 2, Rate: 0, Duration: 200 * time.Millisecond, OpTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Paced {
		t.Fatal("rate 0 must be a closed loop")
	}
	if res.Ops[0].Stall.Max != 0 {
		t.Errorf("closed loop stall must be zero, got %v", res.Ops[0].Stall.Max)
	}
	if res.Ops[0].Ops < 100 {
		t.Errorf("ops: got %d, want a few hundred", res.Ops[0].Ops)
	}
}

func TestTimeoutIsCountedAsError(t *testing.T) {
	res, err := Step(context.Background(), slow{50 * time.Millisecond}, Options{
		Concurrency: 1, Rate: 0, Duration: 200 * time.Millisecond, OpTimeout: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	op := res.Ops[0]
	if op.Errors == 0 || op.Timeouts != op.Errors {
		t.Errorf("errors=%d timeouts=%d, want every op to time out", op.Errors, op.Timeouts)
	}
	if op.Throughput != 0 {
		t.Errorf("throughput counts successes only, got %v", op.Throughput)
	}
}
