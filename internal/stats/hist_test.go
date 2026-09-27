package stats

import (
	"math"
	"testing"
	"time"
)

func TestQuantileWithinOnePercent(t *testing.T) {
	var h Histogram
	for i := 1; i <= 10000; i++ {
		h.Observe(time.Duration(i) * time.Microsecond)
	}

	for _, q := range []float64{0.5, 0.95, 0.99} {
		want := q * 10000 * float64(time.Microsecond)
		got := float64(h.Quantile(q))
		if got < want || got > want*1.011 {
			t.Errorf("q%.2f: got %v, want within 1%% above %v", q, time.Duration(got), time.Duration(want))
		}
	}

	if h.Max() != 10000*time.Microsecond {
		t.Errorf("max: got %v", h.Max())
	}
	if math.Abs(float64(h.Mean())-5000.5*float64(time.Microsecond)) > float64(time.Microsecond) {
		t.Errorf("mean: got %v", h.Mean())
	}
}

func TestZeroAndMerge(t *testing.T) {
	var a, b Histogram
	for i := 0; i < 90; i++ {
		a.Observe(0)
	}
	for i := 0; i < 10; i++ {
		b.Observe(time.Second)
	}
	a.Merge(&b)

	if a.Count() != 100 {
		t.Fatalf("count: got %d", a.Count())
	}
	if a.Quantile(0.5) != 0 {
		t.Errorf("p50: got %v, want 0", a.Quantile(0.5))
	}
	if got := a.Quantile(0.95); got < 990*time.Millisecond || got > time.Second {
		t.Errorf("p95: got %v, want ~1s", got)
	}
}
