// hist.go
package stats

import (
	"math"
	"time"
)

// Histogram is a log-linear latency histogram: bucket i holds every value in
// (base^(i-1), base^i] nanoseconds, so a bucket is never wider than 1% of its
// value and any percentile read from it is within 1% of the true one, at a
// fixed ~2.5k buckets for the whole 1ns..100s range. Exact enough for a p99
// and far cheaper than keeping millions of samples per step.
//
// A Histogram is not safe for concurrent use; each worker keeps its own and
// they are merged at the end of a step.
type Histogram struct {
	counts []uint64
	count  uint64
	sum    time.Duration
	min    time.Duration
	max    time.Duration
}

const base = 1.01

var logBase = math.Log(base)

func bucketOf(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(math.Log(float64(d)) / logBase))
}

func bucketUpper(i int) time.Duration {
	if i == 0 {
		return 0
	}
	return time.Duration(math.Pow(base, float64(i)))
}

// Observe records one value. Negative values are clamped to zero.
func (h *Histogram) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}

	i := bucketOf(d)
	if i >= len(h.counts) {
		grown := make([]uint64, i+64)
		copy(grown, h.counts)
		h.counts = grown
	}

	h.counts[i]++
	if h.count == 0 || d < h.min {
		h.min = d
	}
	if d > h.max {
		h.max = d
	}
	h.count++
	h.sum += d
}

// Merge folds other into h.
func (h *Histogram) Merge(other *Histogram) {
	if other == nil || other.count == 0 {
		return
	}
	if len(other.counts) > len(h.counts) {
		grown := make([]uint64, len(other.counts))
		copy(grown, h.counts)
		h.counts = grown
	}
	for i, c := range other.counts {
		h.counts[i] += c
	}
	if h.count == 0 || other.min < h.min {
		h.min = other.min
	}
	if other.max > h.max {
		h.max = other.max
	}
	h.count += other.count
	h.sum += other.sum
}

// Count is the number of observed values.
func (h *Histogram) Count() uint64 { return h.count }

// Sum is the total of all observed values.
func (h *Histogram) Sum() time.Duration { return h.sum }

// Max is the largest observed value.
func (h *Histogram) Max() time.Duration { return h.max }

// Mean is the arithmetic mean, or 0 for an empty histogram.
func (h *Histogram) Mean() time.Duration {
	if h.count == 0 {
		return 0
	}
	return time.Duration(float64(h.sum) / float64(h.count))
}

// Quantile returns the value at fraction q (0..1) of the distribution: the
// upper bound of the bucket in which the q-th observation falls, so a
// reported percentile is never below the true one by more than 1%.
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	if q <= 0 {
		return h.min
	}
	if q >= 1 {
		return h.max
	}

	target := uint64(math.Ceil(q * float64(h.count)))
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= target {
			v := bucketUpper(i)
			if v > h.max {
				v = h.max
			}
			return v
		}
	}
	return h.max
}

// Summary is the fixed set of numbers a histogram is reported as, in seconds.
type Summary struct {
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	Mean float64 `json:"mean"`
	Max  float64 `json:"max"`
}

// Summarize reads the reported percentiles out of the histogram.
func (h *Histogram) Summarize() Summary {
	return Summary{
		P50:  h.Quantile(0.50).Seconds(),
		P95:  h.Quantile(0.95).Seconds(),
		P99:  h.Quantile(0.99).Seconds(),
		Mean: h.Mean().Seconds(),
		Max:  h.Max().Seconds(),
	}
}
