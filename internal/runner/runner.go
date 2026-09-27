// runner.go
package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/amirtkz/dapr-overhead-bench/internal/connector"
	"github.com/amirtkz/dapr-overhead-bench/internal/metrics"
	"github.com/amirtkz/dapr-overhead-bench/internal/stats"
)

// Options shape one measured step against one connector.
type Options struct {
	Concurrency int           // concurrent callers ("agents") issuing operations
	Rate        int           // target operations/second across all callers; 0 = closed loop, as fast as possible
	Warmup      time.Duration // period run first whose measurements are discarded
	Duration    time.Duration // measured period
	OpTimeout   time.Duration // per-operation deadline, bounds a hung backend
}

// OpResult is what one operation of a connector did during a step.
type OpResult struct {
	Op         string
	Ops        uint64  // operations completed inside the window
	Errors     uint64  // of which failed, timeouts included
	Timeouts   uint64  // of which hit OpTimeout
	FirstError string  // message of the first failure, for diagnosing the count
	Throughput float64 // successful operations per second
	Latency    stats.Summary
	Stall      stats.Summary
	Wait       stats.Summary
}

// Result is one connector's measured step.
type Result struct {
	Elapsed      time.Duration // measured window, first send to last return
	Paced        bool          // false for a closed loop, where stall is not defined
	AchievedRate float64       // operations per second actually issued, ok or not
	Ops          []OpResult
}

// Seed writes every key of the keyspace once through the connector's "write"
// op, so a later "read" never misses regardless of which op ran before it.
// Connectors without a "write" op (pub/sub) need no seeding.
func Seed(ctx context.Context, c connector.Connector, keyspace int, timeout time.Duration) error {
	for _, op := range c.Ops() {
		if op.Name != "write" {
			continue
		}
		for i := 0; i < keyspace; i++ {
			opCtx, cancel := context.WithTimeout(ctx, timeout)
			err := op.Run(opCtx, i)
			cancel()
			if err != nil {
				return fmt.Errorf("seed key %d: %w", i, err)
			}
		}
	}
	return nil
}

// Step warms the connector up, then measures it for opt.Duration.
func Step(ctx context.Context, c connector.Connector, opt Options) (Result, error) {
	if opt.Concurrency < 1 {
		return Result{}, errors.New("concurrency must be at least 1")
	}
	if len(c.Ops()) == 0 {
		return Result{}, errors.New("connector has no operations")
	}

	if opt.Warmup > 0 {
		warmCtx, cancel := context.WithTimeout(ctx, opt.Warmup)
		drive(warmCtx, c, opt, false)
		cancel()
	}
	if ctx.Err() != nil {
		return Result{}, ctx.Err()
	}

	runCtx, cancel := context.WithTimeout(ctx, opt.Duration)
	defer cancel()

	res := drive(runCtx, c, opt, true)
	if ctx.Err() != nil {
		return Result{}, ctx.Err()
	}
	return res, nil
}

// collector is one worker's private tally; workers never share one, so there
// is no lock on the hot path. They are merged once the step is over.
type collector struct {
	latency  []stats.Histogram
	stall    []stats.Histogram
	wait     []stats.Histogram
	errors   []uint64
	timeouts []uint64
	firstErr []string
}

func newCollector(ops int) *collector {
	return &collector{
		latency:  make([]stats.Histogram, ops),
		stall:    make([]stats.Histogram, ops),
		wait:     make([]stats.Histogram, ops),
		errors:   make([]uint64, ops),
		timeouts: make([]uint64, ops),
		firstErr: make([]string, ops),
	}
}

func (col *collector) merge(other *collector) {
	for i := range col.latency {
		col.latency[i].Merge(&other.latency[i])
		col.stall[i].Merge(&other.stall[i])
		col.wait[i].Merge(&other.wait[i])
		col.errors[i] += other.errors[i]
		col.timeouts[i] += other.timeouts[i]
		if col.firstErr[i] == "" {
			col.firstErr[i] = other.firstErr[i]
		}
	}
}

// drive issues operations until ctx ends and returns what was measured.
//
// With a rate, the load is open loop: operation k is due at start + k/rate no
// matter how the previous ones went, exactly as a fleet of agents would keep
// arriving. A caller that finds its operation already overdue records the
// overdue time as stall: that is the queueing delay the agent sat through
// before its call even began. Without a rate the load is closed loop, each
// caller firing again as soon as the previous call returns, and stall is zero
// by construction.
func drive(ctx context.Context, c connector.Connector, opt Options, record bool) Result {
	ops := c.Ops()
	paced := opt.Rate > 0

	var interval float64 // nanoseconds between scheduled sends
	if paced {
		interval = float64(time.Second) / float64(opt.Rate)
	}

	var (
		seq    atomic.Int64
		issued atomic.Uint64
		wg     sync.WaitGroup
	)

	collectors := make([]*collector, opt.Concurrency)
	start := time.Now()

	for w := 0; w < opt.Concurrency; w++ {
		col := newCollector(len(ops))
		collectors[w] = col

		wg.Add(1)
		go func() {
			defer wg.Done()

			for ctx.Err() == nil {
				k := seq.Add(1) - 1

				var stall time.Duration
				if paced {
					due := start.Add(time.Duration(float64(k) * interval))
					if wait := time.Until(due); wait > 0 {
						timer := time.NewTimer(wait)
						select {
						case <-ctx.Done():
							timer.Stop()
							return
						case <-timer.C:
						}
					} else {
						stall = -wait
					}
				}

				idx := int(k % int64(len(ops)))
				op := ops[idx]

				opCtx, cancel := context.WithTimeout(ctx, opt.OpTimeout)
				began := time.Now()
				err := op.Run(opCtx, int(k))
				latency := time.Since(began)
				cancel()

				// shutdown cancellations are not a property of the connector
				if err != nil && ctx.Err() != nil {
					return
				}
				issued.Add(1)
				if !record {
					continue
				}

				col.latency[idx].Observe(latency)
				col.stall[idx].Observe(stall)
				col.wait[idx].Observe(stall + latency)
				if err != nil {
					col.errors[idx]++
					if isTimeout(err) {
						col.timeouts[idx]++
					}
					if col.firstErr[idx] == "" {
						col.firstErr[idx] = err.Error()
					}
				}
				metrics.Observe(c.Backend(), c.Mode(), op.Name, latency, stall, err)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := newCollector(len(ops))
	for _, col := range collectors {
		total.merge(col)
	}

	res := Result{
		Elapsed:      elapsed,
		Paced:        paced,
		AchievedRate: float64(issued.Load()) / elapsed.Seconds(),
	}
	for i, op := range ops {
		n := total.latency[i].Count()
		res.Ops = append(res.Ops, OpResult{
			Op:         op.Name,
			Ops:        n,
			Errors:     total.errors[i],
			Timeouts:   total.timeouts[i],
			FirstError: total.firstErr[i],
			Throughput: float64(n-total.errors[i]) / elapsed.Seconds(),
			Latency:    total.latency[i].Summarize(),
			Stall:      total.stall[i].Summarize(),
			Wait:       total.wait[i].Summarize(),
		})
	}

	if record {
		for _, o := range res.Ops {
			log.Printf("  %-16s %-8s ops=%-8d err=%-5d thr=%8.1f/s  lat p50=%s p99=%s  stall p50=%s p99=%s",
				connector.Name(c), o.Op, o.Ops, o.Errors, o.Throughput,
				ms(o.Latency.P50), ms(o.Latency.P99), ms(o.Stall.P50), ms(o.Stall.P99))
			if o.FirstError != "" {
				log.Printf("  %-16s %-8s first error: %s", connector.Name(c), o.Op, o.FirstError)
			}
		}
	}

	return res
}

// isTimeout recognises OpTimeout firing on either side of the sidecar: the
// direct SDKs return the context error, the Dapr client a gRPC status.
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
}

func ms(seconds float64) string { return fmt.Sprintf("%.2fms", seconds*1000) }
