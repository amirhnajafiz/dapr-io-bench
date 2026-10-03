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

// Options shape one measured step against one connector: a fixed number of
// tasks shared by a fixed number of agents, each agent taking the next task
// the moment its previous call returns.
type Options struct {
	Agents      int           // concurrent callers sharing the tasks
	Tasks       int           // operations to complete; task k runs op k mod len(ops)
	WarmupTasks int           // tasks run first and discarded
	OpTimeout   time.Duration // per-operation deadline, bounds a hung backend
	StepTimeout time.Duration // bound on the whole step; a step that hits it is recorded as truncated
}

// OpResult is what one operation of a connector did during a step.
type OpResult struct {
	Op         string
	Ops        uint64  // operations completed
	Errors     uint64  // of which failed, timeouts included
	Timeouts   uint64  // of which hit OpTimeout
	FirstError string  // message of the first failure, for diagnosing the count
	Throughput float64 // successful operations per second over the step
	Latency    stats.Summary
}

// Result is one connector's measured step.
type Result struct {
	Elapsed   time.Duration // makespan: first call issued to last call returned
	Tasks     int           // tasks asked for
	Completed uint64        // tasks finished, ok or not
	Truncated bool          // the step hit StepTimeout before finishing its tasks
	Ops       []OpResult
}

// Seed has the connector write every key of the keyspace once, so a later
// read or stat never misses regardless of which op ran before it. The whole
// pass gets a deadline proportional to its size rather than one per call, so
// a gigabyte value is allowed the time it needs.
func Seed(ctx context.Context, c connector.Connector, keyspace int, opTimeout time.Duration) error {
	seedCtx, cancel := context.WithTimeout(ctx, opTimeout*time.Duration(2*keyspace+2))
	defer cancel()
	return c.Seed(seedCtx, keyspace)
}

// maxShards bounds how many private tallies a step keeps. A million agents
// cannot each own a histogram, so agents share a shard under a mutex that is
// contended by at most Agents/maxShards of them.
const maxShards = 64

// Step warms the connector up, then runs the tasks and measures them.
func Step(ctx context.Context, c connector.Connector, opt Options) (Result, error) {
	if opt.Agents < 1 {
		return Result{}, errors.New("agents must be at least 1")
	}
	if opt.Tasks < 1 {
		return Result{}, errors.New("tasks must be at least 1")
	}
	if len(c.Ops()) == 0 {
		return Result{}, errors.New("connector has no operations")
	}

	if opt.WarmupTasks > 0 {
		warmCtx, cancel := context.WithTimeout(ctx, opt.StepTimeout)
		drive(warmCtx, c, opt.Agents, opt.WarmupTasks, opt.OpTimeout, false)
		cancel()
	}
	if ctx.Err() != nil {
		return Result{}, ctx.Err()
	}

	runCtx, cancel := context.WithTimeout(ctx, opt.StepTimeout)
	defer cancel()

	res := drive(runCtx, c, opt.Agents, opt.Tasks, opt.OpTimeout, true)
	if ctx.Err() != nil {
		return Result{}, ctx.Err()
	}
	return res, nil
}

// collector is one shard's tally, merged with the others once the step is over.
type collector struct {
	mu       sync.Mutex
	latency  []stats.Histogram
	errors   []uint64
	timeouts []uint64
	firstErr []string
}

func newCollector(ops int) *collector {
	return &collector{
		latency:  make([]stats.Histogram, ops),
		errors:   make([]uint64, ops),
		timeouts: make([]uint64, ops),
		firstErr: make([]string, ops),
	}
}

func (col *collector) merge(other *collector) {
	for i := range col.latency {
		col.latency[i].Merge(&other.latency[i])
		col.errors[i] += other.errors[i]
		col.timeouts[i] += other.timeouts[i]
		if col.firstErr[i] == "" {
			col.firstErr[i] = other.firstErr[i]
		}
	}
}

// drive runs tasks operations across agents callers and returns what was
// measured. Every agent is closed loop: it takes the next task as soon as its
// previous call returns, so the step measures how fast the path can serve a
// given number of concurrent callers, and the makespan is how long that many
// agents wait for all their work to be done.
func drive(ctx context.Context, c connector.Connector, agents, tasks int, opTimeout time.Duration, record bool) Result {
	ops := c.Ops()

	var (
		seq       atomic.Int64
		completed atomic.Uint64
		wg        sync.WaitGroup
	)

	shards := min(agents, maxShards)
	collectors := make([]*collector, shards)
	for i := range collectors {
		collectors[i] = newCollector(len(ops))
	}
	start := time.Now()

	for a := 0; a < agents; a++ {
		col := collectors[a%shards]

		wg.Add(1)
		go func() {
			defer wg.Done()

			for ctx.Err() == nil {
				k := seq.Add(1) - 1
				if k >= int64(tasks) {
					return
				}

				idx := int(k % int64(len(ops)))
				op := ops[idx]

				opCtx, cancel := context.WithTimeout(ctx, opTimeout)
				began := time.Now()
				err := op.Run(opCtx, int(k))
				latency := time.Since(began)
				cancel()

				// a step cut short by its deadline is not a property of the connector
				if err != nil && ctx.Err() != nil {
					return
				}
				completed.Add(1)
				if !record {
					continue
				}

				col.mu.Lock()
				col.latency[idx].Observe(latency)
				if err != nil {
					col.errors[idx]++
					if isTimeout(err) {
						col.timeouts[idx]++
					}
					if col.firstErr[idx] == "" {
						col.firstErr[idx] = err.Error()
					}
				}
				col.mu.Unlock()
				metrics.Observe(c.Backend(), c.Mode(), op.Name, latency, err)
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
		Elapsed:   elapsed,
		Tasks:     tasks,
		Completed: completed.Load(),
		Truncated: completed.Load() < uint64(tasks),
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
		})
	}

	if record {
		for _, o := range res.Ops {
			log.Printf("  %-16s %-8s ops=%-8d err=%-5d thr=%9.1f/s  lat p50=%s p99=%s max=%s",
				connector.Name(c), o.Op, o.Ops, o.Errors, o.Throughput,
				ms(o.Latency.P50), ms(o.Latency.P99), ms(o.Latency.Max))
			if o.FirstError != "" {
				log.Printf("  %-16s %-8s first error: %s", connector.Name(c), o.Op, o.FirstError)
			}
		}
		log.Printf("  %-16s %d/%d tasks in %s%s", connector.Name(c), res.Completed, tasks,
			elapsed.Round(time.Millisecond), map[bool]string{true: " (truncated by step timeout)", false: ""}[res.Truncated])
	}

	return res
}

// isTimeout recognises OpTimeout firing on either side of the sidecar: the
// direct SDKs return the context error, the Dapr client a gRPC status.
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
}

func ms(seconds float64) string { return fmt.Sprintf("%.2fms", seconds*1000) }
