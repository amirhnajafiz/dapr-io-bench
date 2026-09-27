// redis.go
package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisDirect talks to Redis with go-redis: one TCP round trip per operation.
// This is the baseline the Dapr Redis path is measured against.
type RedisDirect struct {
	addr     string
	workload *Workload
	client   *redis.Client
}

func NewRedisDirect(addr string, w *Workload) *RedisDirect {
	return &RedisDirect{addr: addr, workload: w}
}

func (r *RedisDirect) Backend() string { return BackendRedis }

func (r *RedisDirect) Mode() string { return ModeDirect }

func (r *RedisDirect) Connect(ctx context.Context) error {
	r.client = redis.NewClient(&redis.Options{
		Addr:     r.addr,
		PoolSize: 32,
	})

	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis at %s: %w", r.addr, err)
	}

	return nil
}

func (r *RedisDirect) Ops() []Op {
	return []Op{
		{
			Name: "write",
			Run: func(ctx context.Context, i int) error {
				return r.client.Set(ctx, r.workload.Key(i), r.workload.Payload(), time.Hour).Err()
			},
		},
		{
			Name: "read",
			Run: func(ctx context.Context, i int) error {
				return r.client.Get(ctx, r.workload.Key(i)).Err()
			},
		},
	}
}

func (r *RedisDirect) Close() error {
	if r.client != nil {
		return r.client.Close()
	}

	return nil
}

// RedisDapr performs the same get/set against the same Redis instance, but via
// the Dapr state store API.
type RedisDapr struct {
	daprBase
	store    string
	workload *Workload
}

func NewRedisDapr(grpcPort, store string, w *Workload) *RedisDapr {
	return &RedisDapr{daprBase: daprBase{grpcPort: grpcPort}, store: store, workload: w}
}

func (r *RedisDapr) Backend() string { return BackendRedis }

func (r *RedisDapr) Connect(ctx context.Context) error { return r.connect(ctx) }

func (r *RedisDapr) Ops() []Op {
	return []Op{
		{
			Name: "write",
			Run: func(ctx context.Context, i int) error {
				return r.client.SaveState(ctx, r.store, r.workload.Key(i), r.workload.Payload(), nil)
			},
		},
		{
			Name: "read",
			Run: func(ctx context.Context, i int) error {
				_, err := r.client.GetState(ctx, r.store, r.workload.Key(i), nil)
				return err
			},
		},
	}
}
