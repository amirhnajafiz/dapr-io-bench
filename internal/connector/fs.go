// fs.go
package connector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	dapr "github.com/dapr/go-sdk/client"
)

// FSDirect reads and writes files in a directory with the os package: the
// baseline for Dapr's local storage binding, which does the same from the
// sidecar on a volume both containers share.
type FSDirect struct {
	dir      string
	workload *Workload
}

func NewFSDirect(dir string, w *Workload) *FSDirect {
	return &FSDirect{dir: dir, workload: w}
}

func (f *FSDirect) Backend() string { return BackendFS }

func (f *FSDirect) Mode() string { return ModeDirect }

func (f *FSDirect) Connect(context.Context) error {
	if err := os.MkdirAll(f.dir, 0o777); err != nil {
		return fmt.Errorf("create %s: %w", f.dir, err)
	}
	return nil
}

func (f *FSDirect) path(i int) string { return filepath.Join(f.dir, f.workload.Key(i)) }

func (f *FSDirect) Seed(_ context.Context, keyspace int) error {
	for i := 0; i < keyspace; i++ {
		if err := os.WriteFile(f.path(i), f.workload.Payload(), 0o644); err != nil {
			return fmt.Errorf("seed key %d: %w", i, err)
		}
	}
	return nil
}

func (f *FSDirect) Ops() []Op {
	return []Op{
		{
			Name: OpWrite,
			Run: func(_ context.Context, i int) error {
				return os.WriteFile(f.path(i), f.workload.Payload(), 0o644)
			},
		},
		{
			Name: OpRead,
			Run: func(_ context.Context, i int) error {
				_, err := os.ReadFile(f.path(i))
				return err
			},
		},
		{
			Name: OpStat,
			Run: func(_ context.Context, i int) error {
				_, err := os.Stat(f.path(i))
				return err
			},
		},
	}
}

func (f *FSDirect) Close() error { return nil }

// FSDapr does the same file I/O through Dapr's localstorage output binding,
// whose rootPath is a directory on the volume FSDirect writes to.
type FSDapr struct {
	daprBase
	binding  string
	workload *Workload
}

func NewFSDapr(grpcPort, binding string, w *Workload) *FSDapr {
	return &FSDapr{daprBase: daprBase{grpcPort: grpcPort}, binding: binding, workload: w}
}

func (f *FSDapr) Backend() string { return BackendFS }

func (f *FSDapr) Connect(ctx context.Context) error { return f.connect(ctx) }

func (f *FSDapr) create(ctx context.Context, name string, data []byte) error {
	return f.client.InvokeOutputBinding(ctx, &dapr.InvokeBindingRequest{
		Name: f.binding, Operation: "create", Data: data,
		Metadata: map[string]string{"fileName": name},
	})
}

func (f *FSDapr) get(ctx context.Context, name string) error {
	_, err := f.client.InvokeBinding(ctx, &dapr.InvokeBindingRequest{
		Name: f.binding, Operation: "get",
		Metadata: map[string]string{"fileName": name},
	})
	return err
}

func (f *FSDapr) Seed(ctx context.Context, keyspace int) error {
	for i := 0; i < keyspace; i++ {
		if err := f.create(ctx, f.workload.Key(i), f.workload.Payload()); err != nil {
			return fmt.Errorf("seed key %d: %w", i, err)
		}
		if err := f.create(ctx, f.workload.MarkerKey(i), Marker); err != nil {
			return fmt.Errorf("seed marker %d: %w", i, err)
		}
	}
	return nil
}

func (f *FSDapr) Ops() []Op {
	return []Op{
		{
			Name: OpWrite,
			Run: func(ctx context.Context, i int) error {
				return f.create(ctx, f.workload.Key(i), f.workload.Payload())
			},
		},
		{
			Name: OpRead,
			Run:  func(ctx context.Context, i int) error { return f.get(ctx, f.workload.Key(i)) },
		},
		{
			// the binding has no stat operation; fetching a one-byte marker
			// file is the lightest call it offers
			Name: OpStat,
			Run:  func(ctx context.Context, i int) error { return f.get(ctx, f.workload.MarkerKey(i)) },
		},
	}
}
