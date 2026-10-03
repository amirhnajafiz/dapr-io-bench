// dapr.go
package connector

import (
	"context"
	"fmt"
	"math"

	dapr "github.com/dapr/go-sdk/client"
	"google.golang.org/grpc"
)

// daprBase is the shared plumbing of the Dapr connectors.
type daprBase struct {
	grpcPort string
	client   dapr.Client
}

func (d *daprBase) connect(_ context.Context) error {
	// the SDK's default 4 MiB message cap would turn every large payload into
	// a client-side error before it reached the sidecar; the sidecar's own cap
	// is raised to match in docker-compose.yml
	c, err := dapr.NewClientWithPort(d.grpcPort, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
		grpc.MaxCallSendMsgSize(math.MaxInt32),
	))
	if err != nil {
		return fmt.Errorf("dial dapr sidecar on port %s: %w", d.grpcPort, err)
	}

	d.client = c

	return nil
}

func (d *daprBase) Mode() string { return ModeDapr }

func (d *daprBase) Close() error {
	if d.client != nil {
		d.client.Close()
	}

	return nil
}

// seedState writes every key's payload and every marker through a state store.
func (d *daprBase) seedState(ctx context.Context, store string, w *Workload, keyspace int) error {
	for i := 0; i < keyspace; i++ {
		if err := d.client.SaveState(ctx, store, w.Key(i), w.Payload(), nil); err != nil {
			return fmt.Errorf("seed key %d: %w", i, err)
		}
		if err := d.client.SaveState(ctx, store, w.MarkerKey(i), Marker, nil); err != nil {
			return fmt.Errorf("seed marker %d: %w", i, err)
		}
	}
	return nil
}

// stateOps are the write/read/stat operations every Dapr state store shares.
func (d *daprBase) stateOps(store string, w *Workload) []Op {
	return []Op{
		{
			Name: OpWrite,
			Run: func(ctx context.Context, i int) error {
				return d.client.SaveState(ctx, store, w.Key(i), w.Payload(), nil)
			},
		},
		{
			Name: OpRead,
			Run: func(ctx context.Context, i int) error {
				_, err := d.client.GetState(ctx, store, w.Key(i), nil)
				return err
			},
		},
		{
			// the state API has no exists call, so the lightest thing it can
			// do for a key is fetch a one-byte marker record
			Name: OpStat,
			Run: func(ctx context.Context, i int) error {
				_, err := d.client.GetState(ctx, store, w.MarkerKey(i), nil)
				return err
			},
		},
	}
}
