// connector.go
package connector

import "context"

// connector mode
const (
	ModeDirect = "direct" // app -> backend SDK -> backend
	ModeDapr   = "dapr"   // app -> gRPC -> daprd sidecar -> backend
)

// backend names
const (
	BackendNATS     = "nats"
	BackendPostgres = "postgres"
	BackendRedis    = "redis"
	BackendFS       = "fs"
)

// Operation names shared by the connectors. Heavy ops carry the payload;
// stat is the lightest call that touches a key without moving data, so the
// two classes separate per-call overhead from the cost of moving bytes.
const (
	OpWrite   = "write"
	OpRead    = "read"
	OpStat    = "stat"
	OpPublish = "publish"
)

// Op is a single named operation a connector can perform, e.g. "write".
// i is the monotonically increasing iteration number; connectors derive their
// key from it so every connector touches the same keyspace in the same order.
type Op struct {
	Name string
	Run  func(ctx context.Context, i int) error
}

// Connector is one of the benchmarked data paths.
//
// Ops are independent of each other: the sweep calls Seed before measuring,
// so a "read" or "stat" never depends on the write that happened to precede it.
type Connector interface {
	// Backend reports which backing service is exercised (nats/postgres/redis/fs).
	Backend() string
	// Mode reports whether the path goes through Dapr (direct/dapr).
	Mode() string
	// Connect establishes the client and prepares any schema or streams.
	Connect(ctx context.Context) error
	// Seed writes every key of the keyspace once, so later reads never miss.
	Seed(ctx context.Context, keyspace int) error
	// Ops returns the operations to benchmark.
	Ops() []Op
	// Close releases the client.
	Close() error
}

// Name is the metric-friendly identifier of a connector, e.g. "redis-dapr".
func Name(c Connector) string { return c.Backend() + "-" + c.Mode() }
