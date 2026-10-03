// registry.go
package connector

import (
	"fmt"
	"sort"
	"strings"
)

// Endpoints are the addresses a connector needs to reach its backend, either
// directly or through the sidecar.
type Endpoints struct {
	NATSURL     string
	RedisAddr   string
	PostgresDSN string
	FSDir       string // directory the direct filesystem connector writes to

	DaprGRPCPort   string
	DaprPubsubNATS string
	DaprStateRedis string
	DaprStatePG    string
	DaprBindingFS  string
}

type factory func(e Endpoints, w *Workload) Connector

var registry = map[string]factory{
	BackendNATS + "-" + ModeDirect:     func(e Endpoints, w *Workload) Connector { return NewNATSDirect(e.NATSURL, w) },
	BackendNATS + "-" + ModeDapr:       func(e Endpoints, w *Workload) Connector { return NewNATSDapr(e.DaprGRPCPort, e.DaprPubsubNATS, w) },
	BackendPostgres + "-" + ModeDirect: func(e Endpoints, w *Workload) Connector { return NewPostgresDirect(e.PostgresDSN, w) },
	BackendPostgres + "-" + ModeDapr:   func(e Endpoints, w *Workload) Connector { return NewPostgresDapr(e.DaprGRPCPort, e.DaprStatePG, w) },
	BackendRedis + "-" + ModeDirect:    func(e Endpoints, w *Workload) Connector { return NewRedisDirect(e.RedisAddr, w) },
	BackendRedis + "-" + ModeDapr:      func(e Endpoints, w *Workload) Connector { return NewRedisDapr(e.DaprGRPCPort, e.DaprStateRedis, w) },
	BackendFS + "-" + ModeDirect:       func(e Endpoints, w *Workload) Connector { return NewFSDirect(e.FSDir, w) },
	BackendFS + "-" + ModeDapr:         func(e Endpoints, w *Workload) Connector { return NewFSDapr(e.DaprGRPCPort, e.DaprBindingFS, w) },
}

// Names lists every connector this binary knows, sorted.
func Names() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// New builds the connector called name, e.g. "redis-dapr".
func New(name string, e Endpoints, w *Workload) (Connector, error) {
	f, ok := registry[strings.TrimSpace(name)]
	if !ok {
		return nil, fmt.Errorf("unknown connector %q; known: %s", name, strings.Join(Names(), ", "))
	}
	return f(e, w), nil
}
