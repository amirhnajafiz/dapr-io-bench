// postgres.go
package connector

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresDirect uses pgx against a single key/value table.
type PostgresDirect struct {
	dsn      string
	workload *Workload
	pool     *pgxpool.Pool
}

func NewPostgresDirect(dsn string, w *Workload) *PostgresDirect {
	return &PostgresDirect{dsn: dsn, workload: w}
}

func (p *PostgresDirect) Backend() string { return BackendPostgres }

func (p *PostgresDirect) Mode() string { return ModeDirect }

func (p *PostgresDirect) Connect(ctx context.Context) error {
	cfg, err := pgxpool.ParseConfig(p.dsn)
	if err != nil {
		return fmt.Errorf("parse postgres dsn: %w", err)
	}

	cfg.MaxConns = 32

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}

	p.pool = pool

	// the table the direct path writes to. Dapr creates its own table for the
	// state store component, so the two paths never share storage.
	_, err = p.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS bench_kv (
			key   TEXT PRIMARY KEY,
			value BYTEA NOT NULL
		)`)
	if err != nil {
		return fmt.Errorf("create bench_kv table: %w", err)
	}

	return p.pool.Ping(ctx)
}

func (p *PostgresDirect) Ops() []Op {
	return []Op{
		{
			Name: "write",
			Run: func(ctx context.Context, i int) error {
				_, err := p.pool.Exec(ctx,
					`INSERT INTO bench_kv (key, value) VALUES ($1, $2)
				 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
					p.workload.Key(i), p.workload.Payload())
				return err
			},
		},
		{
			Name: "read",
			Run: func(ctx context.Context, i int) error {
				var value []byte
				return p.pool.QueryRow(ctx,
					`SELECT value FROM bench_kv WHERE key = $1`, p.workload.Key(i)).Scan(&value)
			},
		},
	}
}

func (p *PostgresDirect) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}

	return nil
}

// PostgresDapr writes the same values to the same database through the Dapr
// state store API. Dapr owns its table and schema; we only speak the state API.
type PostgresDapr struct {
	daprBase
	store    string
	workload *Workload
}

func NewPostgresDapr(grpcPort, store string, w *Workload) *PostgresDapr {
	return &PostgresDapr{daprBase: daprBase{grpcPort: grpcPort}, store: store, workload: w}
}

func (p *PostgresDapr) Backend() string { return BackendPostgres }

func (p *PostgresDapr) Connect(ctx context.Context) error { return p.connect(ctx) }

func (p *PostgresDapr) Ops() []Op {
	return []Op{
		{
			Name: "write",
			Run: func(ctx context.Context, i int) error {
				return p.client.SaveState(ctx, p.store, p.workload.Key(i), p.workload.Payload(), nil)
			},
		},
		{
			Name: "read",
			Run: func(ctx context.Context, i int) error {
				_, err := p.client.GetState(ctx, p.store, p.workload.Key(i), nil)
				return err
			},
		},
	}
}
