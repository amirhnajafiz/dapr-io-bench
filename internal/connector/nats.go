// nats.go
package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// nats configuration
const (
	natsStream       = "BENCH"
	natsSubjects     = "bench.>"
	natsTopicDirect  = "bench.direct"
	natsTopicDapr    = "bench.dapr"
	natsSetupTimeout = 10 * time.Second
)

// NATSDirect publishes with JetStream and waits for the server ack.
type NATSDirect struct {
	url      string
	workload *Workload
	conn     *nats.Conn
	js       jetstream.JetStream
}

func NewNATSDirect(url string, w *Workload) *NATSDirect {
	return &NATSDirect{url: url, workload: w}
}

func (n *NATSDirect) Backend() string { return BackendNATS }

func (n *NATSDirect) Mode() string { return ModeDirect }

func (n *NATSDirect) Connect(ctx context.Context) error {
	conn, err := nats.Connect(n.url, nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("connect nats at %s: %w", n.url, err)
	}

	n.conn = conn

	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("init jetstream: %w", err)
	}

	n.js = js

	// created here rather than in a component file because both the direct and
	// the Dapr path publish into it; Dapr's JetStream component expects the
	// stream to already exist.
	setupCtx, cancel := context.WithTimeout(ctx, natsSetupTimeout)
	defer cancel()

	_, err = js.CreateOrUpdateStream(setupCtx, jetstream.StreamConfig{
		Name:      natsStream,
		Subjects:  []string{natsSubjects},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxMsgs:   100_000,
		MaxAge:    10 * time.Minute,
	})

	if err != nil {
		return fmt.Errorf("create stream %s: %w", natsStream, err)
	}

	return nil
}

func (n *NATSDirect) Ops() []Op {
	return []Op{
		{
			Name: "publish",
			Run: func(ctx context.Context, i int) error {
				_, err := n.js.Publish(ctx, natsTopicDirect, n.workload.Payload())
				return err
			},
		},
	}
}

func (n *NATSDirect) Close() error {
	if n.conn != nil {
		n.conn.Close()
	}

	return nil
}

// NATSDapr publishes the same payload to the same stream through the Dapr
// pub/sub API.
type NATSDapr struct {
	daprBase
	pubsub   string
	workload *Workload
}

func NewNATSDapr(grpcPort, pubsub string, w *Workload) *NATSDapr {
	return &NATSDapr{daprBase: daprBase{grpcPort: grpcPort}, pubsub: pubsub, workload: w}
}

func (n *NATSDapr) Backend() string { return BackendNATS }

func (n *NATSDapr) Connect(ctx context.Context) error { return n.connect(ctx) }

func (n *NATSDapr) Ops() []Op {
	return []Op{
		{
			Name: "publish",
			Run: func(ctx context.Context, i int) error {
				return n.client.PublishEvent(ctx, n.pubsub, natsTopicDapr, n.workload.Payload())
			},
		},
	}
}
