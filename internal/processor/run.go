package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Run decodes events.raw onto events.decoded and folds events.decoded into aircraft.state until ctx is
// cancelled or SIGINT/SIGTERM. Two loops with their own consumer groups: decoded is keyed by ICAO, so
// instances split aircraft (not stations) between them by partition. Each instance holds state only for the
// decoded partitions it owns, warming it on assignment and dropping it on revoke.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	decodeClient, err := consumerClient(ctx, cfg.KafkaBrokers, cfg.ProcessorGroup, cfg.KafkaRaw)
	if err != nil {
		return err
	}
	defer decodeClient.CloseAllowingRebalance()
	// Snapshots go to the same partition number their decoded messages came from, so the state topic needs at
	// least as many partitions as the decoded one.
	sl := &stateLoop{seeds: cfg.KafkaBrokers, in: cfg.KafkaDecoded, topic: cfg.KafkaState, state: newState(time.Duration(cfg.ExpireSeconds) * time.Second), logger: logger, now: time.Now}
	stateClient, err := consumerClient(ctx, cfg.KafkaBrokers, cfg.ProcessorStateGroup, cfg.KafkaDecoded,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.OnPartitionsAssigned(sl.onAssigned),
		kgo.OnPartitionsRevoked(sl.onRevoked),
		kgo.OnPartitionsLost(sl.onRevoked),
	)
	if err != nil {
		return err
	}
	defer stateClient.CloseAllowingRebalance()
	sl.client = stateClient
	logger.Info("processing", "in", cfg.KafkaRaw, "out", cfg.KafkaDecoded, "state", cfg.KafkaState)

	// Either loop failing stops both; ctx cancellation stops both cleanly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 2)
	go func() { errc <- decodeLoop(ctx, decodeClient, cfg.KafkaDecoded, logger) }()
	go func() { errc <- sl.run(ctx) }()
	first := <-errc
	cancel()
	second := <-errc
	logger.Info("shut down")
	return errors.Join(first, second)
}

// consumerClient builds a group consumer that commits manually and holds rebalances while a batch is in flight,
// so a commit never lands on a partition this member no longer owns. Loops must AllowRebalance after committing.
func consumerClient(ctx context.Context, seeds []string, group, topic string, opts ...kgo.Opt) (*kgo.Client, error) {
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), // offsets advance only after the produced batch is acked
		kgo.BlockRebalanceOnPoll(),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("configure kafka client for %s: %w", topic, err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect to kafka: %w", err)
	}
	return client, nil
}
