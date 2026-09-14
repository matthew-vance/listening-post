package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Getenv, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run decodes events.raw onto events.decoded and folds events.decoded into aircraft.state until ctx is
// cancelled or SIGINT/SIGTERM. Two loops with their own consumer groups: decoded is keyed by ICAO, so
// instances split aircraft (not stations) between them by partition. Each instance holds state only for the
// decoded partitions it owns, warming it on assignment and dropping it on revoke.
//
// Environment:
//
//	KAFKA_BROKERS      comma-separated bootstrap brokers (required)
//	KAFKA_IN           raw topic to read (default events.raw)
//	KAFKA_OUT          decoded topic to write (default events.decoded)
//	KAFKA_GROUP        decode loop consumer group (default processor)
//	KAFKA_STATE        state topic to write (default aircraft.state)
//	KAFKA_STATE_GROUP  state loop consumer group (default processor-state)
//	EXPIRE_SECONDS     tombstone an aircraft silent this long (default 300)
func run(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	brokers := getenv("KAFKA_BROKERS")
	if brokers == "" {
		return errors.New("KAFKA_BROKERS is not set")
	}
	seeds := strings.Split(brokers, ",")
	in, out, group := cmp.Or(getenv("KAFKA_IN"), "events.raw"), cmp.Or(getenv("KAFKA_OUT"), "events.decoded"), cmp.Or(getenv("KAFKA_GROUP"), "processor")
	stateTopic, stateGroup := cmp.Or(getenv("KAFKA_STATE"), "aircraft.state"), cmp.Or(getenv("KAFKA_STATE_GROUP"), "processor-state")
	expireSeconds, err := strconv.Atoi(cmp.Or(getenv("EXPIRE_SECONDS"), "300"))
	if err != nil {
		return fmt.Errorf("EXPIRE_SECONDS: %w", err)
	}

	decodeClient, err := consumerClient(ctx, seeds, group, in)
	if err != nil {
		return err
	}
	defer decodeClient.CloseAllowingRebalance()
	// Snapshots go to the same partition number their decoded messages came from, so the state topic needs at
	// least as many partitions as the decoded one.
	sl := &stateLoop{seeds: seeds, in: out, topic: stateTopic, state: newState(time.Duration(expireSeconds) * time.Second), logger: logger, now: time.Now}
	stateClient, err := consumerClient(ctx, seeds, stateGroup, out,
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
	logger.Info("processing", "in", in, "out", out, "state", stateTopic)

	// Either loop failing stops both; ctx cancellation stops both cleanly.
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 2)
	go func() { errc <- (&processor{client: decodeClient, out: out, logger: logger}).run(ctx) }()
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
