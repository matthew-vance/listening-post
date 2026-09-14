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
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Getenv, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run decodes events.raw onto events.decoded until ctx is cancelled or SIGINT/SIGTERM.
//
// Environment:
//
//	KAFKA_BROKERS  comma-separated bootstrap brokers (required)
//	KAFKA_IN       topic to read (default events.raw)
//	KAFKA_OUT      topic to write (default events.decoded)
//	KAFKA_GROUP    consumer group (default processor)
func run(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	brokers := getenv("KAFKA_BROKERS")
	if brokers == "" {
		return errors.New("KAFKA_BROKERS is not set")
	}
	in, out, group := cmp.Or(getenv("KAFKA_IN"), "events.raw"), cmp.Or(getenv("KAFKA_OUT"), "events.decoded"), cmp.Or(getenv("KAFKA_GROUP"), "processor")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(in),
		kgo.DisableAutoCommit(), // offsets advance only after the decoded batch is acked
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("configure kafka client: %w", err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("connect to kafka: %w", err)
	}
	logger.Info("processing", "in", in, "out", out, "group", group)

	p := &processor{client: client, out: out, logger: logger}
	err = p.run(ctx)
	logger.Info("shut down")
	return err
}
