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

// run archives events.raw to Parquet until ctx is cancelled or SIGINT/SIGTERM.
//
// Environment:
//
//	KAFKA_BROKERS  comma-separated bootstrap brokers (required)
//	KAFKA_TOPIC    topic to archive (default events.raw)
//	KAFKA_GROUP    consumer group (default archiver)
//	ARCHIVE_DIR    root directory for Parquet files (required)
//	FLUSH_RECORDS  write a batch after this many records (default 10000)
//	FLUSH_SECONDS  or after this long since the last write (default 300)
func run(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	brokers, dir := getenv("KAFKA_BROKERS"), getenv("ARCHIVE_DIR")
	if brokers == "" {
		return errors.New("KAFKA_BROKERS is not set")
	}
	if dir == "" {
		return errors.New("ARCHIVE_DIR is not set")
	}
	flushRecords, err := strconv.Atoi(cmp.Or(getenv("FLUSH_RECORDS"), "10000"))
	if err != nil {
		return fmt.Errorf("FLUSH_RECORDS: %w", err)
	}
	flushSeconds, err := strconv.Atoi(cmp.Or(getenv("FLUSH_SECONDS"), "300"))
	if err != nil {
		return fmt.Errorf("FLUSH_SECONDS: %w", err)
	}
	topic, group := cmp.Or(getenv("KAFKA_TOPIC"), "events.raw"), cmp.Or(getenv("KAFKA_GROUP"), "archiver")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), // offsets advance only once files are on disk
	)
	if err != nil {
		return fmt.Errorf("configure kafka client: %w", err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("connect to kafka: %w", err)
	}
	logger.Info("archiving", "topic", topic, "group", group, "dir", dir, "flush_records", flushRecords, "flush_seconds", flushSeconds)

	a := &archiver{
		client:       client,
		dir:          dir,
		logger:       logger,
		flushRecords: flushRecords,
		flushAfter:   time.Duration(flushSeconds) * time.Second,
	}
	err = a.run(ctx)
	logger.Info("shut down")
	return err
}
