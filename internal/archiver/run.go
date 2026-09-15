package archiver

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Run archives events.raw to Parquet until ctx is cancelled.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ConsumerGroup(cfg.ArchiverGroup),
		kgo.ConsumeTopics(cfg.KafkaRaw),
		kgo.DisableAutoCommit(), // offsets advance only once files are on disk
	)
	if err != nil {
		return fmt.Errorf("configure kafka client: %w", err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("connect to kafka: %w", err)
	}
	logger.Info("archiving", "topic", cfg.KafkaRaw, "group", cfg.ArchiverGroup, "dir", cfg.ArchiveDir, "flush_records", cfg.FlushRecords, "flush_seconds", cfg.FlushSeconds)

	a := &archiver{
		client:       client,
		dir:          cfg.ArchiveDir,
		logger:       logger,
		flushRecords: cfg.FlushRecords,
		flushAfter:   time.Duration(cfg.FlushSeconds) * time.Second,
	}
	err = a.run(ctx)
	logger.Info("shut down")
	return err
}
