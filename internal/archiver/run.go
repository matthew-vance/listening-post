package archiver

import (
	"context"
	"errors"
	"log/slog"

	"github.com/matthew-vance/listening-post/internal/consume"
	"github.com/matthew-vance/listening-post/internal/pause"
	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Run archives events.raw to Parquet until ctx is cancelled. When gate is paused the client is closed — releasing
// the consumer group — and Run blocks until the gate resumes, then reconnects at the end of the topic, skipping
// whatever was replayed while paused. That is how the backfill keeps the archiver from re-archiving the replay
// without a separate container.
func Run(ctx context.Context, cfg Config, logger *slog.Logger, gate *pause.Gate) error {
	skip := false
	for {
		a, err := open(ctx, cfg, logger, skip)
		if err != nil {
			return err
		}
		skip = false // the skip (if any) is consumed by this open
		err = a.run(ctx, gate)
		a.client.Close()
		if !errors.Is(err, consume.ErrPaused) {
			logger.Info("shut down")
			return err
		}
		logger.Info("paused")
		skip = true // resume past the replay: reconnect at the end of the topic
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gate.WaitResume():
		}
	}
}

// open connects to Kafka and pings, failing at startup rather than on the first record. skip resets the consumer
// to the end of the topic, used only when resuming after a pause so a replayed topic isn't re-archived.
func open(ctx context.Context, cfg Config, logger *slog.Logger, skip bool) (*archiver, error) {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(cfg.ArchiverGroup),
		kgo.ConsumeTopics(cfg.KafkaRaw),
		kgo.DisableAutoCommit(), // offsets advance only once files are on disk
	}
	if skip {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	client, err := wire.OpenKafka(ctx, cfg.KafkaBrokers, opts...)
	if err != nil {
		return nil, err
	}
	logger.Info("archiving", "topic", cfg.KafkaRaw, "group", cfg.ArchiverGroup, "dir", cfg.ArchiveDir, "flush_records", cfg.FlushRecords, "flush_seconds", cfg.FlushSeconds)
	return &archiver{client: client, logger: logger, Config: cfg}, nil
}
