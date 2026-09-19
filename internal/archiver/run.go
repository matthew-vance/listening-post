package archiver

import (
	"context"
	"log/slog"

	"github.com/matthew-vance/listening-post/internal/pause"
	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Run archives events.raw to Parquet until ctx is cancelled. When gate pauses, the current session is cancelled —
// its pending batch flushed and committed, the client closed so the consumer group is released — and Run waits
// for the resume, then reconnects at the group's committed offset. The backfill moves that offset itself while
// the archiver is away, so the replay isn't re-archived.
func Run(ctx context.Context, cfg Config, logger *slog.Logger, gate *pause.Gate) error {
	for {
		changed := gate.Changed() // before Paused, so a transition between the two is never missed
		if gate.Paused() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
			continue
		}
		a, err := open(ctx, cfg, logger)
		if err != nil {
			return err
		}
		session, stop := context.WithCancel(ctx)
		go func() {
			select {
			case <-changed:
				stop()
			case <-session.Done():
			}
		}()
		err = a.run(session)
		stop()
		a.client.Close()
		if err != nil || ctx.Err() != nil {
			logger.Info("shut down")
			return err
		}
		logger.Info("paused")
	}
}

// open connects to Kafka and pings, failing at startup rather than on the first record.
func open(ctx context.Context, cfg Config, logger *slog.Logger) (*archiver, error) {
	client, err := wire.OpenKafka(ctx, cfg.KafkaBrokers,
		kgo.ConsumerGroup(cfg.ArchiverGroup),
		kgo.ConsumeTopics(cfg.KafkaRaw),
		kgo.DisableAutoCommit(), // offsets advance only once files are on disk
	)
	if err != nil {
		return nil, err
	}
	logger.Info("archiving", "topic", cfg.KafkaRaw, "group", cfg.ArchiverGroup, "dir", cfg.ArchiveDir, "flush_records", cfg.FlushRecords, "flush_seconds", cfg.FlushSeconds)
	return &archiver{client: client, logger: logger, Config: cfg}, nil
}
