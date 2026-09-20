package archiver

import (
	"context"
	"log/slog"

	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Run archives events.raw to Parquet until ctx is cancelled. It connects and pings first, failing at startup
// rather than on the first record.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	client, err := wire.OpenKafka(ctx, cfg.KafkaBrokers,
		kgo.ConsumerGroup(cfg.ArchiverGroup),
		kgo.ConsumeTopics(cfg.KafkaRaw),
		kgo.DisableAutoCommit(), // offsets advance only once files are on disk
	)
	if err != nil {
		return err
	}
	defer client.Close()
	logger.Info("archiving", "topic", cfg.KafkaRaw, "group", cfg.ArchiverGroup, "dir", cfg.ArchiveDir, "flush_records", cfg.FlushRecords, "flush_seconds", cfg.FlushSeconds)
	a := &archiver{client: client, logger: logger, Config: cfg}
	err = a.run(ctx)
	logger.Info("shut down")
	return err
}
