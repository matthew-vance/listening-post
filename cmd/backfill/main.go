// Command backfill replays the archive onto events.raw.replay in event-time order, one day at a time, so a
// second instance of the processor (TOPIC_SUFFIX=.replay) folds the whole history without touching the live one.
// It is the replay half of scripts/backfill.sh; run it standalone with KAFKA_BROKERS and ARCHIVE_DIR (default
// archive).
package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/matthew-vance/listening-post/internal/archiver"
	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	replayTopic = wire.RawTopic + wire.ReplaySuffix
	batchSize   = 5000
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	archive := os.Getenv("ARCHIVE_DIR")
	if archive == "" {
		archive = "archive"
	}
	brokers, err := wire.Brokers(os.Getenv)
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}
	days, err := archiver.Days(archive)
	if err != nil {
		logger.Error("list archive", "err", err)
		os.Exit(1)
	}

	client, err := wire.OpenKafka(ctx, brokers)
	if err != nil {
		logger.Error("connect to kafka", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	total := 0
	for _, day := range days {
		rows, err := archiver.ReadDay(day)
		if err != nil {
			logger.Error("read day", "dir", day, "err", err)
			os.Exit(1)
		}
		for start := 0; start < len(rows); start += batchSize {
			end := min(start+batchSize, len(rows))
			records := make([]*kgo.Record, 0, end-start)
			for _, r := range rows[start:end] {
				rec, err := wire.Record(replayTopic, wire.Event{StationID: r.StationID, TS: r.TS, Raw: r.Raw})
				if err != nil {
					logger.Error("encode event", "err", err)
					os.Exit(1)
				}
				records = append(records, rec)
			}
			if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
				logger.Error("produce", "err", err)
				os.Exit(1)
			}
		}
		total += len(rows)
		logger.Info("replayed", "day", filepath.Base(day), "events", len(rows), "total", total)
	}
}
